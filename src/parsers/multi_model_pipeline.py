"""
Multi-Model Extraction Pipeline

Three-stage document extraction:
1. Gemini Flash (Draft) - Fast extraction of categories, topics, medications
2. GPT-5.1 (Critic) - Review with reasoning, identify issues
3. GPT-4.1 (Fix) - Apply corrections

Pipeline produces validated extractions with higher accuracy than single-model approach.
"""

import json
import asyncio
from typing import List, Dict, Any, Optional, AsyncGenerator
from dataclasses import dataclass, asdict
from datetime import datetime

from src.generators.core_ai import (
    call_gemini_model,
    call_openai_model,
    stream_gemini_generator
)
from src.parsers.document_parser import (
    get_chunk_prompt_v2,
    parse_chunk_response_v2,
    parse_page_block_v2,
    ChunkParseResult,
    PageData,
    DocumentData,
    build_document_data,
    load_meta_categories_from_json,
    format_meta_categories_for_prompt,
    format_topics_for_prompt
)


@dataclass
class ExtractionIssue:
    """Issue found by critic model."""
    issue_type: str  # "missing_category", "wrong_category", "invalid_topic", "anchor_mismatch"
    page: int
    field: str  # "codes", "topics", "medications"
    current_value: str
    suggested_fix: str
    reasoning: str


@dataclass
class PipelineResult:
    """Result of multi-model pipeline."""
    draft_extraction: Dict
    critique: List[ExtractionIssue]
    final_extraction: Dict
    stages_completed: List[str]
    total_issues_found: int
    total_issues_fixed: int
    processing_time_ms: int
    parsed_pages: List = None  # Parsed PageData objects with correct page numbers


# ============================================================
# PROMPTS
# ============================================================

CRITIC_PROMPT = """You are a medical coding expert reviewing document extraction results.

=== SOURCE TEXT (from PDF) ===
{source_text}

=== DRAFT EXTRACTION ===
{draft_extraction}

=== AVAILABLE CODE CATEGORIES ===
{meta_categories}

=== AVAILABLE TOPICS ===
{topics_dictionary}

=== YOUR TASK ===

Review the draft extraction and identify issues. For each issue found:

1. Check CODE_CATEGORIES:
   - Is the pattern valid (matches available categories)?
   - Is the code_type correct (ICD-10, CPT, HCPCS)?
   - Does the anchor text actually exist in source?
   - Should additional categories be extracted?

2. Check TOPICS:
   - Is the topic name from the dictionary?
   - Does the anchor text exist in source?
   - Are there topics that should be added?

3. Check MEDICATIONS:
   - Is the drug name correct (generic preferred)?
   - Does the anchor text exist in source?

=== OUTPUT FORMAT ===

Return a JSON array of issues. If no issues, return empty array [].

```json
[
  {{
    "issue_type": "wrong_category",
    "page": 1,
    "field": "codes",
    "current_value": "E119 (ICD-10)",
    "suggested_fix": "E11 (ICD-10)",
    "reasoning": "Should extract category E11, not specific code E119"
  }},
  {{
    "issue_type": "missing_category",
    "page": 2,
    "field": "codes",
    "current_value": "-",
    "suggested_fix": "J19 (HCPCS: \"Semaglutide J1950\"...\"once weekly\")",
    "reasoning": "Page mentions J1950 for semaglutide but code category was not extracted"
  }},
  {{
    "issue_type": "invalid_topic",
    "page": 1,
    "field": "topics",
    "current_value": "diabetes treatment",
    "suggested_fix": "Type 2 Diabetes Mellitus",
    "reasoning": "Topic must match dictionary exactly. 'diabetes treatment' should be 'Type 2 Diabetes Mellitus'"
  }},
  {{
    "issue_type": "anchor_mismatch",
    "page": 1,
    "field": "topics",
    "current_value": "\"Type 2 Diabetes\" (\"text not in document\"...\"end\")",
    "suggested_fix": "\"Type 2 Diabetes Mellitus\" (\"GLP-1 receptor agonists are indicated\"...\"glycemic control\")",
    "reasoning": "Anchor text 'text not in document' does not appear in source"
  }}
]
```

Issue types:
- "wrong_category": Extracted specific code instead of category
- "missing_category": Code/topic/medication exists in text but not extracted
- "invalid_topic": Topic name not from dictionary
- "anchor_mismatch": Anchor text not found in source
- "wrong_type": Incorrect code type (e.g., HCPCS marked as CPT)

Only report real issues. If extraction is correct, return [].
"""


FIX_PROMPT = """You are applying corrections to document extraction results.

=== ORIGINAL DRAFT ===
{draft_extraction}

=== ISSUES TO FIX ===
{issues}

=== TASK ===

Apply the suggested fixes to produce corrected extraction.

Rules:
1. Apply each fix exactly as suggested
2. Keep ALL other pages and extractions UNCHANGED
3. Maintain the exact output format
4. Do not add anything not in the fixes

=== OUTPUT FORMAT ===

IMPORTANT: You MUST output ALL pages from the original draft, not just the corrected ones!

Return the COMPLETE extraction with fixes applied:

```
[PAGE_START]
[PAGE_TYPE: clinical]
[CODE_CATEGORIES: E11 (ICD-10: "start..."..."end"), ...]
[TOPICS: "Topic Name" ("start..."..."end"), ...]
[MEDICATIONS: "drug" ("start..."..."end"), ...]

Content here...
[PAGE_END]
```

Output ALL page blocks from the original draft with corrections applied. Do NOT skip any pages.
"""


# ============================================================
# PIPELINE STAGES
# ============================================================

async def stage1_draft_extraction(
    pdf_text_chunks: List[str],
    topics: List[Dict],
    meta_categories: Dict = None,
    thinking_budget: int = 2048,
    start_page: int = 1,
    provider: str = "gemini",
    openai_model: str = "gpt-5.2",
    reasoning_effort: str = "low"
) -> str:
    """
    Stage 1: Draft Extraction (Gemini or OpenAI)

    Args:
        pdf_text_chunks: List of PDF page text chunks
        topics: Topics dictionary
        meta_categories: Meta-categories dict
        thinking_budget: Gemini thinking budget (ignored for OpenAI)
        start_page: Starting page number (for correct page labeling)
        provider: "gemini" or "openai"
        openai_model: OpenAI model to use (default: gpt-5.1)
        reasoning_effort: OpenAI reasoning effort (default: low)

    Returns:
        Raw extraction text from model
    """
    if meta_categories is None:
        meta_categories = load_meta_categories_from_json()

    # Build prompt with V2 format
    prompt = get_chunk_prompt_v2(
        pages_count=len(pdf_text_chunks),
        start_page=start_page,
        topics=topics,
        meta_categories=meta_categories
    )

    # Add the actual PDF content
    prompt += "\n\n=== PDF CONTENT ===\n\n"
    for i, chunk in enumerate(pdf_text_chunks):
        page_num = start_page + i
        prompt += f"--- Page {page_num} ---\n{chunk}\n\n"

    # Call model based on provider
    if provider == "openai":
        response = await call_openai_model(
            prompt,
            model=openai_model,
            reasoning_effort=reasoning_effort
        )
    else:
        response = await call_gemini_model(prompt, thinking_budget=thinking_budget)

    return response


async def retry_single_page(
    page_text: str,
    page_num: int,
    topics: List[Dict],
    meta_categories: Dict,
    max_retries: int = 2
) -> Optional[PageData]:
    """
    Retry parsing a single page that failed.
    Content comes from original PDF, only metadata from Gemini.

    Args:
        page_text: Original PDF text for this page
        page_num: Page number
        topics: Topics dictionary
        meta_categories: Meta-categories dict
        max_retries: Maximum retry attempts

    Returns:
        PageData if successful, None if failed
    """
    from src.parsers.document_parser import validate_and_fix_anchors

    for attempt in range(max_retries):
        prompt = get_chunk_prompt_v2(
            pages_count=1,
            start_page=page_num,
            topics=topics,
            meta_categories=meta_categories
        )
        prompt += f"\n\n=== PDF CONTENT ===\n\n--- Page {page_num} ---\n{page_text}\n\n"

        try:
            response = await call_gemini_model(prompt, thinking_budget=1024)

            # Parse response
            import re
            pattern = r'\[PAGE_START(?::\s*(\d+))?\](.*?)\[PAGE_END\]'
            matches = re.findall(pattern, response, re.DOTALL)

            if matches:
                claimed_str, block_content = matches[0]
                page_data = parse_page_block_v2(block_content, page_num)

                # Set content from original PDF
                if not page_data.skip_reason:
                    page_data.content = page_text
                    # Validate anchors
                    page_data, _ = validate_and_fix_anchors(page_data, page_text)

                return page_data

        except Exception as e:
            print(f"    Retry error for page {page_num}: {e}")

    return None


async def stage2_critic_review(
    source_text: str,
    draft_extraction: str,
    topics: List[Dict],
    meta_categories: Dict = None,
    model: str = "gpt-4.1"
) -> List[ExtractionIssue]:
    """
    Stage 2: GPT-4.1 Critic Review

    Args:
        source_text: Original PDF text
        draft_extraction: Gemini's draft extraction
        topics: Topics dictionary
        meta_categories: Meta-categories dict
        model: Model to use (default gpt-4.1, can use gpt-5.1 for deeper analysis)

    Returns:
        List of issues found
    """
    if meta_categories is None:
        meta_categories = load_meta_categories_from_json()

    # Format dictionaries for prompt
    meta_cats_str = format_meta_categories_for_prompt(meta_categories)
    topics_str = format_topics_for_prompt(topics)

    prompt = CRITIC_PROMPT.format(
        source_text=source_text[:15000],  # Limit source text
        draft_extraction=draft_extraction,
        meta_categories=meta_cats_str,
        topics_dictionary=topics_str
    )

    # Call critic model (GPT-4.1 is 10x faster than GPT-5.1 with similar quality)
    response = await call_openai_model(
        prompt=prompt,
        model=model,
        reasoning_effort="medium" if model == "gpt-5.1" else None
    )

    # Parse issues from response
    issues = []
    try:
        # Extract JSON from response
        json_match = response
        if "```json" in response:
            json_match = response.split("```json")[1].split("```")[0]
        elif "```" in response:
            json_match = response.split("```")[1].split("```")[0]

        issues_data = json.loads(json_match.strip())

        for issue_dict in issues_data:
            issues.append(ExtractionIssue(
                issue_type=issue_dict.get("issue_type", "unknown"),
                page=issue_dict.get("page", 0),
                field=issue_dict.get("field", ""),
                current_value=issue_dict.get("current_value", ""),
                suggested_fix=issue_dict.get("suggested_fix", ""),
                reasoning=issue_dict.get("reasoning", "")
            ))
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        print(f"Warning: Could not parse critic response: {e}")
        # Return empty issues if parsing fails
        pass

    return issues


async def stage3_apply_fixes(
    draft_extraction: str,
    issues: List[ExtractionIssue]
) -> str:
    """
    Stage 3: GPT-4.1 Apply Fixes

    Args:
        draft_extraction: Original draft extraction
        issues: List of issues to fix

    Returns:
        Corrected extraction text
    """
    if not issues:
        # No issues to fix, return draft as-is
        return draft_extraction

    # Format issues for prompt
    issues_str = json.dumps([asdict(i) for i in issues], indent=2)

    prompt = FIX_PROMPT.format(
        draft_extraction=draft_extraction,
        issues=issues_str
    )

    # Call GPT-4.1 (no reasoning needed)
    response = await call_openai_model(
        prompt=prompt,
        model="gpt-4.1",
        reasoning_effort=None
    )

    return response


# ============================================================
# CHUNK PIPELINE (3-stage per chunk)
# ============================================================

CHUNK_SIZE = 1  # Pages per chunk (1 = per-page processing, no confusion)
PARALLEL_LIMIT = 30  # Concurrent API calls


async def process_single_chunk(
    chunk_pages: List[str],
    chunk_index: int,
    start_page: int,
    topics: List[Dict],
    meta_categories: Dict,
    skip_critic: bool = False,
    skip_fix: bool = False,
    provider: str = "gemini",
    openai_model: str = "gpt-5.2",
    reasoning_effort: str = "low"
) -> Dict:
    """
    Process a single chunk through all 3 stages.

    Args:
        chunk_pages: List of page texts for this chunk
        chunk_index: Index of this chunk (for logging)
        start_page: Starting page number
        topics: Topics dictionary
        meta_categories: Meta-categories dict
        skip_critic: Skip GPT-5.1 critic
        skip_fix: Skip GPT-4.1 fix
        provider: "gemini" or "openai" for draft extraction
        openai_model: OpenAI model (default: gpt-5.1)
        reasoning_effort: OpenAI reasoning effort (default: low)

    Returns:
        Dict with draft, issues, final extraction for this chunk
    """
    import time
    import re
    chunk_start = time.time()
    pages_range = f"{start_page}-{start_page + len(chunk_pages) - 1}"

    print(f"  [Chunk {chunk_index}] Starting pages {pages_range}...")

    # Stage 1: Draft Extraction
    draft = await stage1_draft_extraction(
        pdf_text_chunks=chunk_pages,
        topics=topics,
        meta_categories=meta_categories,
        start_page=start_page,
        provider=provider,
        openai_model=openai_model,
        reasoning_effort=reasoning_effort
    )
    provider_name = f"{openai_model}" if provider == "openai" else "Gemini"
    print(f"  [Chunk {chunk_index}] ✓ {provider_name} draft complete")

    issues = []
    final = draft

    # Stage 2: GPT-4.1 Critic (10x faster than GPT-5.1, similar quality)
    if not skip_critic:
        source_text = "\n\n".join(chunk_pages)
        issues = await stage2_critic_review(
            source_text=source_text,
            draft_extraction=draft,
            topics=topics,
            meta_categories=meta_categories,
            model="gpt-4.1"
        )
        print(f"  [Chunk {chunk_index}] ✓ GPT-4.1 critic: {len(issues)} issues")

        # Stage 3: GPT-4.1 Fix
        if issues and not skip_fix:
            fixed = await stage3_apply_fixes(
                draft_extraction=draft,
                issues=issues
            )

            # Check if fix returned all pages - if not, use draft as base
            draft_blocks = len(re.findall(r'\[PAGE_START\]', draft))
            fixed_blocks = len(re.findall(r'\[PAGE_START\]', fixed))

            if fixed_blocks >= draft_blocks:
                final = fixed
                print(f"  [Chunk {chunk_index}] ✓ GPT-4.1 fixes applied ({fixed_blocks} pages)")
            else:
                # GPT-4.1 returned partial result - use draft as base
                # This happens when GPT-4.1 only outputs corrected pages
                print(f"  [Chunk {chunk_index}] ⚠ GPT-4.1 returned {fixed_blocks}/{draft_blocks} pages, using draft")
                final = draft

    elapsed_ms = int((time.time() - chunk_start) * 1000)
    print(f"  [Chunk {chunk_index}] Done in {elapsed_ms}ms")

    return {
        "chunk_index": chunk_index,
        "start_page": start_page,
        "pages_count": len(chunk_pages),
        "original_pages": chunk_pages,  # Store original text for validation
        "draft": draft,
        "issues": issues,
        "final": final,
        "elapsed_ms": elapsed_ms
    }


# ============================================================
# MAIN PIPELINE (Parallel chunks)
# ============================================================

async def run_extraction_pipeline(
    pdf_text_chunks: List[str],
    topics: List[Dict],
    meta_categories: Dict = None,
    skip_critic: bool = False,
    skip_fix: bool = False,
    chunk_size: int = CHUNK_SIZE,
    parallel_limit: int = PARALLEL_LIMIT,
    provider: str = "gemini",
    openai_model: str = "gpt-5.2",
    reasoning_effort: str = "low"
) -> PipelineResult:
    """
    Run the full 3-stage extraction pipeline with parallel chunk processing.

    Pipeline per chunk:
        Model (Draft) → GPT-5.1 (Critic) → GPT-4.1 (Fix)

    Multiple chunks are processed in parallel.

    Args:
        pdf_text_chunks: List of PDF page texts (one per page)
        topics: Topics dictionary
        meta_categories: Meta-categories dict (loaded if None)
        skip_critic: Skip Stage 2 (critic review)
        skip_fix: Skip Stage 3 (apply fixes)
        chunk_size: Pages per chunk (default 15)
        parallel_limit: Max concurrent chunk pipelines (default 3)
        provider: "gemini" or "openai" for draft extraction
        openai_model: OpenAI model (default: gpt-5.1)
        reasoning_effort: OpenAI reasoning effort (default: low)

    Returns:
        PipelineResult with merged extractions from all chunks
    """
    import time
    start_time = time.time()

    if meta_categories is None:
        meta_categories = load_meta_categories_from_json()

    total_pages = len(pdf_text_chunks)
    num_chunks = (total_pages + chunk_size - 1) // chunk_size

    provider_name = f"{openai_model}" if provider == "openai" else "Gemini"
    print(f"🔄 Multi-Model Pipeline: {total_pages} pages in {num_chunks} chunks")
    print(f"   Provider: {provider_name}, Parallel limit: {parallel_limit}, Chunk size: {chunk_size}")

    # Create chunk tasks
    semaphore = asyncio.Semaphore(parallel_limit)

    async def process_with_semaphore(chunk_pages, chunk_idx, start_page):
        async with semaphore:
            return await process_single_chunk(
                chunk_pages=chunk_pages,
                chunk_index=chunk_idx,
                start_page=start_page,
                topics=topics,
                meta_categories=meta_categories,
                skip_critic=skip_critic,
                skip_fix=skip_fix,
                provider=provider,
                openai_model=openai_model,
                reasoning_effort=reasoning_effort
            )

    # Build tasks
    tasks = []
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min(start_idx + chunk_size, total_pages)
        chunk_pages = pdf_text_chunks[start_idx:end_idx]
        start_page = start_idx + 1  # 1-based page numbers

        task = process_with_semaphore(chunk_pages, i, start_page)
        tasks.append(task)

    # Run all chunks in parallel (limited by semaphore)
    chunk_results = await asyncio.gather(*tasks)

    # Merge results - parse each chunk individually with correct page numbers
    all_drafts = []
    all_finals = []
    all_issues = []
    all_pages = []  # Parsed PageData objects with correct page numbers
    total_elapsed = 0

    for result in sorted(chunk_results, key=lambda x: x["chunk_index"]):
        all_drafts.append(result["draft"])
        all_finals.append(result["final"])
        all_issues.extend(result["issues"])
        total_elapsed += result["elapsed_ms"]

        # Build original_pages dict for validation
        original_pages = {
            result["start_page"] + i: text
            for i, text in enumerate(result["original_pages"])
        }

        # Parse this chunk's final output with content validation
        chunk_result = parse_chunk_response_v2(
            result["final"],
            original_pages=original_pages,
            start_page=result["start_page"],
            expected_count=result["pages_count"]
        )

        # Log validation stats
        if chunk_result.warnings:
            print(f"  [Chunk {result['chunk_index']}] Validation warnings:")
            for w in chunk_result.warnings[:5]:  # Limit to first 5
                print(f"    - {w}")
        print(f"  [Chunk {result['chunk_index']}] Trusted: {chunk_result.trusted_count}, "
              f"Remapped: {chunk_result.remapped_count}, Dropped: {chunk_result.dropped_count}")

        # Check for missing pages and retry them individually
        parsed_page_nums = {p.page for p in chunk_result.pages}
        missing_pages = set(original_pages.keys()) - parsed_page_nums

        if missing_pages:
            print(f"  [Chunk {result['chunk_index']}] Retrying {len(missing_pages)} missing pages individually...")
            for missing_page in sorted(missing_pages):
                page_text = original_pages[missing_page]
                retry_result = await retry_single_page(
                    page_text=page_text,
                    page_num=missing_page,
                    topics=topics,
                    meta_categories=meta_categories
                )
                if retry_result:
                    chunk_result.pages.append(retry_result)
                    print(f"    ✓ Page {missing_page} recovered")
                else:
                    print(f"    ✗ Page {missing_page} failed retry")

        all_pages.extend(chunk_result.pages)

    merged_draft = "\n\n".join(all_drafts)
    merged_final = "\n\n".join(all_finals)

    elapsed_ms = int((time.time() - start_time) * 1000)

    stages = ["gemini_draft"]
    if not skip_critic:
        stages.append("gpt51_critic")
        if all_issues and not skip_fix:
            stages.append("gpt41_fix")

    print(f"✓ Pipeline complete: {len(all_issues)} total issues, {elapsed_ms}ms")

    return PipelineResult(
        draft_extraction={"raw": merged_draft},
        critique=all_issues,
        final_extraction={"raw": merged_final},
        stages_completed=stages,
        total_issues_found=len(all_issues),
        total_issues_fixed=len(all_issues) if "gpt41_fix" in stages else 0,
        processing_time_ms=elapsed_ms,
        parsed_pages=all_pages
    )


async def run_extraction_pipeline_streaming(
    pdf_text_chunks: List[str],
    topics: List[Dict],
    meta_categories: Dict = None,
    chunk_size: int = CHUNK_SIZE,
    parallel_limit: int = PARALLEL_LIMIT
) -> AsyncGenerator[str, None]:
    """
    Run pipeline with SSE streaming for real-time progress.

    Uses parallel chunk processing - each chunk goes through all 3 stages.

    Yields:
        JSON SSE events with progress updates
    """
    import time
    start_time = time.time()

    if meta_categories is None:
        meta_categories = load_meta_categories_from_json()

    total_pages = len(pdf_text_chunks)
    num_chunks = (total_pages + chunk_size - 1) // chunk_size

    yield json.dumps({
        "type": "info",
        "message": f"Processing {total_pages} pages in {num_chunks} chunks (parallel limit: {parallel_limit})"
    }) + "\n\n"

    # Track progress
    chunks_completed = 0
    all_issues = []
    all_finals = []

    # Process chunks with progress tracking
    semaphore = asyncio.Semaphore(parallel_limit)
    results_queue = asyncio.Queue()

    async def process_chunk_with_progress(chunk_pages, chunk_idx, start_page):
        async with semaphore:
            result = await process_single_chunk(
                chunk_pages=chunk_pages,
                chunk_index=chunk_idx,
                start_page=start_page,
                topics=topics,
                meta_categories=meta_categories,
                skip_critic=False,
                skip_fix=False
            )
            await results_queue.put(result)
            return result

    # Create and start all tasks
    tasks = []
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min(start_idx + chunk_size, total_pages)
        chunk_pages = pdf_text_chunks[start_idx:end_idx]
        start_page = start_idx + 1

        task = asyncio.create_task(
            process_chunk_with_progress(chunk_pages, i, start_page)
        )
        tasks.append(task)

    # Stream progress as chunks complete
    chunk_results = []
    while chunks_completed < num_chunks:
        try:
            result = await asyncio.wait_for(results_queue.get(), timeout=1.0)
            chunks_completed += 1
            chunk_results.append(result)

            yield json.dumps({
                "type": "chunk_complete",
                "chunk": result["chunk_index"],
                "pages": f"{result['start_page']}-{result['start_page'] + result['pages_count'] - 1}",
                "issues_found": len(result["issues"]),
                "chunks_done": chunks_completed,
                "total_chunks": num_chunks,
                "percent": int(chunks_completed / num_chunks * 100)
            }) + "\n\n"

        except asyncio.TimeoutError:
            # Just continue waiting
            continue

    # Wait for all tasks to complete (should already be done)
    await asyncio.gather(*tasks)

    # Merge results in order - parse each chunk with content validation
    chunk_results.sort(key=lambda x: x["chunk_index"])
    all_pages = []
    total_trusted = 0
    total_remapped = 0
    total_dropped = 0

    for result in chunk_results:
        all_finals.append(result["final"])
        all_issues.extend(result["issues"])

        # Build original_pages dict for validation
        original_pages = {
            result["start_page"] + i: text
            for i, text in enumerate(result["original_pages"])
        }

        # Parse this chunk's output with content validation
        chunk_result = parse_chunk_response_v2(
            result["final"],
            original_pages=original_pages,
            start_page=result["start_page"],
            expected_count=result["pages_count"]
        )

        total_trusted += chunk_result.trusted_count
        total_remapped += chunk_result.remapped_count
        total_dropped += chunk_result.dropped_count

        all_pages.extend(chunk_result.pages)

    merged_final = "\n\n".join(all_finals)
    elapsed_ms = int((time.time() - start_time) * 1000)

    # Sort pages by page number
    all_pages.sort(key=lambda p: p.page)

    # Final result - include parsed pages count and validation stats
    yield json.dumps({
        "type": "done",
        "final_extraction": merged_final,
        "issues_found": len(all_issues),
        "issues_fixed": len(all_issues),
        "total_pages": total_pages,
        "parsed_pages_count": len(all_pages),
        "chunks_processed": num_chunks,
        "processing_time_ms": elapsed_ms,
        "validation": {
            "trusted": total_trusted,
            "remapped": total_remapped,
            "dropped": total_dropped
        }
    }) + "\n\n"


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def parse_pipeline_result(result: PipelineResult, start_page: int = 1) -> List[PageData]:
    """
    Parse the final extraction into PageData objects.

    Args:
        result: Pipeline result
        start_page: Starting page number (ignored if parsed_pages available)

    Returns:
        List of PageData objects
    """
    # Use pre-parsed pages if available (correctly parsed per-chunk with right page numbers)
    if result.parsed_pages:
        # Sort by page number to ensure correct order
        return sorted(result.parsed_pages, key=lambda p: p.page)

    # Fallback: parse from raw text (legacy behavior, no validation)
    import re
    final_text = result.final_extraction.get("raw", "")
    pattern = r'\[PAGE_START(?::\s*(\d+))?\](.*?)\[PAGE_END\]'
    matches = re.findall(pattern, final_text, re.DOTALL)

    pages = []
    for i, (claimed_str, block_content) in enumerate(matches):
        page_num = int(claimed_str) if claimed_str else start_page + i
        page_data = parse_page_block_v2(block_content, page_num)
        pages.append(page_data)

    return sorted(pages, key=lambda p: p.page)


def load_topics_from_db(db_path: str = None) -> List[Dict]:
    """
    Load topics from database.

    Args:
        db_path: Path to database (uses default if None)

    Returns:
        List of topic dictionaries
    """
    import sqlite3
    from pathlib import Path

    if db_path is None:
        db_path = Path(__file__).parent.parent.parent / "data" / "db" / "reference.db"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT name, category, aliases, description, icd10_patterns, cpt_patterns, hcpcs_patterns
        FROM topics_dictionary
    """)

    topics = []
    for row in cursor.fetchall():
        topic = {
            "name": row["name"],
            "category": row["category"],
            "description": row["description"]
        }
        if row["aliases"]:
            topic["aliases"] = json.loads(row["aliases"])
        if row["icd10_patterns"]:
            topic["icd10_patterns"] = json.loads(row["icd10_patterns"])
        if row["cpt_patterns"]:
            topic["cpt_patterns"] = json.loads(row["cpt_patterns"])
        if row["hcpcs_patterns"]:
            topic["hcpcs_patterns"] = json.loads(row["hcpcs_patterns"])
        topics.append(topic)

    conn.close()
    return topics
