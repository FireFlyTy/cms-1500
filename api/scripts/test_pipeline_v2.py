#!/usr/bin/env python3
"""
Pipeline V2 Test Script

Tests the multi-model extraction pipeline and compares with V1.
Measures timing and extraction quality metrics.

Usage:
    python api/scripts/test_pipeline_v2.py [--pdf PATH] [--v1-only] [--v2-only] [--skip-critic]
"""

import sys
import os
import json
import time
import asyncio
import argparse
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pypdf import PdfReader

# V1 imports
from src.parsers.document_parser import (
    parse_chunk_response,
    merge_chunk_results,
    build_document_data,
    get_chunk_prompt,
    PageData
)

# V2 imports
from src.parsers.multi_model_pipeline import (
    run_extraction_pipeline,
    load_topics_from_db,
    parse_pipeline_result,
    load_meta_categories_from_json,
    CHUNK_SIZE,
    PARALLEL_LIMIT
)

from src.generators.core_ai import call_gemini_model


# ============================================================
# METRICS
# ============================================================

@dataclass
class ChunkMetrics:
    chunk_index: int
    pages: str  # "1-15"
    stage1_ms: int  # Gemini
    stage2_ms: int  # GPT-5.1 critic
    stage3_ms: int  # GPT-4.1 fix
    total_ms: int
    issues_found: int


@dataclass
class PipelineMetrics:
    pipeline_version: str  # "V1" or "V2"
    total_pages: int
    num_chunks: int
    chunk_size: int
    parallel_limit: int

    # Timing
    total_time_ms: int
    avg_chunk_time_ms: float
    chunks_metrics: List[ChunkMetrics]

    # Extraction quality
    codes_extracted: int
    topics_extracted: int
    medications_extracted: int
    content_pages: int
    skipped_pages: int

    # V2 specific
    total_issues_found: int
    total_issues_fixed: int

    # Raw data paths
    output_file: str


@dataclass
class ComparisonResult:
    pdf_path: str
    pdf_pages: int
    test_timestamp: str

    v1_metrics: Optional[PipelineMetrics]
    v2_metrics: Optional[PipelineMetrics]

    # Comparison
    time_diff_ms: int  # V2 - V1 (negative = V2 faster)
    time_diff_percent: float
    codes_diff: int  # V2 - V1
    quality_notes: List[str]


# ============================================================
# V1 PIPELINE (Gemini only, parallel chunks)
# ============================================================

async def run_v1_pipeline(
    pdf_path: str,
    chunk_size: int = 15,
    parallel_limit: int = 5
) -> PipelineMetrics:
    """Run V1 pipeline (Gemini only) and collect metrics."""

    print("\n" + "="*60)
    print("V1 PIPELINE (Gemini only)")
    print("="*60)

    start_time = time.time()

    # Read PDF
    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)
    num_chunks = (total_pages + chunk_size - 1) // chunk_size

    print(f"Pages: {total_pages}, Chunks: {num_chunks}")

    # Extract text from all pages
    page_texts = []
    for page in reader.pages:
        text = page.extract_text() or ""
        page_texts.append(text)

    # Process chunks
    semaphore = asyncio.Semaphore(parallel_limit)
    chunk_metrics = []
    all_pages = []

    async def process_chunk(chunk_idx: int, start_page: int, pages: List[str]):
        async with semaphore:
            chunk_start = time.time()

            # Build prompt
            prompt = get_chunk_prompt(len(pages), start_page)
            prompt += "\n\n=== PDF CONTENT ===\n\n"
            for i, text in enumerate(pages):
                prompt += f"--- Page {start_page + i} ---\n{text}\n\n"

            # Call Gemini
            response = await call_gemini_model(prompt, thinking_budget=2048)

            # Parse response
            parsed_pages = parse_chunk_response(response, start_page, len(pages))

            chunk_time = int((time.time() - chunk_start) * 1000)

            print(f"  [Chunk {chunk_idx}] Pages {start_page}-{start_page+len(pages)-1}: {chunk_time}ms")

            return {
                "chunk_idx": chunk_idx,
                "start_page": start_page,
                "pages": parsed_pages,
                "metrics": ChunkMetrics(
                    chunk_index=chunk_idx,
                    pages=f"{start_page}-{start_page+len(pages)-1}",
                    stage1_ms=chunk_time,
                    stage2_ms=0,
                    stage3_ms=0,
                    total_ms=chunk_time,
                    issues_found=0
                )
            }

    # Create tasks
    tasks = []
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min(start_idx + chunk_size, total_pages)
        chunk_pages = page_texts[start_idx:end_idx]
        start_page = start_idx + 1

        tasks.append(process_chunk(i, start_page, chunk_pages))

    # Run all
    results = await asyncio.gather(*tasks)

    # Merge
    results.sort(key=lambda x: x["chunk_idx"])
    for r in results:
        all_pages.extend(r["pages"])
        chunk_metrics.append(r["metrics"])

    # Build document
    file_hash = f"v1_test_{int(time.time())}"
    doc = build_document_data(
        file_hash=file_hash,
        filename=os.path.basename(pdf_path),
        total_pages=total_pages,
        pages=all_pages
    )

    total_time = int((time.time() - start_time) * 1000)

    # Save output
    output_dir = Path("data/processed/pipeline_tests")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"v1_{file_hash}.json"

    with open(output_file, 'w') as f:
        json.dump(doc.to_dict(), f, indent=2)

    metrics = PipelineMetrics(
        pipeline_version="V1",
        total_pages=total_pages,
        num_chunks=num_chunks,
        chunk_size=chunk_size,
        parallel_limit=parallel_limit,
        total_time_ms=total_time,
        avg_chunk_time_ms=sum(c.total_ms for c in chunk_metrics) / len(chunk_metrics),
        chunks_metrics=chunk_metrics,
        codes_extracted=len(doc.summary['all_codes']),
        topics_extracted=len(doc.summary['topics']),
        medications_extracted=len(doc.summary['medications']),
        content_pages=doc.summary['content_page_count'],
        skipped_pages=len(doc.summary['skipped_pages']),
        total_issues_found=0,
        total_issues_fixed=0,
        output_file=str(output_file)
    )

    print(f"\nV1 Complete: {total_time}ms")
    print(f"  Codes: {metrics.codes_extracted}")
    print(f"  Topics: {metrics.topics_extracted}")
    print(f"  Medications: {metrics.medications_extracted}")
    print(f"  Content pages: {metrics.content_pages}/{total_pages}")

    return metrics


# ============================================================
# V2 PIPELINE (Gemini + GPT-5.1 + GPT-4.1)
# ============================================================

async def run_v2_pipeline(
    pdf_path: str,
    skip_critic: bool = False,
    skip_fix: bool = False,
    chunk_size: int = CHUNK_SIZE,
    parallel_limit: int = PARALLEL_LIMIT
) -> PipelineMetrics:
    """Run V2 multi-model pipeline and collect metrics."""

    print("\n" + "="*60)
    print("V2 PIPELINE (Gemini + GPT-5.1 + GPT-4.1)")
    print("="*60)

    if skip_critic:
        print("  [!] Critic stage SKIPPED")

    start_time = time.time()

    # Read PDF
    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)
    num_chunks = (total_pages + chunk_size - 1) // chunk_size

    print(f"Pages: {total_pages}, Chunks: {num_chunks}")

    # Extract text
    page_texts = []
    for page in reader.pages:
        text = page.extract_text() or ""
        page_texts.append(text)

    # Load topics and meta-categories
    topics = load_topics_from_db()
    meta_categories = load_meta_categories_from_json()

    print(f"Loaded {len(topics)} topics, {sum(len(c['categories']) for c in meta_categories.values())} meta-categories")

    # Run V2 pipeline
    result = await run_extraction_pipeline(
        pdf_text_chunks=page_texts,
        topics=topics,
        meta_categories=meta_categories,
        skip_critic=skip_critic,
        skip_fix=skip_fix,
        chunk_size=chunk_size,
        parallel_limit=parallel_limit
    )

    # Parse results
    pages = parse_pipeline_result(result, start_page=1)

    # Build document
    file_hash = f"v2_test_{int(time.time())}"
    doc = build_document_data(
        file_hash=file_hash,
        filename=os.path.basename(pdf_path),
        total_pages=total_pages,
        pages=pages
    )

    total_time = int((time.time() - start_time) * 1000)

    # Save output
    output_dir = Path("data/processed/pipeline_tests")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"v2_{file_hash}.json"

    # Convert issues to dicts for JSON
    from dataclasses import asdict
    issues_list = [asdict(issue) for issue in result.critique] if result.critique else []

    with open(output_file, 'w') as f:
        json.dump({
            "document": doc.to_dict(),
            "pipeline_result": {
                "stages_completed": result.stages_completed,
                "total_issues_found": result.total_issues_found,
                "total_issues_fixed": result.total_issues_fixed,
                "processing_time_ms": result.processing_time_ms,
                "issues": issues_list
            }
        }, f, indent=2)

    # Note: V2 doesn't track per-chunk metrics as detailed, using aggregate
    chunk_metrics = [
        ChunkMetrics(
            chunk_index=i,
            pages=f"{i*chunk_size+1}-{min((i+1)*chunk_size, total_pages)}",
            stage1_ms=0,
            stage2_ms=0,
            stage3_ms=0,
            total_ms=result.processing_time_ms // num_chunks,
            issues_found=result.total_issues_found // num_chunks if num_chunks > 0 else 0
        )
        for i in range(num_chunks)
    ]

    metrics = PipelineMetrics(
        pipeline_version="V2",
        total_pages=total_pages,
        num_chunks=num_chunks,
        chunk_size=chunk_size,
        parallel_limit=parallel_limit,
        total_time_ms=total_time,
        avg_chunk_time_ms=result.processing_time_ms / num_chunks if num_chunks > 0 else 0,
        chunks_metrics=chunk_metrics,
        codes_extracted=len(doc.summary['all_codes']),
        topics_extracted=len(doc.summary['topics']),
        medications_extracted=len(doc.summary['medications']),
        content_pages=doc.summary['content_page_count'],
        skipped_pages=len(doc.summary['skipped_pages']),
        total_issues_found=result.total_issues_found,
        total_issues_fixed=result.total_issues_fixed,
        output_file=str(output_file)
    )

    print(f"\nV2 Complete: {total_time}ms")
    print(f"  Codes: {metrics.codes_extracted}")
    print(f"  Topics: {metrics.topics_extracted}")
    print(f"  Medications: {metrics.medications_extracted}")
    print(f"  Content pages: {metrics.content_pages}/{total_pages}")
    print(f"  Issues found/fixed: {metrics.total_issues_found}/{metrics.total_issues_fixed}")

    return metrics


# ============================================================
# COMPARISON
# ============================================================

def compare_results(
    pdf_path: str,
    v1_metrics: Optional[PipelineMetrics],
    v2_metrics: Optional[PipelineMetrics]
) -> ComparisonResult:
    """Compare V1 and V2 results."""

    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)

    notes = []
    time_diff = 0
    time_diff_pct = 0
    codes_diff = 0

    if v1_metrics and v2_metrics:
        time_diff = v2_metrics.total_time_ms - v1_metrics.total_time_ms
        time_diff_pct = (time_diff / v1_metrics.total_time_ms) * 100 if v1_metrics.total_time_ms > 0 else 0
        codes_diff = v2_metrics.codes_extracted - v1_metrics.codes_extracted

        if time_diff < 0:
            notes.append(f"V2 is {abs(time_diff)}ms ({abs(time_diff_pct):.1f}%) FASTER")
        else:
            notes.append(f"V2 is {time_diff}ms ({time_diff_pct:.1f}%) SLOWER")

        if codes_diff > 0:
            notes.append(f"V2 extracted {codes_diff} MORE codes")
        elif codes_diff < 0:
            notes.append(f"V2 extracted {abs(codes_diff)} FEWER codes")

        if v2_metrics.total_issues_found > 0:
            notes.append(f"V2 critic found {v2_metrics.total_issues_found} issues")

    return ComparisonResult(
        pdf_path=pdf_path,
        pdf_pages=total_pages,
        test_timestamp=datetime.now().isoformat(),
        v1_metrics=v1_metrics,
        v2_metrics=v2_metrics,
        time_diff_ms=time_diff,
        time_diff_percent=time_diff_pct,
        codes_diff=codes_diff,
        quality_notes=notes
    )


def print_comparison(comparison: ComparisonResult):
    """Print comparison results."""

    print("\n" + "="*60)
    print("COMPARISON RESULTS")
    print("="*60)
    print(f"PDF: {comparison.pdf_path}")
    print(f"Pages: {comparison.pdf_pages}")
    print(f"Timestamp: {comparison.test_timestamp}")

    if comparison.v1_metrics:
        v1 = comparison.v1_metrics
        print(f"\n--- V1 (Gemini only) ---")
        print(f"  Time: {v1.total_time_ms}ms ({v1.total_time_ms/1000:.1f}s)")
        print(f"  Codes: {v1.codes_extracted}")
        print(f"  Topics: {v1.topics_extracted}")
        print(f"  Medications: {v1.medications_extracted}")
        print(f"  Content pages: {v1.content_pages}")

    if comparison.v2_metrics:
        v2 = comparison.v2_metrics
        print(f"\n--- V2 (Gemini + GPT-5.1 + GPT-4.1) ---")
        print(f"  Time: {v2.total_time_ms}ms ({v2.total_time_ms/1000:.1f}s)")
        print(f"  Codes: {v2.codes_extracted}")
        print(f"  Topics: {v2.topics_extracted}")
        print(f"  Medications: {v2.medications_extracted}")
        print(f"  Content pages: {v2.content_pages}")
        print(f"  Issues found: {v2.total_issues_found}")
        print(f"  Issues fixed: {v2.total_issues_fixed}")

    if comparison.quality_notes:
        print(f"\n--- Summary ---")
        for note in comparison.quality_notes:
            print(f"  • {note}")


def save_comparison(comparison: ComparisonResult, output_path: str = None):
    """Save comparison to JSON."""

    if output_path is None:
        output_dir = Path("data/processed/pipeline_tests")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"comparison_{int(time.time())}.json"

    # Convert dataclasses to dicts
    data = {
        "pdf_path": comparison.pdf_path,
        "pdf_pages": comparison.pdf_pages,
        "test_timestamp": comparison.test_timestamp,
        "time_diff_ms": comparison.time_diff_ms,
        "time_diff_percent": comparison.time_diff_percent,
        "codes_diff": comparison.codes_diff,
        "quality_notes": comparison.quality_notes,
    }

    if comparison.v1_metrics:
        data["v1_metrics"] = {
            "pipeline_version": comparison.v1_metrics.pipeline_version,
            "total_time_ms": comparison.v1_metrics.total_time_ms,
            "codes_extracted": comparison.v1_metrics.codes_extracted,
            "topics_extracted": comparison.v1_metrics.topics_extracted,
            "medications_extracted": comparison.v1_metrics.medications_extracted,
            "content_pages": comparison.v1_metrics.content_pages,
            "output_file": comparison.v1_metrics.output_file
        }

    if comparison.v2_metrics:
        data["v2_metrics"] = {
            "pipeline_version": comparison.v2_metrics.pipeline_version,
            "total_time_ms": comparison.v2_metrics.total_time_ms,
            "codes_extracted": comparison.v2_metrics.codes_extracted,
            "topics_extracted": comparison.v2_metrics.topics_extracted,
            "medications_extracted": comparison.v2_metrics.medications_extracted,
            "content_pages": comparison.v2_metrics.content_pages,
            "total_issues_found": comparison.v2_metrics.total_issues_found,
            "total_issues_fixed": comparison.v2_metrics.total_issues_fixed,
            "output_file": comparison.v2_metrics.output_file
        }

    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"\nComparison saved to: {output_path}")
    return str(output_path)


# ============================================================
# MAIN
# ============================================================

async def main():
    parser = argparse.ArgumentParser(description="Test Pipeline V2")
    parser.add_argument("--pdf", type=str,
                       default="data/raw/documents/codebooks/icd_10_cm_october_2025_guidelines_0.pdf",
                       help="Path to PDF file")
    parser.add_argument("--v1-only", action="store_true", help="Run V1 only")
    parser.add_argument("--v2-only", action="store_true", help="Run V2 only")
    parser.add_argument("--skip-critic", action="store_true", help="Skip GPT-5.1 critic in V2")
    parser.add_argument("--chunk-size", type=int, default=15, help="Pages per chunk")
    parser.add_argument("--parallel", type=int, default=3, help="Parallel limit for V2")

    args = parser.parse_args()

    if not os.path.exists(args.pdf):
        print(f"Error: PDF not found: {args.pdf}")
        sys.exit(1)

    print("="*60)
    print("PIPELINE COMPARISON TEST")
    print("="*60)
    print(f"PDF: {args.pdf}")
    print(f"Chunk size: {args.chunk_size}")
    print(f"Parallel limit: {args.parallel}")

    v1_metrics = None
    v2_metrics = None

    # Run V1
    if not args.v2_only:
        try:
            v1_metrics = await run_v1_pipeline(
                args.pdf,
                chunk_size=args.chunk_size,
                parallel_limit=5  # V1 can handle more parallel
            )
        except Exception as e:
            print(f"V1 Error: {e}")
            import traceback
            traceback.print_exc()

    # Run V2
    if not args.v1_only:
        try:
            v2_metrics = await run_v2_pipeline(
                args.pdf,
                skip_critic=args.skip_critic,
                chunk_size=args.chunk_size,
                parallel_limit=args.parallel
            )
        except Exception as e:
            print(f"V2 Error: {e}")
            import traceback
            traceback.print_exc()

    # Compare
    comparison = compare_results(args.pdf, v1_metrics, v2_metrics)
    print_comparison(comparison)
    save_comparison(comparison)


if __name__ == "__main__":
    asyncio.run(main())
