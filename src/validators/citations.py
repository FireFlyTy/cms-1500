"""
validators.py — Multi-Document Citation Verification

Верификация цитат для защиты от галлюцинаций LLM.
Поддержка нескольких источников с doc_id (первые 8 символов SHA256).

Usage:
    from validators import verify_citations, parse_sources_to_pages

    doc_pages = parse_sources_to_pages(sources_text)
    result = verify_citations(llm_output, doc_pages)

Features:
    - Multi-document support with doc_id tracking
    - Two-pass verification with auto-repair
    - Fuzzy matching with confidence scores
    - Cross-document phrase search
    - Automatic anchor repair for truncated/modified phrases
"""

import re
from typing import Dict, List, TypedDict, Optional, Tuple
from difflib import SequenceMatcher


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================

class CitationResult(TypedDict, total=False):
    """Result of verifying a single citation."""
    doc_id: str
    page: int
    phrase: str
    status: str  # "OK" | "NOT_FOUND" | "PAGE_MISSING" | "WRONG_PAGE" | "PAGE_OVERFLOW" | "AMBIGUOUS" | "INVALID_FORMAT" | "REPAIRED" | "DOC_ERROR" | "DOC_MISSING"
    reason: str
    confidence: float  # 0.0 - 1.0 confidence score
    match_type: str  # "exact" | "ngram" | "fuzzy" | "repaired"
    # Correction suggestions
    suggested_doc_id: str  # For DOC_ERROR
    suggested_page: int  # For WRONG_PAGE
    suggested_locations: List[Tuple[str, int]]  # For AMBIGUOUS: [(doc_id, page), ...]
    overflow_to: int  # For PAGE_OVERFLOW
    split_info: dict  # For PAGE_OVERFLOW details
    # Repair fields
    original_phrase: str  # Original phrase before repair
    repaired_phrase: str  # Repaired phrase
    repair_reason: str  # Why repair was needed


class RepairResult(TypedDict, total=False):
    """Result of attempting to repair a broken citation."""
    success: bool
    repaired_phrase: str
    confidence: float
    repair_type: str  # "truncated" | "modified" | "partial_match"
    original_in_source: str  # The actual text found in source
    found_doc_id: str  # Document where repair was found
    found_page: int  # Page where repair was found


class VerificationResult(TypedDict):
    """Aggregated result of verifying all citations."""
    verified: List[CitationResult]
    failed: List[CitationResult]
    wrong_page: List[CitationResult]
    wrong_doc: List[CitationResult]  # NEW: wrong document ID
    overflow: List[CitationResult]
    ambiguous: List[CitationResult]
    invalid_format: List[CitationResult]
    repaired: List[CitationResult]
    total: int
    success_rate: float
    status: str
    audit_summary: dict


# Type alias for document pages structure
# {doc_id: {page_num: text}}
DocumentPages = Dict[str, Dict[int, str]]


# =============================================================================
# PARSING FUNCTIONS
# =============================================================================

def parse_sources_to_pages(sources_text: str) -> DocumentPages:
    """
    Parse multi-document source text into structured format.
    
    Input format:
        === SOURCE: filename.pdf [doc_id: abc12345] ===
        ## Page 45
        content...
        
        ## Page 46
        content...
        
        === SOURCE: another.pdf [doc_id: def67890] ===
        ## Page 10
        content...
    
    Returns:
        {doc_id: {page_num: text, ...}, ...}
    """
    result: DocumentPages = {}
    
    # Pattern to match source headers
    source_pattern = re.compile(
        r'===\s*SOURCE:\s*([^\[]+)\[doc_id:\s*([a-f0-9]+)\s*\]\s*===',
        re.IGNORECASE
    )
    
    # Pattern to match page markers
    page_pattern = re.compile(
        r'(?:^|\n)(?:#{1,3}\s*|\*\*)?Page\s+(\d+)(?:\*\*)?[:\s]*\n',
        re.IGNORECASE | re.MULTILINE
    )
    
    # Find all source sections
    source_matches = list(source_pattern.finditer(sources_text))
    
    if not source_matches:
        # Fallback: treat as single document with unknown doc_id
        pages = _parse_single_doc_pages(sources_text)
        if pages:
            result["unknown"] = pages
        return result
    
    # Process each source section
    for i, source_match in enumerate(source_matches):
        filename = source_match.group(1).strip()
        doc_id = source_match.group(2).strip()
        
        # Determine section boundaries
        section_start = source_match.end()
        if i + 1 < len(source_matches):
            section_end = source_matches[i + 1].start()
        else:
            section_end = len(sources_text)
        
        section_text = sources_text[section_start:section_end]
        
        # Parse pages within this section
        pages = _parse_single_doc_pages(section_text)
        
        if pages:
            result[doc_id] = pages
    
    return result


def _parse_single_doc_pages(text: str) -> Dict[int, str]:
    """Parse page markers within a single document section."""
    pages: Dict[int, str] = {}
    
    page_pattern = re.compile(
        r'(?:^|\n)(?:#{1,3}\s*|\*\*)?Page\s+(\d+)(?:\*\*)?[:\s]*\n',
        re.IGNORECASE | re.MULTILINE
    )
    
    matches = list(page_pattern.finditer(text))
    
    if not matches:
        # No page markers - treat entire text as page 1
        if text.strip():
            pages[1] = text.strip()
        return pages
    
    for i, match in enumerate(matches):
        page_num = int(match.group(1))
        start_pos = match.end()
        
        if i + 1 < len(matches):
            end_pos = matches[i + 1].start()
        else:
            end_pos = len(text)
        
        page_text = text[start_pos:end_pos].strip()
        pages[page_num] = page_text
    
    return pages


def get_doc_id_from_full_id(full_id: str) -> str:
    """Extract short doc_id (first 8 chars) from full SHA256 hash."""
    return full_id[:8] if len(full_id) >= 8 else full_id


# =============================================================================
# TEXT NORMALIZATION & MATCHING
# =============================================================================

def normalize_for_search(text: str) -> str:
    """Normalize text for fuzzy search."""
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text


def get_ngrams(text: str, n: int = 4) -> list:
    """Extract n-grams (sequences of n words)."""
    words = normalize_for_search(text).split()
    words = [w for w in words if len(w) >= 2]
    
    if len(words) < n:
        return [tuple(words)] if words else []
    
    return [tuple(words[i:i+n]) for i in range(len(words) - n + 1)]


def calculate_ngram_match(phrase: str, page_text: str, n: int = 4) -> float:
    """Calculate ratio of phrase n-grams found in page text."""
    phrase_ngrams = get_ngrams(phrase, n)
    page_ngrams = set(get_ngrams(page_text, n))
    
    if not phrase_ngrams:
        return 0.0
    
    matched = sum(1 for ng in phrase_ngrams if ng in page_ngrams)
    return matched / len(phrase_ngrams)


def calculate_similarity(s1: str, s2: str) -> float:
    """Calculate similarity ratio between two strings."""
    return SequenceMatcher(
        None, 
        normalize_for_search(s1), 
        normalize_for_search(s2)
    ).ratio()


def find_best_match_in_text(
    phrase: str, 
    page_text: str, 
    min_similarity: float = 0.6
) -> Optional[Tuple[str, float]]:
    """
    Find the best matching substring in page_text for the given phrase.
    
    Returns:
        (matched_text, similarity_score) or None if no good match
    """
    normalized_phrase = normalize_for_search(phrase)
    phrase_words = normalized_phrase.split()
    phrase_len = len(phrase_words)
    
    if phrase_len < 3:
        return None
    
    normalized_page = normalize_for_search(page_text)
    page_words = normalized_page.split()
    
    best_match = None
    best_score = min_similarity
    
    # Sliding window approach
    window_sizes = [phrase_len, phrase_len + 2, phrase_len - 2, phrase_len + 4]
    window_sizes = [w for w in window_sizes if w > 2 and w <= len(page_words)]
    
    for window_size in window_sizes:
        for i in range(len(page_words) - window_size + 1):
            window = ' '.join(page_words[i:i + window_size])
            score = calculate_similarity(normalized_phrase, window)
            
            if score > best_score:
                best_score = score
                best_match = window
    
    if best_match and best_score >= min_similarity:
        return (best_match, best_score)
    return None


def is_short_ambiguous_phrase(phrase: str) -> bool:
    """Determine if phrase is short and potentially ambiguous."""
    words = normalize_for_search(phrase).split()
    return len(words) <= 8


# =============================================================================
# CITATION EXTRACTION
# =============================================================================

def extract_citations(llm_output: str) -> List[Tuple[str, int, str]]:
    """
    Extract all citations from finalized LLM output.
    
    Supported formats:
    - [[doc_id:page | "phrase"]]
    - [[doc_id:page, "phrase"]]
    - [[doc_id:pages 95-96 | "phrase"]] (range - take first page)
    
    Returns:
        List of (doc_id, page, phrase) tuples
    """
    citations = []
    
    # Pattern 1: Single page - [[doc_id:N | "phrase"]] or [[doc_id:N, "phrase"]]
    pattern_single = re.compile(
        r'\[\[([a-f0-9]+):(\d+)\s*[|,]\s*"([^"]+)"\]\]',
        re.IGNORECASE
    )
    for match in pattern_single.finditer(llm_output):
        doc_id = match.group(1)
        page = int(match.group(2))
        phrase = match.group(3)
        citations.append((doc_id, page, phrase))
    
    # Pattern 2: Page range - [[doc_id:pages 95-96 | "phrase"]]
    pattern_range = re.compile(
        r'\[\[([a-f0-9]+):pages?\s*(\d+)\s*-\s*(\d+)\s*[|,]\s*"([^"]+)"\]\]',
        re.IGNORECASE
    )
    for match in pattern_range.finditer(llm_output):
        doc_id = match.group(1)
        start_page = int(match.group(2))
        phrase = match.group(4)
        citations.append((doc_id, start_page, phrase))
    
    # Pattern 3: Legacy format (backward compatibility) - [[Page: N | "phrase"]]
    pattern_legacy = re.compile(
        r'\[\[Page:\s*(\d+)\s*[|,]\s*"([^"]+)"\]\]',
        re.IGNORECASE
    )
    for match in pattern_legacy.finditer(llm_output):
        page = int(match.group(1))
        phrase = match.group(2)
        # Use "unknown" doc_id for legacy citations
        citations.append(("unknown", page, phrase))
    
    # Deduplicate while preserving order
    seen = set()
    unique_citations = []
    for item in citations:
        key = (item[0], item[1], item[2][:50])  # doc_id + page + first 50 chars
        if key not in seen:
            seen.add(key)
            unique_citations.append(item)
    
    return unique_citations


def extract_citations_from_draft(llm_output: str) -> List[Tuple[str, int, str]]:
    """
    Extract citations from DRAFT format (Step 1).
    
    Formats:
    - 1. [doc_id] Page N. `"quote"`
    - 1. [doc_id] Page N. "quote"
    
    Returns:
        List of (doc_id, page, phrase) tuples
    """
    citations = []
    
    # Pattern 1: With doc_id - [doc_id] Page N. `"quote"` or "quote"
    pattern_with_docid = re.compile(
        r'^\s*\d+\.\s*\[([a-f0-9]+)\]\s*Page\s+(\d+)\.\s*[`"]+"?([^`"]+)"?[`"]+',
        re.MULTILINE | re.IGNORECASE
    )
    
    matches = pattern_with_docid.findall(llm_output)
    if matches:
        citations.extend([
            (doc_id.strip(), int(page), phrase.strip()) 
            for doc_id, page, phrase in matches
        ])
        return citations
    
    # Pattern 2: Without doc_id (legacy) - Page N. `"quote"`
    pattern_legacy = re.compile(
        r'^\s*\d+\.\s*Page\s+(\d+)\.\s*[`"]+"?([^`"]+)"?[`"]+',
        re.MULTILINE | re.IGNORECASE
    )
    
    matches = pattern_legacy.findall(llm_output)
    if matches:
        citations.extend([
            ("unknown", int(page), phrase.strip()) 
            for page, phrase in matches
        ])
        return citations
    
    # Pattern 3: Unquoted format
    pattern_unquoted = re.compile(
        r'^\s*\d+\.\s*\[([a-f0-9]+)\]\s*Page\s+(\d+)\.\s*(.+?)(?=^\s*\d+\.\s*\[|\Z)',
        re.MULTILINE | re.DOTALL | re.IGNORECASE
    )
    
    matches = pattern_unquoted.findall(llm_output)
    if matches:
        for doc_id, page, text in matches:
            phrase = text.strip()
            words = phrase.split()
            if len(words) > 20:
                phrase = ' '.join(words[:15])
            elif len(words) > 10:
                phrase = ' '.join(words[:10])
            citations.append((doc_id.strip(), int(page), phrase))
    
    return citations


# =============================================================================
# CROSS-DOCUMENT SEARCH
# =============================================================================

def find_phrase_in_all_documents(
    phrase: str,
    doc_pages: DocumentPages,
    exclude_doc_id: str = None,
    exclude_page: int = None
) -> List[Tuple[str, int, float]]:
    """
    Find phrase across ALL documents and pages.
    
    Returns:
        List of (doc_id, page_num, match_ratio) sorted by ratio descending
    """
    results = []
    normalized_phrase = normalize_for_search(phrase)
    
    for doc_id, pages in doc_pages.items():
        if exclude_doc_id and doc_id == exclude_doc_id:
            continue
            
        for page_num, page_text in pages.items():
            if exclude_doc_id == doc_id and exclude_page == page_num:
                continue
            
            normalized_page = normalize_for_search(page_text)
            
            # Exact match
            if normalized_phrase in normalized_page:
                results.append((doc_id, page_num, 1.0))
                continue
            
            # N-gram match
            ngram_ratio = calculate_ngram_match(phrase, page_text, n=4)
            if ngram_ratio >= 0.6:
                results.append((doc_id, page_num, ngram_ratio))
    
    results.sort(key=lambda x: x[2], reverse=True)
    return results


def find_all_occurrences(
    phrase: str,
    doc_pages: DocumentPages,
    min_ratio: float = 0.6
) -> List[dict]:
    """
    Find ALL occurrences of phrase with context.
    
    Returns:
        List of {doc_id, page, ratio, match_type, context}
    """
    results = []
    normalized_phrase = normalize_for_search(phrase)
    phrase_words = len(normalized_phrase.split())
    
    for doc_id, pages in doc_pages.items():
        for page_num, page_text in pages.items():
            normalized_page = normalize_for_search(page_text)
            
            # Exact match
            if normalized_phrase in normalized_page:
                idx = normalized_page.find(normalized_phrase)
                start = max(0, idx - 60)
                end = min(len(normalized_page), idx + len(normalized_phrase) + 60)
                context = (
                    ("..." if start > 0 else "") + 
                    normalized_page[start:end] + 
                    ("..." if end < len(normalized_page) else "")
                )
                
                results.append({
                    "doc_id": doc_id,
                    "page": page_num,
                    "ratio": 1.0,
                    "match_type": "exact",
                    "context": context
                })
                continue
            
            # N-gram match for longer phrases
            if phrase_words >= 6:
                ratio = calculate_ngram_match(phrase, page_text, n=4)
                if ratio >= min_ratio:
                    context = normalized_page[:120] + "..."
                    results.append({
                        "doc_id": doc_id,
                        "page": page_num,
                        "ratio": ratio,
                        "match_type": "ngram",
                        "context": context
                    })
    
    results.sort(key=lambda x: x["ratio"], reverse=True)
    return results


# =============================================================================
# PAGE OVERFLOW DETECTION
# =============================================================================

def detect_page_overflow(
    phrase: str,
    current_page: str,
    next_page: str,
    n: int = 4
) -> dict:
    """Detect if citation spans across pages."""
    phrase_ngrams = get_ngrams(phrase, n)
    
    if len(phrase_ngrams) < 4:
        return {"is_overflow": False, "reason": "Quote too short for overflow detection"}
    
    current_ngrams = set(get_ngrams(current_page, n))
    next_ngrams = set(get_ngrams(next_page, n)) if next_page else set()
    
    if not next_ngrams:
        return {"is_overflow": False, "reason": "No next page"}
    
    best_split = None
    best_score = 0
    
    for split in range(2, len(phrase_ngrams) - 1):
        first_part = phrase_ngrams[:split]
        second_part = phrase_ngrams[split:]
        
        first_match = sum(1 for ng in first_part if ng in current_ngrams) / len(first_part)
        second_match = sum(1 for ng in second_part if ng in next_ngrams) / len(second_part)
        
        if first_match >= 0.6 and second_match >= 0.6:
            combined_score = (
                first_match * len(first_part) + second_match * len(second_part)
            ) / len(phrase_ngrams)
            
            if combined_score > best_score:
                best_score = combined_score
                best_split = {
                    "split_index": split,
                    "first_match": first_match,
                    "second_match": second_match,
                    "combined_score": combined_score
                }
    
    if best_split and best_score >= 0.7:
        return {"is_overflow": True, **best_split}
    
    return {"is_overflow": False}


# =============================================================================
# SINGLE CITATION VERIFICATION
# =============================================================================

def verify_single_citation(
    doc_id: str,
    page_num: int,
    phrase: str,
    doc_pages: DocumentPages,
    draft_doc_id: Optional[str] = None,
    draft_page: Optional[int] = None
) -> CitationResult:
    """
    Verify a single citation against source documents.
    
    Args:
        doc_id: Document ID from citation
        page_num: Page number from citation
        phrase: Anchor phrase text
        doc_pages: All document pages {doc_id: {page: text}}
        draft_doc_id: Original doc_id from Draft (for ambiguous resolution)
        draft_page: Original page from Draft (for ambiguous resolution)
    """
    
    # Check 0: Detect stitched citations with ... in middle
    if '...' in phrase or '…' in phrase:
        stripped = phrase.strip()
        if not stripped.startswith('...') and not stripped.startswith('…'):
            if not stripped.endswith('...') and not stripped.endswith('…'):
                if '...' in stripped[3:-3] or '…' in stripped[3:-3]:
                    return {
                        "doc_id": doc_id,
                        "page": page_num,
                        "phrase": phrase,
                        "status": "INVALID_FORMAT",
                        "reason": "Citation contains '...' in the middle - indicates concatenated phrase.",
                        "confidence": 0.0,
                        "match_type": "invalid"
                    }
    
    phrase_words = len([w for w in normalize_for_search(phrase).split() if len(w) >= 2])
    is_short_quote = phrase_words < 8
    
    # Check 1: Does document exist?
    if doc_id not in doc_pages:
        # Search in all documents
        found_locations = find_phrase_in_all_documents(phrase, doc_pages)
        
        if found_locations:
            if len(found_locations) > 1 and is_short_quote:
                return {
                    "doc_id": doc_id,
                    "page": page_num,
                    "phrase": phrase,
                    "status": "AMBIGUOUS",
                    "reason": f"Document '{doc_id}' not found. Phrase on multiple locations.",
                    "suggested_locations": [(loc[0], loc[1]) for loc in found_locations],
                    "confidence": 0.5,
                    "match_type": "ambiguous"
                }
            
            best = found_locations[0]
            return {
                "doc_id": doc_id,
                "page": page_num,
                "phrase": phrase,
                "status": "DOC_MISSING",
                "reason": f"Document '{doc_id}' not found. Phrase exists in [{best[0]}] Page {best[1]}",
                "suggested_doc_id": best[0],
                "suggested_page": best[1],
                "confidence": 0.7,
                "match_type": "wrong_doc"
            }
        
        return {
            "doc_id": doc_id,
            "page": page_num,
            "phrase": phrase,
            "status": "DOC_MISSING",
            "reason": f"Document '{doc_id}' not found and phrase not in any document",
            "confidence": 0.0,
            "match_type": "missing"
        }
    
    pages = doc_pages[doc_id]
    
    # Check 2: Does page exist in this document?
    if page_num not in pages:
        # Search in other pages of same document first
        found_in_doc = []
        for pg_num, pg_text in pages.items():
            normalized_page = normalize_for_search(pg_text)
            normalized_phrase = normalize_for_search(phrase)
            
            if normalized_phrase in normalized_page:
                found_in_doc.append((pg_num, 1.0))
            else:
                ngram_ratio = calculate_ngram_match(phrase, pg_text, n=4)
                if ngram_ratio >= 0.6:
                    found_in_doc.append((pg_num, ngram_ratio))
        
        if found_in_doc:
            found_in_doc.sort(key=lambda x: x[1], reverse=True)
            
            if len(found_in_doc) > 1 and is_short_quote:
                return {
                    "doc_id": doc_id,
                    "page": page_num,
                    "phrase": phrase,
                    "status": "AMBIGUOUS",
                    "reason": f"Page {page_num} not found. Short quote on multiple pages.",
                    "suggested_locations": [(doc_id, pg[0]) for pg in found_in_doc],
                    "confidence": 0.5,
                    "match_type": "ambiguous"
                }
            
            return {
                "doc_id": doc_id,
                "page": page_num,
                "phrase": phrase,
                "status": "WRONG_PAGE",
                "reason": f"Page {page_num} not found, but phrase on page {found_in_doc[0][0]}",
                "suggested_page": found_in_doc[0][0],
                "confidence": 0.7,
                "match_type": "wrong_page"
            }
        
        # Search in other documents
        found_elsewhere = find_phrase_in_all_documents(
            phrase, doc_pages, exclude_doc_id=doc_id
        )
        
        if found_elsewhere:
            best = found_elsewhere[0]
            return {
                "doc_id": doc_id,
                "page": page_num,
                "phrase": phrase,
                "status": "DOC_ERROR",
                "reason": f"Page {page_num} not in [{doc_id}]. Found in [{best[0]}] Page {best[1]}",
                "suggested_doc_id": best[0],
                "suggested_page": best[1],
                "confidence": 0.7,
                "match_type": "wrong_doc"
            }
        
        return {
            "doc_id": doc_id,
            "page": page_num,
            "phrase": phrase,
            "status": "PAGE_MISSING",
            "reason": f"Page {page_num} not found in document [{doc_id}]",
            "confidence": 0.0,
            "match_type": "missing"
        }
    
    page_text = pages[page_num]
    normalized_page = normalize_for_search(page_text)
    normalized_phrase = normalize_for_search(phrase)
    
    # Check 3: Exact match
    if normalized_phrase in normalized_page:
        if is_short_quote:
            all_occurrences = find_all_occurrences(phrase, doc_pages, min_ratio=0.9)
            other_locations = [
                (o["doc_id"], o["page"]) 
                for o in all_occurrences 
                if o["doc_id"] != doc_id or o["page"] != page_num
            ]
            
            if other_locations:
                # Check if current location matches draft - prioritize it
                if draft_doc_id == doc_id and draft_page == page_num:
                    return {
                        "doc_id": doc_id,
                        "page": page_num,
                        "phrase": phrase,
                        "status": "OK",
                        "reason": f"Exact match (Draft priority). Also on: {other_locations}",
                        "confidence": 1.0,
                        "match_type": "exact_draft_priority"
                    }
                
                return {
                    "doc_id": doc_id,
                    "page": page_num,
                    "phrase": phrase,
                    "status": "AMBIGUOUS",
                    "reason": f"Short quote also found on: {other_locations}",
                    "suggested_locations": [(doc_id, page_num)] + other_locations,
                    "confidence": 0.6,
                    "match_type": "ambiguous"
                }
        
        return {
            "doc_id": doc_id,
            "page": page_num,
            "phrase": phrase,
            "status": "OK",
            "reason": "Exact match found",
            "confidence": 1.0,
            "match_type": "exact"
        }
    
    # Check 4: N-gram matching
    ngram_ratio = calculate_ngram_match(phrase, page_text, n=4)
    
    if ngram_ratio >= 0.7:
        return {
            "doc_id": doc_id,
            "page": page_num,
            "phrase": phrase,
            "status": "OK",
            "reason": f"N-gram match: {ngram_ratio:.0%} of 4-grams found",
            "confidence": ngram_ratio,
            "match_type": "ngram"
        }
    
    # Check 5: Page overflow
    next_page_num = page_num + 1
    if next_page_num in pages:
        overflow = detect_page_overflow(phrase, page_text, pages[next_page_num])
        if overflow.get("is_overflow"):
            return {
                "doc_id": doc_id,
                "page": page_num,
                "phrase": phrase,
                "status": "PAGE_OVERFLOW",
                "reason": f"Quote spans pages {page_num}-{next_page_num}",
                "overflow_to": next_page_num,
                "split_info": overflow,
                "confidence": overflow.get("combined_score", 0.8),
                "match_type": "overflow"
            }
    
    # Check 6: Search other pages in same document
    for pg_num, pg_text in pages.items():
        if pg_num == page_num:
            continue
        
        normalized_pg = normalize_for_search(pg_text)
        if normalized_phrase in normalized_pg:
            return {
                "doc_id": doc_id,
                "page": page_num,
                "phrase": phrase,
                "status": "WRONG_PAGE",
                "reason": f"Phrase not on page {page_num}, but on page {pg_num}",
                "suggested_page": pg_num,
                "confidence": 0.7,
                "match_type": "wrong_page"
            }
        
        ngram_ratio = calculate_ngram_match(phrase, pg_text, n=4)
        if ngram_ratio >= 0.6:
            return {
                "doc_id": doc_id,
                "page": page_num,
                "phrase": phrase,
                "status": "WRONG_PAGE",
                "reason": f"N-gram match ({ngram_ratio:.0%}) on page {pg_num}",
                "suggested_page": pg_num,
                "confidence": ngram_ratio * 0.9,
                "match_type": "wrong_page_ngram"
            }
    
    # Check 7: Search other documents
    found_elsewhere = find_phrase_in_all_documents(
        phrase, doc_pages, exclude_doc_id=doc_id
    )
    
    if found_elsewhere:
        best = found_elsewhere[0]
        
        if len(found_elsewhere) > 1 and is_short_quote:
            return {
                "doc_id": doc_id,
                "page": page_num,
                "phrase": phrase,
                "status": "AMBIGUOUS",
                "reason": f"Not in [{doc_id}]. Found in multiple locations.",
                "suggested_locations": [(loc[0], loc[1]) for loc in found_elsewhere],
                "confidence": 0.5,
                "match_type": "ambiguous"
            }
        
        return {
            "doc_id": doc_id,
            "page": page_num,
            "phrase": phrase,
            "status": "DOC_ERROR",
            "reason": f"Not in [{doc_id}]. Found in [{best[0]}] Page {best[1]}",
            "suggested_doc_id": best[0],
            "suggested_page": best[1],
            "confidence": best[2],
            "match_type": "wrong_doc"
        }
    
    # Check 8: Partial match (first words)
    words = phrase.split()
    if len(words) > 8:
        short_phrase = ' '.join(words[:8])
        normalized_short = normalize_for_search(short_phrase)
        
        # Check current doc first
        for pg_num, pg_text in pages.items():
            normalized_pg = normalize_for_search(pg_text)
            if normalized_short in normalized_pg:
                if pg_num == page_num:
                    return {
                        "doc_id": doc_id,
                        "page": page_num,
                        "phrase": phrase,
                        "status": "OK",
                        "reason": f"First {len(short_phrase.split())} words found",
                        "confidence": 0.8,
                        "match_type": "partial"
                    }
                else:
                    return {
                        "doc_id": doc_id,
                        "page": page_num,
                        "phrase": phrase,
                        "status": "WRONG_PAGE",
                        "reason": f"Partial match on page {pg_num}",
                        "suggested_page": pg_num,
                        "confidence": 0.65,
                        "match_type": "partial_wrong_page"
                    }
    
    # Not found anywhere
    return {
        "doc_id": doc_id,
        "page": page_num,
        "phrase": phrase,
        "status": "NOT_FOUND",
        "reason": f"Phrase not found in [{doc_id}] page {page_num} or any other location",
        "confidence": 0.0,
        "match_type": "not_found"
    }


# =============================================================================
# ANCHOR REPAIR
# =============================================================================

def attempt_anchor_repair(
    phrase: str,
    doc_id: str,
    page_num: int,
    doc_pages: DocumentPages
) -> RepairResult:
    """
    Attempt to repair a broken/modified anchor phrase.
    
    Strategies:
    1. Match by first N words (for truncated anchors)
    2. Fuzzy match (for modified phrases)
    3. Search in other documents
    """
    result: RepairResult = {"success": False}
    
    # Strategy 1: Try specified document and page first
    if doc_id in doc_pages and page_num in doc_pages[doc_id]:
        page_text = doc_pages[doc_id][page_num]
        
        # Match by first N words
        words = phrase.split()
        for n_words in [6, 5, 4, 3]:
            if len(words) >= n_words:
                prefix = ' '.join(words[:n_words])
                normalized_prefix = normalize_for_search(prefix)
                normalized_page = normalize_for_search(page_text)
                
                if normalized_prefix in normalized_page:
                    idx = normalized_page.find(normalized_prefix)
                    remaining = normalized_page[idx:].split()[:15]
                    repaired = ' '.join(remaining)
                    
                    # Try to get original text
                    original_lower = page_text.lower()
                    orig_idx = original_lower.find(normalized_prefix)
                    if orig_idx != -1:
                        orig_words = page_text[orig_idx:].split()[:15]
                        repaired = ' '.join(orig_words)
                    
                    return {
                        "success": True,
                        "repaired_phrase": repaired.strip('.,;:'),
                        "confidence": 0.85,
                        "repair_type": "truncated",
                        "original_in_source": repaired,
                        "found_doc_id": doc_id,
                        "found_page": page_num
                    }
        
        # Fuzzy match
        match_result = find_best_match_in_text(phrase, page_text, min_similarity=0.65)
        if match_result:
            matched_text, score = match_result
            return {
                "success": True,
                "repaired_phrase": matched_text.strip('.,;:'),
                "confidence": score,
                "repair_type": "modified",
                "original_in_source": matched_text,
                "found_doc_id": doc_id,
                "found_page": page_num
            }
    
    # Strategy 2: Search in other pages of same document
    if doc_id in doc_pages:
        for pg_num, pg_text in doc_pages[doc_id].items():
            if pg_num == page_num:
                continue
            
            words = phrase.split()
            for n_words in [6, 5, 4]:
                if len(words) >= n_words:
                    prefix = ' '.join(words[:n_words])
                    normalized_prefix = normalize_for_search(prefix)
                    normalized_page = normalize_for_search(pg_text)
                    
                    if normalized_prefix in normalized_page:
                        idx = normalized_page.find(normalized_prefix)
                        remaining = normalized_page[idx:].split()[:12]
                        repaired = ' '.join(remaining)
                        
                        return {
                            "success": True,
                            "repaired_phrase": repaired.strip('.,;:'),
                            "confidence": 0.75,
                            "repair_type": "partial_match",
                            "original_in_source": repaired,
                            "found_doc_id": doc_id,
                            "found_page": pg_num
                        }
    
    # Strategy 3: Search in other documents
    for other_doc_id, pages in doc_pages.items():
        if other_doc_id == doc_id:
            continue
        
        for pg_num, pg_text in pages.items():
            words = phrase.split()
            for n_words in [6, 5, 4]:
                if len(words) >= n_words:
                    prefix = ' '.join(words[:n_words])
                    normalized_prefix = normalize_for_search(prefix)
                    normalized_page = normalize_for_search(pg_text)
                    
                    if normalized_prefix in normalized_page:
                        idx = normalized_page.find(normalized_prefix)
                        remaining = normalized_page[idx:].split()[:12]
                        repaired = ' '.join(remaining)
                        
                        return {
                            "success": True,
                            "repaired_phrase": repaired.strip('.,;:'),
                            "confidence": 0.70,
                            "repair_type": "cross_doc_repair",
                            "original_in_source": repaired,
                            "found_doc_id": other_doc_id,
                            "found_page": pg_num
                        }
    
    return result


def repair_invalid_format_citation(
    phrase: str,
    doc_id: str,
    page_num: int,
    doc_pages: DocumentPages
) -> RepairResult:
    """
    Repair INVALID_FORMAT citations (those with ... in the middle).
    
    Strategy: Take the first part before ... and find it in source.
    """
    result: RepairResult = {"success": False}
    
    # Split on ... or …
    parts = re.split(r'\.\.\.+|…+', phrase)
    
    if len(parts) < 2:
        return result
    
    # Try the first part (usually more reliable)
    first_part = parts[0].strip()
    
    if len(first_part) < 10:
        # First part too short, try the last part
        first_part = parts[-1].strip()
    
    if len(first_part) < 10:
        return result
    
    # Try to find and repair using first part
    return attempt_anchor_repair(first_part, doc_id, page_num, doc_pages)


# =============================================================================
# TRACEABILITY LOG PARSING
# =============================================================================

def parse_traceability_log(llm_output: str) -> Dict[str, Tuple[str, int]]:
    """
    Parse TRACEABILITY LOG table from LLM output.
    
    Expected format:
    | # | Statement | Source | Doc ID | Page | Source Quote | Anchor |
    | 1 | ... | DRAFT [1] | abc123 | 40 | "Long term use" | "Long term" |
    
    Returns:
        Dict mapping normalized anchor phrases to (doc_id, page) tuples
    """
    phrase_to_location: Dict[str, Tuple[str, int]] = {}
    
    # Find TRACEABILITY LOG section
    log_start = llm_output.upper().find('TRACEABILITY')
    if log_start == -1:
        return phrase_to_location
    
    log_section = llm_output[log_start:]
    
    for line in log_section.split('\n'):
        if not line.strip().startswith('|'):
            continue
        if '---' in line or 'Statement' in line or 'Anchor' in line:
            continue
        
        parts = [p.strip() for p in line.split('|')]
        
        if len(parts) < 7:
            continue
        
        doc_id = None
        page_num = None
        anchor = None
        
        for i, part in enumerate(parts):
            # Check if this part looks like a doc_id (8 hex chars)
            if re.match(r'^[a-f0-9]{8}$', part, re.IGNORECASE):
                doc_id = part.lower()
                continue
            
            # Check if this is a page number
            if part.isdigit() and 1 <= int(part) <= 9999:
                # Check if next part looks like an anchor
                if i + 1 < len(parts):
                    next_part = parts[i + 1]
                    if '"' in next_part or '⟦' in next_part or '⟧' in next_part:
                        page_num = int(part)
                        anchor = next_part
        
        if doc_id and page_num and anchor:
            # Clean anchor
            anchor_clean = anchor.strip('"⟦⟧\'"` ')
            if len(anchor_clean) >= 5:
                normalized = normalize_for_search(anchor_clean)
                phrase_to_location[normalized] = (doc_id, page_num)
                
                # Add partial keys for fuzzy lookup
                if len(normalized) > 30:
                    phrase_to_location[normalized[:30]] = (doc_id, page_num)
                words = normalized.split()
                if len(words) >= 4:
                    phrase_to_location[' '.join(words[:4])] = (doc_id, page_num)
    
    return phrase_to_location


# =============================================================================
# MAIN VERIFICATION FUNCTIONS
# =============================================================================

def verify_citations_two_pass(
    llm_output: str,
    doc_pages: DocumentPages,
    draft_citations: Optional[List[Tuple[str, int, str]]] = None,
    auto_repair: bool = True
) -> VerificationResult:
    """
    Two-pass citation verification with auto-repair.
    
    Pass 1: Standard verification
    Pass 2: Attempt repairs on failed citations
    
    Args:
        llm_output: LLM output text containing citations
        doc_pages: {doc_id: {page_num: text}}
        draft_citations: Original citations from draft for priority resolution
        auto_repair: Whether to attempt automatic repairs
    """
    citations = extract_citations(llm_output)
    
    if not citations:
        return {
            "verified": [],
            "failed": [],
            "wrong_page": [],
            "wrong_doc": [],
            "overflow": [],
            "ambiguous": [],
            "invalid_format": [],
            "repaired": [],
            "total": 0,
            "success_rate": 1.0,
            "status": "NO_CITATIONS",
            "audit_summary": {"total_citations": 0}
        }
    
    # Build lookup from TRACEABILITY LOG
    traceability_lookup = parse_traceability_log(llm_output)
    
    # Build lookup from draft citations
    draft_lookup: Dict[str, Tuple[str, int]] = {}
    if draft_citations:
        for doc_id, page, phrase in draft_citations:
            normalized = normalize_for_search(phrase)
            draft_lookup[normalized[:50]] = (doc_id, page)
            draft_lookup[normalized[:30]] = (doc_id, page)
            words = normalized.split()
            if len(words) >= 4:
                draft_lookup[' '.join(words[:4])] = (doc_id, page)
    
    def find_authoritative_location(phrase: str) -> Optional[Tuple[str, int]]:
        """Find authoritative (doc_id, page) for a phrase."""
        normalized = normalize_for_search(phrase)
        
        # Strategy 1: Exact match in TRACEABILITY LOG
        if normalized in traceability_lookup:
            return traceability_lookup[normalized]
        
        # Strategy 2: Partial match in TRACEABILITY LOG
        for key_len in [30, 20]:
            if len(normalized) >= key_len:
                key = normalized[:key_len]
                if key in traceability_lookup:
                    return traceability_lookup[key]
        
        # Strategy 3: First N words in TRACEABILITY LOG
        words = normalized.split()
        if len(words) >= 4:
            key = ' '.join(words[:4])
            if key in traceability_lookup:
                return traceability_lookup[key]
        
        # Strategy 4: Fall back to draft lookup
        for key_len in [50, 30]:
            if len(normalized) >= key_len:
                key = normalized[:key_len]
                if key in draft_lookup:
                    return draft_lookup[key]
        
        return None
    
    verified: List[CitationResult] = []
    failed: List[CitationResult] = []
    wrong_page: List[CitationResult] = []
    wrong_doc: List[CitationResult] = []
    overflow: List[CitationResult] = []
    ambiguous: List[CitationResult] = []
    invalid_format: List[CitationResult] = []
    repaired: List[CitationResult] = []
    
    # PASS 1: Standard verification
    pass1_failed = []
    pass1_invalid = []
    pass1_ambiguous = []
    
    for doc_id, page_num, phrase in citations:
        # Try to find authoritative location
        auth_location = find_authoritative_location(phrase)
        auth_doc_id = auth_location[0] if auth_location else None
        auth_page = auth_location[1] if auth_location else None
        
        result = verify_single_citation(
            doc_id, page_num, phrase, doc_pages,
            draft_doc_id=auth_doc_id,
            draft_page=auth_page
        )
        
        if result["status"] == "OK":
            verified.append(result)
        elif result["status"] == "WRONG_PAGE":
            wrong_page.append(result)
        elif result["status"] in ("DOC_ERROR", "DOC_MISSING"):
            wrong_doc.append(result)
        elif result["status"] == "PAGE_OVERFLOW":
            overflow.append(result)
        elif result["status"] == "AMBIGUOUS":
            # Try to resolve using authoritative location
            if auth_location:
                suggested = result.get("suggested_locations", [])
                if auth_location in suggested:
                    result["status"] = "OK"
                    result["reason"] = f"Resolved via TRACEABILITY LOG [{auth_doc_id}:{auth_page}]"
                    result["confidence"] = 0.95
                    result["match_type"] = "traceability_resolved"
                    verified.append(result)
                    continue
            
            # Check if cited location is valid
            cited_loc = (doc_id, page_num)
            suggested = result.get("suggested_locations", [])
            if cited_loc in suggested:
                result["status"] = "OK"
                result["reason"] = f"Cited location is valid (also on {suggested})"
                result["confidence"] = 0.85
                result["match_type"] = "cited_valid"
                verified.append(result)
            else:
                pass1_ambiguous.append((doc_id, page_num, phrase, result))
        elif result["status"] == "INVALID_FORMAT":
            pass1_invalid.append((doc_id, page_num, phrase, result))
        else:
            pass1_failed.append((doc_id, page_num, phrase, result))
    
    # PASS 2: Attempt repairs
    if auto_repair:
        # Repair INVALID_FORMAT
        for doc_id, page_num, phrase, original_result in pass1_invalid:
            repair_result = repair_invalid_format_citation(
                phrase, doc_id, page_num, doc_pages
            )
            
            if repair_result.get("success"):
                repaired_phrase = repair_result["repaired_phrase"]
                found_doc = repair_result.get("found_doc_id", doc_id)
                found_page = repair_result.get("found_page", page_num)
                
                verify_result = verify_single_citation(
                    found_doc, found_page, repaired_phrase, doc_pages
                )
                
                if verify_result["status"] == "OK":
                    repaired.append({
                        "doc_id": doc_id,
                        "page": page_num,
                        "phrase": phrase,
                        "status": "REPAIRED",
                        "reason": f"Split concatenated citation ({repair_result['repair_type']})",
                        "confidence": repair_result["confidence"],
                        "match_type": "repaired_invalid_format",
                        "original_phrase": phrase,
                        "repaired_phrase": repaired_phrase,
                        "repair_reason": "split_concatenated",
                        "suggested_doc_id": found_doc,
                        "suggested_page": found_page
                    })
                    continue
            
            invalid_format.append(original_result)
        
        # Repair NOT_FOUND
        for doc_id, page_num, phrase, original_result in pass1_failed:
            repair_result = attempt_anchor_repair(
                phrase, doc_id, page_num, doc_pages
            )
            
            if repair_result.get("success"):
                repaired_phrase = repair_result["repaired_phrase"]
                found_doc = repair_result.get("found_doc_id", doc_id)
                found_page = repair_result.get("found_page", page_num)
                
                verify_result = verify_single_citation(
                    found_doc, found_page, repaired_phrase, doc_pages
                )
                
                if verify_result["status"] == "OK":
                    repaired.append({
                        "doc_id": doc_id,
                        "page": page_num,
                        "phrase": phrase,
                        "status": "REPAIRED",
                        "reason": f"Auto-repaired ({repair_result['repair_type']})",
                        "confidence": repair_result["confidence"],
                        "match_type": "repaired",
                        "original_phrase": phrase,
                        "repaired_phrase": repaired_phrase,
                        "repair_reason": repair_result["repair_type"],
                        "suggested_doc_id": found_doc,
                        "suggested_page": found_page
                    })
                    continue
            
            failed.append(original_result)
        
        # Handle remaining AMBIGUOUS
        for doc_id, page_num, phrase, original_result in pass1_ambiguous:
            ambiguous.append(original_result)
    else:
        # No auto-repair
        for _, _, _, result in pass1_invalid:
            invalid_format.append(result)
        for _, _, _, result in pass1_failed:
            failed.append(result)
        for _, _, _, result in pass1_ambiguous:
            ambiguous.append(result)
    
    total = len(citations)
    
    # Calculate success rate
    if total > 0:
        score = (
            len(verified) * 1.0 +
            len(repaired) * 0.9 +
            len(overflow) * 0.8 +
            len(wrong_page) * 0.5 +
            len(wrong_doc) * 0.4 +
            len(ambiguous) * 0.3
        )
        success_rate = score / total
    else:
        success_rate = 1.0
    
    # Determine status
    if total == 0:
        status = "NO_CITATIONS"
    elif len(failed) == 0 and len(invalid_format) == 0:
        if len(wrong_page) == 0 and len(wrong_doc) == 0 and len(ambiguous) == 0:
            if len(repaired) > 0:
                status = "VERIFIED_WITH_REPAIRS"
            elif len(overflow) > 0:
                status = "VERIFIED_WITH_OVERFLOW"
            else:
                status = "VERIFIED"
        else:
            status = "NEEDS_REVIEW"
    elif success_rate >= 0.7:
        status = "PARTIALLY_VERIFIED"
    else:
        status = "FAILED"
    
    # Build audit summary
    audit_summary = {
        "total_citations": total,
        "exact_matches": len([v for v in verified if v.get("match_type") == "exact"]),
        "ngram_matches": len([v for v in verified if v.get("match_type") == "ngram"]),
        "repaired_count": len(repaired),
        "ambiguous_count": len(ambiguous),
        "wrong_page_count": len(wrong_page),
        "wrong_doc_count": len(wrong_doc),
        "overflow_count": len(overflow),
        "failed_count": len(failed),
        "invalid_format_count": len(invalid_format),
        "average_confidence": (
            sum(v.get("confidence", 0) for v in verified + repaired) / 
            max(len(verified) + len(repaired), 1)
        )
    }
    
    return {
        "verified": verified,
        "failed": failed,
        "wrong_page": wrong_page,
        "wrong_doc": wrong_doc,
        "overflow": overflow,
        "ambiguous": ambiguous,
        "invalid_format": invalid_format,
        "repaired": repaired,
        "total": total,
        "success_rate": success_rate,
        "status": status,
        "audit_summary": audit_summary
    }


def verify_citations(
    llm_output: str,
    doc_pages: DocumentPages,
    draft_citations: Optional[List[Tuple[str, int, str]]] = None
) -> VerificationResult:
    """
    Main function: verify ALL citations in LLM output.
    Wrapper around verify_citations_two_pass for backward compatibility.
    """
    return verify_citations_two_pass(
        llm_output=llm_output,
        doc_pages=doc_pages,
        draft_citations=draft_citations,
        auto_repair=True
    )


def verify_draft_citations(
    draft_output: str,
    doc_pages: DocumentPages
) -> VerificationResult:
    """Verify citations in DRAFT format (Step 1)."""
    citations = extract_citations_from_draft(draft_output)
    
    verified: List[CitationResult] = []
    failed: List[CitationResult] = []
    wrong_page: List[CitationResult] = []
    wrong_doc: List[CitationResult] = []
    overflow: List[CitationResult] = []
    ambiguous: List[CitationResult] = []
    invalid_format: List[CitationResult] = []
    repaired: List[CitationResult] = []
    
    for doc_id, page_num, phrase in citations:
        result = verify_single_citation(doc_id, page_num, phrase, doc_pages)
        
        if result["status"] == "OK":
            verified.append(result)
        elif result["status"] == "WRONG_PAGE":
            wrong_page.append(result)
        elif result["status"] in ("DOC_ERROR", "DOC_MISSING"):
            wrong_doc.append(result)
        elif result["status"] == "PAGE_OVERFLOW":
            overflow.append(result)
        elif result["status"] == "AMBIGUOUS":
            ambiguous.append(result)
        elif result["status"] == "INVALID_FORMAT":
            invalid_format.append(result)
        else:
            failed.append(result)
    
    total = len(citations)
    
    if total > 0:
        score = (
            len(verified) * 1.0 +
            len(overflow) * 0.8 +
            len(wrong_page) * 0.5 +
            len(wrong_doc) * 0.4 +
            len(ambiguous) * 0.3
        )
        success_rate = score / total
    else:
        success_rate = 1.0
    
    if total == 0:
        status = "NO_CITATIONS"
    elif len(failed) == 0 and len(wrong_page) == 0 and len(wrong_doc) == 0 and len(ambiguous) == 0 and len(invalid_format) == 0:
        status = "VERIFIED"
    elif len(failed) == 0 and len(invalid_format) == 0:
        status = "NEEDS_REVIEW"
    elif success_rate >= 0.7:
        status = "PARTIALLY_VERIFIED"
    else:
        status = "FAILED"
    
    return {
        "verified": verified,
        "failed": failed,
        "wrong_page": wrong_page,
        "wrong_doc": wrong_doc,
        "overflow": overflow,
        "ambiguous": ambiguous,
        "invalid_format": invalid_format,
        "repaired": repaired,
        "total": total,
        "success_rate": success_rate,
        "status": status,
        "audit_summary": {
            "total_citations": total,
            "failed_count": len(failed),
            "repaired_count": 0
        }
    }


# =============================================================================
# PROMPT FORMATTING
# =============================================================================

def format_citation_errors_for_prompt(result: VerificationResult) -> str:
    """
    Format verification results for insertion into validator prompts.
    
    This goes into $citation_errors placeholder in prompts.
    """
    lines = ["=== AUTOMATED CITATION CHECK ===", ""]
    
    has_errors = False
    
    # PAGE_ERROR
    for item in result.get("wrong_page", []):
        has_errors = True
        doc_id = item.get("doc_id", "unknown")
        page = item.get("page", 0)
        suggested = item.get("suggested_page", "?")
        phrase = item.get("phrase", "")[:50]
        lines.append(
            f'[PAGE_ERROR] Citation [{doc_id}] Page {page}: '
            f'"{phrase}..." → should be Page {suggested}'
        )
    
    # DOC_ERROR
    for item in result.get("wrong_doc", []):
        has_errors = True
        doc_id = item.get("doc_id", "unknown")
        page = item.get("page", 0)
        suggested_doc = item.get("suggested_doc_id", "?")
        suggested_page = item.get("suggested_page", "?")
        phrase = item.get("phrase", "")[:50]
        lines.append(
            f'[DOC_ERROR] Citation [{doc_id}] Page {page}: '
            f'"{phrase}..." → found in [{suggested_doc}] Page {suggested_page}'
        )
    
    # PAGE_OVERFLOW
    for item in result.get("overflow", []):
        has_errors = True
        doc_id = item.get("doc_id", "unknown")
        page = item.get("page", 0)
        overflow_to = item.get("overflow_to", page + 1)
        phrase = item.get("phrase", "")[:50]
        lines.append(
            f'[PAGE_OVERFLOW] Citation [{doc_id}] Page {page}: '
            f'"{phrase}..." → spans Pages {page}-{overflow_to}'
        )
    
    # AMBIGUOUS
    for item in result.get("ambiguous", []):
        has_errors = True
        doc_id = item.get("doc_id", "unknown")
        page = item.get("page", 0)
        locations = item.get("suggested_locations", [])
        phrase = item.get("phrase", "")[:50]
        loc_str = ", ".join([f"[{loc[0]}] Page {loc[1]}" for loc in locations[:3]])
        lines.append(
            f'[AMBIGUOUS] Citation [{doc_id}] Page {page}: '
            f'"{phrase}..." → found on: {loc_str}'
        )
    
    # NOT_FOUND
    for item in result.get("failed", []):
        has_errors = True
        doc_id = item.get("doc_id", "unknown")
        page = item.get("page", 0)
        phrase = item.get("phrase", "")[:50]
        lines.append(
            f'[NOT_FOUND] Citation [{doc_id}] Page {page}: '
            f'"{phrase}..." → not found in any document'
        )
    
    # INVALID_FORMAT
    for item in result.get("invalid_format", []):
        has_errors = True
        doc_id = item.get("doc_id", "unknown")
        page = item.get("page", 0)
        phrase = item.get("phrase", "")[:50]
        lines.append(
            f'[INVALID_FORMAT] Citation [{doc_id}] Page {page}: '
            f'"{phrase}..." → contains stitched text (...)'
        )
    
    if not has_errors:
        lines.append("ALL CITATIONS PASSED - No errors detected.")
    
    lines.append("")
    lines.append(f"Summary: {result.get('total', 0)} citations checked, "
                 f"{len(result.get('verified', []))} verified, "
                 f"{len(result.get('repaired', []))} repaired, "
                 f"{result.get('status', 'UNKNOWN')} overall.")
    
    return "\n".join(lines)


def apply_repairs_to_output(llm_output: str, repairs: List[CitationResult]) -> str:
    """
    Apply repairs to the LLM output text.
    Replaces original anchors with repaired anchors.
    """
    result = llm_output
    
    for repair in repairs:
        if repair.get("status") != "REPAIRED":
            continue
        
        original = repair.get("original_phrase", "")
        repaired = repair.get("repaired_phrase", "")
        doc_id = repair.get("doc_id", "")
        page = repair.get("page", 0)
        new_doc_id = repair.get("suggested_doc_id", doc_id)
        new_page = repair.get("suggested_page", page)
        
        if not original or not repaired:
            continue
        
        # Find and replace the citation
        old_citation = f'[[{doc_id}:{page} | "{original}"]]'
        new_citation = f'[[{new_doc_id}:{new_page} | "{repaired}"]]'
        
        result = result.replace(old_citation, new_citation)
        
        # Also try with comma separator
        old_citation_comma = f'[[{doc_id}:{page}, "{original}"]]'
        result = result.replace(old_citation_comma, new_citation)
    
    return result


# =============================================================================
# CLI FOR TESTING
# =============================================================================

if __name__ == "__main__":
    # Test with multi-document source
    test_sources = """
=== SOURCE: icd10_codebook.pdf [doc_id: abc12345] ===

## Page 39
The patient should be treated with both oral hypoglycemic drugs and monitored.

## Page 40
Long term (current) use of insulin should be documented.
If the patient is treated with both insulin and an injectable non-insulin drug, assign codes accordingly.

=== SOURCE: cms_guidelines.pdf [doc_id: def67890] ===

## Page 95
Do not assign a code to treat an acute illness without documentation.
Assign a code from Z79 if the patient is receiving medication for an extended period.

## Page 195
Do not assign a code for medication being administered for a brief period of time to treat an acute illness.
"""
    
    test_llm_output = """
## Summary
Patients on insulin require documentation [[abc12345:40 | "Long term (current) use of insulin"]].

## Instructions
- Check for acute conditions [[def67890:95 | "treat an acute illness"]]
- Verify injectable drugs [[abc12345:40 | "insulin and an injectable non-insulin"]]
- This is WRONG page [[abc12345:395 | "medication being administered for a brief period of time"]]
- This is hallucinated [[abc12345:99 | "some fake text that does not exist"]]
- This is wrong doc [[abc12345:195 | "medication being administered for a brief period"]]
"""
    
    doc_pages = parse_sources_to_pages(test_sources)
    print("Parsed documents:", list(doc_pages.keys()))
    for doc_id, pages in doc_pages.items():
        print(f"  [{doc_id}]: pages {list(pages.keys())}")
    
    result = verify_citations(test_llm_output, doc_pages)
    
    print(f"\nStatus: {result['status']}")
    print(f"Success rate: {result['success_rate']:.0%}")
    print(f"Total citations: {result['total']}")
    print(f"\nAudit Summary: {result['audit_summary']}")
    
    print(f"\n✅ Verified ({len(result['verified'])}):")
    for v in result['verified']:
        print(f"   [{v['doc_id']}:{v['page']}]: \"{v['phrase'][:40]}...\" — {v.get('match_type', 'N/A')}")
    
    print(f"\n🔧 Repaired ({len(result['repaired'])}):")
    for r in result['repaired']:
        print(f"   [{r['doc_id']}:{r['page']}]: \"{r.get('original_phrase', '')[:30]}...\" → \"{r.get('repaired_phrase', '')[:30]}...\"")
    
    print(f"\n📄 Wrong Page ({len(result['wrong_page'])}):")
    for w in result['wrong_page']:
        print(f"   [{w['doc_id']}:{w['page']}]: should be Page {w.get('suggested_page', '?')}")
    
    print(f"\n📁 Wrong Doc ({len(result['wrong_doc'])}):")
    for w in result['wrong_doc']:
        print(f"   [{w['doc_id']}:{w['page']}]: should be [{w.get('suggested_doc_id', '?')}] Page {w.get('suggested_page', '?')}")
    
    print(f"\n❌ Failed ({len(result['failed'])}):")
    for f in result['failed']:
        print(f"   [{f['doc_id']}:{f['page']}]: \"{f['phrase'][:40]}...\" — {f['reason']}")
    
    print("\n" + "="*60)
    print("FORMATTED FOR PROMPT:")
    print("="*60)
    print(format_citation_errors_for_prompt(result))
