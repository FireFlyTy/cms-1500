"""
Document Parser - извлекает текст и метаданные из PDF через Gemini.

VERSION 2: Category-based extraction with paragraph anchors

Формат вывода Gemini V2:
[PAGE_START]
[PAGE_TYPE: clinical|administrative|reference|toc|empty]
[CODE_CATEGORIES: E11% (ICD-10: "start..."..."end"), J195% (HCPCS: "start..."..."end")]
[TOPICS: "Topic Name" ("start anchor..."..."end anchor")]
[MEDICATIONS: "drug name" ("start..."..."end")]
[SKIP: reason if skipped]

## Content here...
[PAGE_END]

Key changes from V1:
- CODE_CATEGORIES: Extract category patterns (E11%) not specific codes (E11.9)
- TOPICS: Match against predefined topics_dictionary
- Paragraph anchors: start...end format for better PDF highlighting
"""

import re
import json
import hashlib
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict, field
from datetime import datetime
from difflib import SequenceMatcher


# ============================================================
# TEXT SIMILARITY FUNCTIONS
# ============================================================

def get_signature(text: str, skip_lines: int = 2, length: int = 150) -> str:
    """
    Extract unique content signature from text.

    Skips common headers (like "Revision Date...") and returns
    first N characters of actual content for comparison.

    Args:
        text: Full text content
        skip_lines: Number of lines to skip (headers)
        length: Length of signature to return

    Returns:
        Normalized signature string
    """
    if not text:
        return ""

    lines = text.strip().split('\n')

    # Skip header lines (usually "Revision Date...", page numbers, etc.)
    content_lines = lines[skip_lines:] if len(lines) > skip_lines else lines
    content = '\n'.join(content_lines)

    # Normalize whitespace and take first N chars
    content = ' '.join(content.split())
    return content[:length].lower().strip()


def similarity(a: str, b: str) -> float:
    """
    Calculate similarity ratio between two strings.

    Uses difflib.SequenceMatcher for fuzzy matching.

    Args:
        a: First string
        b: Second string

    Returns:
        Similarity ratio 0.0 to 1.0
    """
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


@dataclass
class CodeInfo:
    code: str
    type: str  # ICD-10, HCPCS, CPT, NDC
    context: Optional[str] = None
    anchor: Optional[str] = None  # Exact text from document that led to this code (V1 format)
    # V2 paragraph anchors
    anchor_start: Optional[str] = None  # First 5-10 words of paragraph
    anchor_end: Optional[str] = None  # Last 5-10 words of paragraph
    reason: Optional[str] = None  # Why this code is relevant


@dataclass
class TopicInfo:
    name: str
    anchor: Optional[str] = None  # Exact text from document for this topic (V1 format)
    # V2 paragraph anchors
    anchor_start: Optional[str] = None  # First 5-10 words of paragraph
    anchor_end: Optional[str] = None  # Last 5-10 words of paragraph
    topic_id: Optional[int] = None  # FK to topics_dictionary
    reason: Optional[str] = None  # Why this topic is relevant


@dataclass
class MedicationInfo:
    name: str
    anchor: Optional[str] = None  # Exact text from document for this medication (V1 format)
    # V2 paragraph anchors
    anchor_start: Optional[str] = None  # First 5-10 words of paragraph
    anchor_end: Optional[str] = None  # Last 5-10 words of paragraph
    reason: Optional[str] = None  # Why this medication is relevant


@dataclass
class PageData:
    page: int
    page_type: str  # clinical, administrative, reference, toc, empty
    content: Optional[str] = None
    codes: List[CodeInfo] = None
    topics: List[TopicInfo] = None
    medications: List[MedicationInfo] = None
    skip_reason: Optional[str] = None

    def __post_init__(self):
        if self.codes is None:
            self.codes = []
        if self.topics is None:
            self.topics = []
        if self.medications is None:
            self.medications = []


@dataclass 
class DocumentData:
    file_hash: str
    filename: str
    total_pages: int
    parsed_at: str
    pages: List[PageData]
    summary: Dict = None
    
    def __post_init__(self):
        if self.summary is None:
            self.summary = self._build_summary()
    
    def _build_summary(self) -> Dict:
        """Собирает summary из всех страниц"""
        all_codes = {}
        all_topics = {}
        all_medications = {}
        content_pages = []
        skipped_pages = []

        for page in self.pages:
            if page.content:
                content_pages.append(page.page)
            else:
                skipped_pages.append(page.page)

            # Aggregate codes with pages and anchors
            for code_info in page.codes:
                key = code_info.code
                if key not in all_codes:
                    all_codes[key] = {
                        'code': code_info.code,
                        'type': code_info.type,
                        'pages': [],
                        'contexts': [],
                        'anchors': []  # List of {page, text} or {page, start, end} for citation
                    }
                if page.page not in all_codes[key]['pages']:
                    all_codes[key]['pages'].append(page.page)
                if code_info.context:
                    all_codes[key]['contexts'].append(code_info.context)
                # Add anchor with page info for citation (support both V1 and V2 formats)
                if code_info.anchor_start and code_info.anchor_end:
                    # V2 paragraph anchor format
                    all_codes[key]['anchors'].append({
                        'page': page.page,
                        'start': code_info.anchor_start,
                        'end': code_info.anchor_end
                    })
                elif code_info.anchor:
                    # V1 simple anchor format
                    all_codes[key]['anchors'].append({
                        'page': page.page,
                        'text': code_info.anchor
                    })

            # Aggregate topics with pages and anchors
            for topic_info in page.topics:
                # Handle three formats: string, dict (from V2 pipeline), TopicInfo object
                if isinstance(topic_info, str):
                    name = topic_info
                    anchor = None
                    anchor_start = None
                    anchor_end = None
                    topic_id = None
                elif isinstance(topic_info, dict):
                    name = topic_info.get('name')
                    anchor = topic_info.get('anchor')
                    anchor_start = topic_info.get('anchor_start')
                    anchor_end = topic_info.get('anchor_end')
                    topic_id = topic_info.get('topic_id')
                else:
                    name = topic_info.name
                    anchor = topic_info.anchor
                    anchor_start = getattr(topic_info, 'anchor_start', None)
                    anchor_end = getattr(topic_info, 'anchor_end', None)
                    topic_id = getattr(topic_info, 'topic_id', None)

                if name not in all_topics:
                    all_topics[name] = {
                        'name': name,
                        'topic_id': topic_id,
                        'pages': [],
                        'anchors': []
                    }
                if page.page not in all_topics[name]['pages']:
                    all_topics[name]['pages'].append(page.page)
                # Support both V1 and V2 anchor formats
                if anchor_start and anchor_end:
                    all_topics[name]['anchors'].append({
                        'page': page.page,
                        'start': anchor_start,
                        'end': anchor_end
                    })
                elif anchor:
                    all_topics[name]['anchors'].append({
                        'page': page.page,
                        'text': anchor
                    })

            # Aggregate medications with pages and anchors
            for med_info in page.medications:
                # Handle three formats: string, dict (from V2 pipeline), MedicationInfo object
                if isinstance(med_info, str):
                    name = med_info
                    anchor = None
                    anchor_start = None
                    anchor_end = None
                elif isinstance(med_info, dict):
                    name = med_info.get('name')
                    anchor = med_info.get('anchor')
                    anchor_start = med_info.get('anchor_start')
                    anchor_end = med_info.get('anchor_end')
                else:
                    name = med_info.name
                    anchor = med_info.anchor
                    anchor_start = getattr(med_info, 'anchor_start', None)
                    anchor_end = getattr(med_info, 'anchor_end', None)

                if name not in all_medications:
                    all_medications[name] = {
                        'name': name,
                        'pages': [],
                        'anchors': []
                    }
                if page.page not in all_medications[name]['pages']:
                    all_medications[name]['pages'].append(page.page)
                # Support both V1 and V2 anchor formats
                if anchor_start and anchor_end:
                    all_medications[name]['anchors'].append({
                        'page': page.page,
                        'start': anchor_start,
                        'end': anchor_end
                    })
                elif anchor:
                    all_medications[name]['anchors'].append({
                        'page': page.page,
                        'text': anchor
                    })

        # Determine doc_type based on page_types
        page_types = [p.page_type for p in self.pages if p.content]
        if 'clinical' in page_types:
            doc_type = 'clinical_guideline'
        elif 'administrative' in page_types:
            doc_type = 'pa_policy'
        else:
            doc_type = 'unknown'

        return {
            'doc_type': doc_type,
            'all_codes': list(all_codes.values()),
            'topics': sorted(all_topics.values(), key=lambda x: x['name']),
            'medications': sorted(all_medications.values(), key=lambda x: x['name']),
            'content_pages': content_pages,
            'skipped_pages': skipped_pages,
            'content_page_count': len(content_pages)
        }
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'file_hash': self.file_hash,
            'filename': self.filename,
            'total_pages': self.total_pages,
            'parsed_at': self.parsed_at,
            'pages': [
                {
                    'page': p.page,
                    'page_type': p.page_type,
                    'content': p.content,
                    'codes': [asdict(c) for c in p.codes],
                    'topics': [asdict(t) if hasattr(t, 'name') else {'name': t, 'anchor': None} for t in p.topics],
                    'medications': [asdict(m) if hasattr(m, 'name') else {'name': m, 'anchor': None} for m in p.medications],
                    'skip_reason': p.skip_reason
                }
                for p in self.pages
            ],
            'summary': self.summary
        }


def normalize_code_pattern(code: str) -> str:
    """
    Нормализует wildcard паттерны и диапазоны в кодах.

    Wildcards:
        E11.*  → E11.%
        E11.-  → E11.%
        E11.x  → E11.%

    Ranges:
        47531-47541 → 47531:47541
        99213-99215 → 99213:99215
        I20-I25     → I20:I25
    """
    if not code:
        return code

    normalized = code.upper().strip()

    # Check for range pattern: CODE-CODE (but not wildcard like E11.-)
    # Range pattern: starts with alphanumeric, has dash in middle, ends with alphanumeric
    range_match = re.match(r'^([A-Z0-9\.]+)-([A-Z0-9\.]+)$', normalized)
    if range_match:
        start, end = range_match.groups()
        # Make sure it's actually a range (not E11.- wildcard)
        if len(end) > 1 and not end.startswith('.'):
            return f"{start}:{end}"

    # Replace wildcard characters at the end
    if normalized.endswith('.*'):
        normalized = normalized[:-1] + '%'
    elif normalized.endswith('.-'):
        normalized = normalized[:-1] + '%'
    elif normalized.endswith('.X'):
        normalized = normalized[:-1] + '%'
    elif normalized.endswith('*'):
        normalized = normalized[:-1] + '%'

    return normalized


def is_wildcard_code(code: str) -> bool:
    """Проверяет является ли код wildcard паттерном."""
    return code and '%' in code


def is_range_code(code: str) -> bool:
    """Проверяет является ли код диапазоном (47531:47541)."""
    return code and ':' in code


def parse_range(range_code: str) -> tuple:
    """
    Парсит диапазон кода.

    47531:47541 → ('47531', '47541')
    I20:I25     → ('I20', 'I25')

    Returns: (start, end) or (None, None) if not a range
    """
    if not range_code or ':' not in range_code:
        return (None, None)

    parts = range_code.split(':')
    if len(parts) != 2:
        return (None, None)

    return (parts[0].strip(), parts[1].strip())


def code_in_range(code: str, range_code: str) -> bool:
    """
    Проверяет входит ли код в диапазон.

    code_in_range("47535", "47531:47541") → True
    code_in_range("47550", "47531:47541") → False
    code_in_range("I21", "I20:I25")       → True
    """
    start, end = parse_range(range_code)
    if not start or not end:
        return False

    code = code.upper()
    start = start.upper()
    end = end.upper()

    # Extract numeric and alpha parts for comparison
    # For pure numeric codes (CPT): 47531 <= 47535 <= 47541
    # For ICD-10 ranges: I20 <= I21 <= I25

    try:
        # Try numeric comparison first
        if code.isdigit() and start.isdigit() and end.isdigit():
            return int(start) <= int(code) <= int(end)

        # Alphanumeric comparison (ICD-10 style)
        # Extract common prefix and compare numeric suffix
        common_prefix = ""
        for i, (c1, c2, c3) in enumerate(zip(code, start, end)):
            if c1 == c2 == c3 and c1.isalpha():
                common_prefix += c1
            else:
                break

        if common_prefix:
            code_suffix = code[len(common_prefix):]
            start_suffix = start[len(common_prefix):]
            end_suffix = end[len(common_prefix):]

            # Compare remaining parts
            if code_suffix.replace('.', '').isdigit() and \
               start_suffix.replace('.', '').isdigit() and \
               end_suffix.replace('.', '').isdigit():
                code_num = float(code_suffix) if '.' in code_suffix else int(code_suffix)
                start_num = float(start_suffix) if '.' in start_suffix else int(start_suffix)
                end_num = float(end_suffix) if '.' in end_suffix else int(end_suffix)
                return start_num <= code_num <= end_num

        # Fallback to string comparison
        return start <= code <= end

    except (ValueError, TypeError):
        # Fallback to string comparison
        return start <= code <= end


def code_matches_pattern(code: str, pattern: str) -> bool:
    """
    Проверяет соответствует ли конкретный код паттерну.

    Exact match:
        code_matches_pattern("E11.9", "E11.9") → True

    Wildcard match:
        code_matches_pattern("E11.9", "E11.%") → True
        code_matches_pattern("E11.9", "E12.%") → False

    Range match:
        code_matches_pattern("47535", "47531:47541") → True
        code_matches_pattern("47550", "47531:47541") → False
    """
    if not code or not pattern:
        return False

    code = code.upper()
    pattern = pattern.upper()

    # Exact match
    if pattern == code:
        return True

    # Range match
    if ':' in pattern:
        return code_in_range(code, pattern)

    # Wildcard match
    if '%' in pattern:
        regex_pattern = '^' + pattern.replace('%', '.*').replace('_', '.') + '$'
        return bool(re.match(regex_pattern, code))

    return False


def parse_code_string(code_str: str) -> List[CodeInfo]:
    """
    Парсит строку кодов с anchor текстом:

    Formats supported:
    - E11.9 (ICD-10)                           → code only
    - E11.9 (ICD-10: Type 2 Diabetes)          → code + context
    - E11.9 (ICD-10: "type 2 diabetes")        → code + anchor (quoted = exact text)
    - E11.9 (ICD-10: Type 2 Diabetes | "type 2 diabetes")  → context + anchor

    Нормализует wildcard паттерны (*, -, x → %)
    """
    codes = []
    if not code_str or code_str.strip() == '-':
        return codes

    # Pattern: CODE (TYPE) or CODE (TYPE: context) or CODE (TYPE: context | "anchor")
    # Also handles CODE (TYPE: "anchor") where anchor is in quotes
    pattern = r'([A-Z0-9\.\-\*]+)\s*\(([^:)]+)(?::\s*([^)]+))?\)'
    matches = re.findall(pattern, code_str, re.IGNORECASE)

    for match in matches:
        code = match[0].strip()
        code_type = match[1].strip().upper()
        extra = match[2].strip() if len(match) > 2 and match[2] else None

        context = None
        anchor = None

        if extra:
            # Check if there's a pipe separator: "Context | "anchor text""
            if '|' in extra:
                parts = extra.split('|', 1)
                context = parts[0].strip()
                anchor_part = parts[1].strip()
                # Extract quoted anchor
                anchor_match = re.search(r'"([^"]+)"', anchor_part)
                if anchor_match:
                    anchor = anchor_match.group(1)
            # Check if the whole thing is quoted (just anchor, no context)
            elif extra.startswith('"') and extra.endswith('"'):
                anchor = extra[1:-1]
            # Check if there's a quoted part at the end
            elif '"' in extra:
                anchor_match = re.search(r'"([^"]+)"', extra)
                if anchor_match:
                    anchor = anchor_match.group(1)
                    # Context is everything before the quote
                    context = extra[:extra.index('"')].strip().rstrip(',').strip()
                    if not context:
                        context = None
            else:
                # No quotes - treat as context only
                context = extra

        # Normalize wildcard patterns
        code = normalize_code_pattern(code)

        # Normalize code type
        if code_type in ['ICD-10', 'ICD10', 'ICD']:
            code_type = 'ICD-10'
        elif code_type in ['HCPCS', 'HCPC']:
            code_type = 'HCPCS'
        elif code_type in ['CPT', 'CPT-4']:
            code_type = 'CPT'
        elif code_type in ['NDC']:
            code_type = 'NDC'

        codes.append(CodeInfo(code=code, type=code_type, context=context, anchor=anchor))

    # Fallback: если pattern не сработал, пробуем простой split
    if not codes and code_str:
        parts = code_str.split(',')
        for part in parts:
            part = part.strip()
            if not part:
                continue
            # Normalize and detect type
            part = normalize_code_pattern(part)
            code_type = detect_code_type(part)
            codes.append(CodeInfo(code=part, type=code_type, context=None, anchor=None))

    return codes


def detect_code_type(code: str) -> str:
    """Определяет тип кода по формату"""
    code = code.strip().upper()
    
    # ICD-10: starts with letter, has dot (E11.9, F32.1, Z79.4)
    if re.match(r'^[A-Z]\d', code) and ('.' in code or len(code) <= 3):
        return 'ICD-10'
    
    # HCPCS: starts with letter, 4 digits (J1950, A4253, E0607)
    if re.match(r'^[A-Z]\d{4}$', code):
        return 'HCPCS'
    
    # CPT: 5 digits (99213, 96372)
    if re.match(r'^\d{5}$', code):
        return 'CPT'
    
    # NDC: 11 digits with dashes
    if re.match(r'^\d{5}-\d{4}-\d{2}$', code) or re.match(r'^\d{11}$', code):
        return 'NDC'
    
    return 'Unknown'


def parse_list_string(list_str: str) -> List[str]:
    """Парсит строку списка: "item1, item2, item3" """
    if not list_str or list_str.strip() == '-':
        return []

    items = [item.strip() for item in list_str.split(',')]
    return [item for item in items if item]


def parse_topics_string(topics_str: str) -> List[TopicInfo]:
    """
    Парсит строку тем с anchor текстом:

    Formats supported:
    - "topic name" ("anchor text")     → topic + anchor
    - topic name                        → topic only (backward compat)

    Example: "GLP-1 indications" ("indicated for treatment"), "metformin failure" ("inadequate response")
    """
    topics = []
    if not topics_str or topics_str.strip() == '-':
        return topics

    # Pattern: "topic name" ("anchor text") or just topic name
    # Match: "name" ("anchor") pattern
    pattern = r'"([^"]+)"\s*\("([^"]+)"\)'
    matches = re.findall(pattern, topics_str)

    if matches:
        for name, anchor in matches:
            topics.append(TopicInfo(name=name.strip(), anchor=anchor.strip()))
    else:
        # Fallback: simple comma-separated list (backward compatibility)
        parts = topics_str.split(',')
        for part in parts:
            part = part.strip().strip('"')
            if part:
                topics.append(TopicInfo(name=part, anchor=None))

    return topics


def parse_medications_string(meds_str: str) -> List[MedicationInfo]:
    """
    Парсит строку медикаментов с anchor текстом:

    Formats supported:
    - "drug name" ("anchor text")       → drug + anchor
    - drug name                          → drug only (backward compat)

    Example: "semaglutide" ("Semaglutide (Ozempic): J1950"), "dulaglutide" ("Trulicity")
    """
    meds = []
    if not meds_str or meds_str.strip() == '-':
        return meds

    # Pattern: "drug name" ("anchor text") or just drug name
    pattern = r'"([^"]+)"\s*\("([^"]+)"\)'
    matches = re.findall(pattern, meds_str)

    if matches:
        for name, anchor in matches:
            meds.append(MedicationInfo(name=name.strip(), anchor=anchor.strip()))
    else:
        # Fallback: simple comma-separated list (backward compatibility)
        parts = meds_str.split(',')
        for part in parts:
            part = part.strip().strip('"')
            if part:
                meds.append(MedicationInfo(name=part, anchor=None))

    return meds


# ============================================================
# V2 PARSING FUNCTIONS - Paragraph anchors (start...end)
# ============================================================

def parse_code_categories_string_v2(code_str: str) -> List[CodeInfo]:
    """
    Парсит строку CODE_CATEGORIES с paragraph anchors и reason (V2 format):

    Format: PATTERN (TYPE: "start anchor..."..."end anchor" | "reason")

    Example:
    [CODE_CATEGORIES: E (ICD-10: "GLP-1 receptor agonists"..."glycemic control" | "diabetes treatment")]
    """
    codes = []
    if not code_str or code_str.strip() == '-':
        return codes

    # Pattern: PATTERN (TYPE: "start..."..."end" | "reason")
    # Groups: 1=code, 2=type, 3=anchor_start, 4=anchor_end, 5=reason
    pattern = r'([A-Z0-9\.]+)\s*\(([^:]+):\s*"([^"]+)"(?:\s*\.\.\.\s*"([^"]+)")?(?:\s*\|\s*"([^"]+)")?\)'
    matches = re.findall(pattern, code_str, re.IGNORECASE)

    for match in matches:
        code_pattern = match[0].strip().upper()
        code_type = match[1].strip().upper()
        anchor_start = match[2].strip() if match[2] else None
        anchor_end = match[3].strip() if len(match) > 3 and match[3] else None
        reason = match[4].strip() if len(match) > 4 and match[4] else None

        # Normalize code type
        if code_type in ['ICD-10', 'ICD10', 'ICD']:
            code_type = 'ICD-10'
        elif code_type in ['HCPCS', 'HCPC']:
            code_type = 'HCPCS'
        elif code_type in ['CPT', 'CPT-4']:
            code_type = 'CPT'

        codes.append(CodeInfo(
            code=code_pattern,
            type=code_type,
            context=reason,  # Store reason as context
            anchor=None,  # V1 format deprecated
            anchor_start=anchor_start,
            anchor_end=anchor_end
        ))

    # Fallback: try V1 format if no V2 matches
    if not codes:
        return parse_code_string(code_str)

    return codes


def parse_topics_string_v2(topics_str: str) -> List[TopicInfo]:
    """
    Парсит строку TOPICS с paragraph anchors (V2 format):

    Format: "Topic Name" ("start anchor..."..."end anchor")

    Example:
    [TOPICS: "Type 2 Diabetes Mellitus" ("GLP-1 receptor agonists"..."glycemic control"), "GLP-1 Therapy" ("Available agents"..."subcutaneously")]
    """
    topics = []
    if not topics_str or topics_str.strip() == '-':
        return topics

    # Pattern: "Name" ("start..."..."end")
    pattern = r'"([^"]+)"\s*\("([^"]+)"(?:\s*\.\.\.\s*"([^"]+)")?\)'
    matches = re.findall(pattern, topics_str)

    for match in matches:
        name = match[0].strip()
        anchor_start = match[1].strip() if match[1] else None
        anchor_end = match[2].strip() if len(match) > 2 and match[2] else None

        topics.append(TopicInfo(
            name=name,
            anchor=None,  # V1 format
            anchor_start=anchor_start,  # V2 format
            anchor_end=anchor_end
        ))

    # Fallback: try V1 format if no V2 matches
    if not topics:
        return parse_topics_string(topics_str)

    return topics


def parse_medications_string_v2(meds_str: str) -> List[MedicationInfo]:
    """
    Парсит строку MEDICATIONS с paragraph anchors (V2 format):

    Format: "drug name" ("start anchor..."..."end anchor")

    Example:
    [MEDICATIONS: "semaglutide" ("Semaglutide (Ozempic) is"..."weekly injection"), "metformin" ("Prior trial of"..."contraindicated")]
    """
    meds = []
    if not meds_str or meds_str.strip() == '-':
        return meds

    # Pattern: "Name" ("start..."..."end")
    pattern = r'"([^"]+)"\s*\("([^"]+)"(?:\s*\.\.\.\s*"([^"]+)")?\)'
    matches = re.findall(pattern, meds_str)

    for match in matches:
        name = match[0].strip()
        anchor_start = match[1].strip() if match[1] else None
        anchor_end = match[2].strip() if len(match) > 2 and match[2] else None

        meds.append(MedicationInfo(
            name=name,
            anchor=None,  # V1 format
            anchor_start=anchor_start,  # V2 format
            anchor_end=anchor_end
        ))

    # Fallback: try V1 format if no V2 matches
    if not meds:
        return parse_medications_string(meds_str)

    return meds


def parse_page_json_v2(page_obj: dict) -> PageData:
    """
    Parse a single page object from JSON response.
    Content will be set later from original PDF text.

    Args:
        page_obj: Dict with page, skip, page_type, codes, topics, medications

    Returns:
        PageData object
    """
    page_num = page_obj.get('page', 0)
    page = PageData(page=page_num, page_type='clinical')

    # Check for skip
    skip = page_obj.get('skip')
    if skip:
        page.skip_reason = skip
        page.page_type = 'skip'
        page.content = None
        return page

    # Page type
    page_type = page_obj.get('page_type')
    if page_type:
        page.page_type = page_type.lower()

    # Parse codes
    codes_list = page_obj.get('codes', [])
    page.codes = []
    for c in codes_list:
        if isinstance(c, dict):
            code_type = c.get('type', 'Unknown')
            # Normalize code type
            if code_type.upper() in ['ICD-10', 'ICD10', 'ICD']:
                code_type = 'ICD-10'
            elif code_type.upper() in ['HCPCS', 'HCPC']:
                code_type = 'HCPCS'
            elif code_type.upper() in ['CPT', 'CPT-4']:
                code_type = 'CPT'

            page.codes.append(CodeInfo(
                code=c.get('code', ''),
                type=code_type,
                context=c.get('reason'),  # Store reason as context for backward compat
                anchor_start=c.get('anchor_start'),
                anchor_end=c.get('anchor_end'),
                reason=c.get('reason')
            ))

    # Parse topics
    topics_list = page_obj.get('topics', [])
    page.topics = []
    for t in topics_list:
        if isinstance(t, dict):
            page.topics.append(TopicInfo(
                name=t.get('name', ''),
                anchor_start=t.get('anchor_start'),
                anchor_end=t.get('anchor_end'),
                reason=t.get('reason')
            ))

    # Parse medications
    meds_list = page_obj.get('medications', [])
    page.medications = []
    for m in meds_list:
        if isinstance(m, dict):
            page.medications.append(MedicationInfo(
                name=m.get('name', ''),
                anchor_start=m.get('anchor_start'),
                anchor_end=m.get('anchor_end'),
                reason=m.get('reason')
            ))

    # Content will be set later from original PDF text
    page.content = None
    return page


def parse_page_block_v2(block: str, page_num: int) -> PageData:
    """
    Parse metadata block from Gemini response (legacy text format).
    Content will be set later from original PDF text.

    Format:
    [PAGE_START: N]
    [SKIP: reason]  OR  [PAGE_TYPE: ...] + metadata
    [PAGE_END]
    """
    page = PageData(page=page_num, page_type='clinical')

    # Extract SKIP reason first - if present, skip all other parsing
    match = re.search(r'\[SKIP:\s*([^\]]+)\]', block, re.IGNORECASE)
    if match:
        page.skip_reason = match.group(1).strip()
        page.page_type = 'skip'
        page.content = None
        return page

    # Extract PAGE_TYPE
    match = re.search(r'\[PAGE_TYPE:\s*([^\]]+)\]', block, re.IGNORECASE)
    if match:
        page.page_type = match.group(1).strip().lower()

    # Extract CODE_CATEGORIES
    match = re.search(r'\[CODE_CATEGORIES:\s*([^\]]+)\]', block, re.IGNORECASE)
    if match:
        page.codes = parse_code_categories_string_v2(match.group(1))

    # Extract TOPICS
    match = re.search(r'\[TOPICS:\s*([^\]]+)\]', block, re.IGNORECASE)
    if match:
        page.topics = parse_topics_string_v2(match.group(1))

    # Extract MEDICATIONS
    match = re.search(r'\[MEDICATIONS:\s*([^\]]+)\]', block, re.IGNORECASE)
    if match:
        page.medications = parse_medications_string_v2(match.group(1))

    # Content will be set later from original PDF text
    page.content = None
    return page


def validate_and_fix_anchors(
    page_data: PageData,
    original_text: str
) -> Tuple[PageData, List[str]]:
    """
    Validate anchors exist in text and repair using word density clustering.

    Algorithm:
    1. Extract key words from anchor (skip stop words)
    2. Find positions of each word in text
    3. Find cluster with highest word density
    4. Extract phrase from cluster region

    Args:
        page_data: Parsed page with metadata
        original_text: Original PDF text for this page

    Returns:
        (fixed_page_data, warnings)
    """
    warnings = []
    text_lower = original_text.lower()

    # Stop words to skip
    STOP_WORDS = {'a', 'an', 'the', 'of', 'and', 'or', 'to', 'in', 'for', 'with',
                  'is', 'are', 'was', 'were', 'be', 'been', 'being', 'that', 'this',
                  'it', 'its', 'as', 'at', 'by', 'on', 'from'}

    def get_key_words(anchor: str) -> List[str]:
        """Extract key words from anchor, skip stop words."""
        words = anchor.lower().split()
        # Keep words with 3+ chars that aren't stop words
        return [w for w in words if len(w) >= 3 and w not in STOP_WORDS]

    def find_word_positions(word: str) -> List[int]:
        """Find all positions of a word in text."""
        positions = []
        start = 0
        word_lower = word.lower()

        while True:
            idx = text_lower.find(word_lower, start)
            if idx == -1:
                break
            # Check word boundaries
            before_ok = idx == 0 or not text_lower[idx-1].isalnum()
            after_ok = idx + len(word) >= len(text_lower) or not text_lower[idx + len(word)].isalnum()
            if before_ok and after_ok:
                positions.append(idx)
            start = idx + 1

        return positions

    def find_density_cluster(key_words: List[str], window_size: int = 200) -> Optional[Tuple[int, int, float]]:
        """
        Find region with highest density of key words.
        Returns (start_pos, end_pos, density_score) or None.
        """
        if not key_words:
            return None

        # Get all word positions
        all_positions = []  # (position, word)
        for word in key_words:
            for pos in find_word_positions(word):
                all_positions.append((pos, word))

        if not all_positions:
            return None

        # Sort by position
        all_positions.sort(key=lambda x: x[0])

        # Sliding window to find densest cluster
        best_cluster = None
        best_score = 0

        for i, (start_pos, _) in enumerate(all_positions):
            # Find all words within window
            words_in_window = set()
            end_pos = start_pos

            for pos, word in all_positions[i:]:
                if pos - start_pos <= window_size:
                    words_in_window.add(word)
                    end_pos = pos
                else:
                    break

            # Score = fraction of key words found
            score = len(words_in_window) / len(key_words)

            if score > best_score:
                best_score = score
                best_cluster = (start_pos, end_pos, score)

        # Require at least 50% of key words
        if best_cluster and best_cluster[2] >= 0.5:
            return best_cluster

        return None

    def extract_phrase_from_cluster(start_pos: int, end_pos: int, target_words: int = 8) -> str:
        """Extract a clean phrase from the cluster region."""
        # Expand slightly to get context
        margin = 20
        region_start = max(0, start_pos - margin)
        region_end = min(len(original_text), end_pos + margin + 50)

        # Find word boundaries
        while region_start > 0 and original_text[region_start].isalnum():
            region_start -= 1
        while region_end < len(original_text) and original_text[region_end].isalnum():
            region_end += 1

        # Extract and clean
        region = original_text[region_start:region_end].strip()

        # Take first N words
        words = region.split()[:target_words]
        return ' '.join(words)

    def find_exact_match(anchor: str) -> Optional[int]:
        """Find exact match position."""
        anchor_lower = anchor.lower()
        idx = text_lower.find(anchor_lower)
        return idx if idx != -1 else None

    def clean_page_numbers(anchor: str) -> str:
        """Remove page number patterns like '29 of 121' from anchor start."""
        # Pattern: "N of M" at start (page numbers from PDF headers/footers)
        cleaned = re.sub(r'^\d+\s+of\s+\d+\s*', '', anchor, flags=re.IGNORECASE)
        # Pattern: "Page N" at start
        cleaned = re.sub(r'^page\s+\d+\s*', '', cleaned, flags=re.IGNORECASE)
        # Pattern: just page number at start "29 "
        cleaned = re.sub(r'^\d+\s+(?=[A-Za-z])', '', cleaned)
        return cleaned.strip()

    def repair_anchor(anchor: str) -> Tuple[Optional[str], str]:
        """
        Try to repair anchor using density clustering.
        Returns (repaired_anchor, repair_note)
        """
        if not anchor or len(anchor) < 5:
            return None, "too short"

        # Clean page numbers from anchor
        anchor = clean_page_numbers(anchor)
        if len(anchor) < 5:
            return None, "too short after cleaning"

        # Strategy 1: Exact match
        idx = find_exact_match(anchor)
        if idx is not None:
            return original_text[idx:idx + len(anchor)], ""

        # Strategy 2: Density clustering
        key_words = get_key_words(anchor)
        if len(key_words) < 2:
            return None, "not enough key words"

        cluster = find_density_cluster(key_words)
        if cluster:
            start_pos, end_pos, score = cluster
            phrase = extract_phrase_from_cluster(start_pos, end_pos)
            if phrase:
                return phrase, f"density match ({score:.0%})"

        # Strategy 3: First key word only
        if key_words:
            positions = find_word_positions(key_words[0])
            if positions:
                idx = positions[0]
                # Extract a few words starting from this position
                end = min(len(original_text), idx + 100)
                words = original_text[idx:end].split()[:6]
                return ' '.join(words), "first keyword"

        return None, "not found"

    def find_best_pair(
        start_anchor: str,
        end_anchor: str
    ) -> Tuple[Optional[str], Optional[str], List[str]]:
        """Find and fix anchor pair."""
        pair_warnings = []

        if not start_anchor and not end_anchor:
            return None, None, []

        # Clean page numbers BEFORE repair (in case original text has them too)
        if start_anchor:
            start_anchor = clean_page_numbers(start_anchor)
        if end_anchor:
            end_anchor = clean_page_numbers(end_anchor)

        # Repair each anchor
        fixed_start, start_note = repair_anchor(start_anchor) if start_anchor else (None, "")
        fixed_end, end_note = repair_anchor(end_anchor) if end_anchor else (None, "")

        # Clean page numbers from result too (in case found in text with page number)
        if fixed_start:
            fixed_start = clean_page_numbers(fixed_start)
        if fixed_end:
            fixed_end = clean_page_numbers(fixed_end)

        if not fixed_start and start_anchor:
            pair_warnings.append(f"anchor_start {start_note}: '{start_anchor[:30]}...'")
        elif start_note:
            pair_warnings.append(f"anchor_start {start_note}")

        if not fixed_end and end_anchor:
            pair_warnings.append(f"anchor_end {end_note}: '{end_anchor[:30]}...'")
        elif end_note:
            pair_warnings.append(f"anchor_end {end_note}")

        # Verify end comes after start
        if fixed_start and fixed_end:
            start_idx = find_exact_match(fixed_start)
            end_idx = find_exact_match(fixed_end)

            if start_idx is not None and end_idx is not None:
                if end_idx < start_idx:
                    pair_warnings.append("swapped order")

        return fixed_start, fixed_end, pair_warnings

    def fix_item_anchors(item, item_type: str):
        """Fix anchors for a single item."""
        start = getattr(item, 'anchor_start', None)
        end = getattr(item, 'anchor_end', None)

        if not start and not end:
            return

        fixed_start, fixed_end, pair_warnings = find_best_pair(start, end)

        for w in pair_warnings:
            warnings.append(f"Page {page_data.page} {item_type}: {w}")

        if hasattr(item, 'anchor_start'):
            item.anchor_start = fixed_start
        if hasattr(item, 'anchor_end'):
            item.anchor_end = fixed_end

    # Validate all items
    for code in (page_data.codes or []):
        fix_item_anchors(code, f"code {code.code}")

    for topic in (page_data.topics or []):
        name = topic.name if hasattr(topic, 'name') else str(topic)
        fix_item_anchors(topic, f"topic '{name}'")

    for med in (page_data.medications or []):
        name = med.name if hasattr(med, 'name') else str(med)
        fix_item_anchors(med, f"med '{name}'")

    return page_data, warnings


@dataclass
class ChunkParseResult:
    """Result of parsing a chunk with validation info."""
    pages: List[PageData]
    trusted_count: int
    remapped_count: int
    dropped_count: int
    warnings: List[str]


def extract_json_from_response(response_text: str) -> Optional[list]:
    """
    Extract JSON array from Gemini response.
    Handles cases where response may have markdown code blocks or extra text.
    """
    text = response_text.strip()

    # Try to find JSON array directly
    # Look for [ ... ] pattern
    start_idx = text.find('[')
    if start_idx == -1:
        return None

    # Find matching closing bracket
    bracket_count = 0
    end_idx = -1
    for i, char in enumerate(text[start_idx:], start_idx):
        if char == '[':
            bracket_count += 1
        elif char == ']':
            bracket_count -= 1
            if bracket_count == 0:
                end_idx = i
                break

    if end_idx == -1:
        return None

    json_str = text[start_idx:end_idx + 1]

    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        print(f"⚠ JSON parse error: {e}")
        # Try to fix common issues
        # Remove trailing commas before ] or }
        json_str = re.sub(r',\s*([}\]])', r'\1', json_str)
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return None


def parse_chunk_response_v2(
    response_text: str,
    original_pages: Dict[int, str],
    start_page: int,
    expected_count: int
) -> ChunkParseResult:
    """
    Parse metadata from Gemini response (JSON format).
    Content comes from original PDF, not from Gemini.

    Args:
        response_text: Raw response from Gemini (JSON array)
        original_pages: Dict mapping page_num -> original text from PDF
        start_page: Starting page number for this chunk
        expected_count: Expected number of pages

    Returns:
        ChunkParseResult with pages and warnings
    """
    warnings = []
    pages = []
    useful_count = 0

    # Try JSON parsing first
    json_data = extract_json_from_response(response_text)

    if json_data and isinstance(json_data, list):
        # JSON format - new path
        for i, page_obj in enumerate(json_data):
            if not isinstance(page_obj, dict):
                warnings.append(f"Invalid page object at index {i}")
                continue

            # Parse page from JSON
            page_data = parse_page_json_v2(page_obj)

            # Ensure page number is set
            if page_data.page == 0:
                page_data.page = start_page + i

            page_num = page_data.page

            # Set content from original PDF (not from Gemini)
            if page_num in original_pages and not page_data.skip_reason:
                page_data.content = original_pages[page_num]
                useful_count += 1

                # Validate and fix anchors against original text
                page_data, anchor_warnings = validate_and_fix_anchors(
                    page_data, original_pages[page_num]
                )
                warnings.extend(anchor_warnings)

            pages.append(page_data)
    else:
        # Fallback to legacy text format parsing
        warnings.append("JSON parse failed, using legacy text format")
        pattern = r'\[PAGE_START(?::\s*(\d+))?\](.*?)\[PAGE_END\]'
        matches = re.findall(pattern, response_text, re.DOTALL)

        for i, (claimed_str, block_content) in enumerate(matches):
            # Get page number
            if claimed_str:
                page_num = int(claimed_str)
            else:
                page_num = start_page + i

            # Parse metadata from block
            page_data = parse_page_block_v2(block_content, page_num)

            # Set content from original PDF (not from Gemini)
            if page_num in original_pages and not page_data.skip_reason:
                page_data.content = original_pages[page_num]
                useful_count += 1

                # Validate and fix anchors against original text
                page_data, anchor_warnings = validate_and_fix_anchors(
                    page_data, original_pages[page_num]
                )
                warnings.extend(anchor_warnings)

            pages.append(page_data)

    # Check for missing pages (warning only)
    parsed_nums = {p.page for p in pages}
    missing = set(original_pages.keys()) - parsed_nums
    if missing:
        warnings.append(f"Missing pages: {sorted(missing)}")

    return ChunkParseResult(
        pages=pages,
        trusted_count=useful_count,
        remapped_count=0,
        dropped_count=0,
        warnings=warnings
    )


@dataclass
class DocumentValidationResult:
    """Result of full document validation."""
    valid: bool
    total_pages: int
    content_ok: int
    content_mismatch: int
    duplicates_found: int
    missing_pages: List[int]
    extra_pages: List[int]
    issues: List[str]


def validate_document(
    final_pages: Dict[int, PageData],
    original_pages: Dict[int, str]
) -> DocumentValidationResult:
    """
    Final validation of entire document after all chunks processed.

    Checks:
    1. Content correctness - each page matches original
    2. Duplicate detection - same content on different pages
    3. Coverage - all original pages present, no extra pages

    Args:
        final_pages: Dict mapping page_num -> PageData (parsed result)
        original_pages: Dict mapping page_num -> original text from PDF

    Returns:
        DocumentValidationResult with detailed validation info
    """
    issues = []
    content_ok = 0
    content_mismatch = 0

    # ══════════════════════════════════════════════
    # 1. Check content correctness for each page
    # ══════════════════════════════════════════════
    for page_num, page_data in final_pages.items():
        if page_num not in original_pages:
            issues.append(f"Page {page_num}: no original to compare")
            continue

        # Skip empty/skipped pages
        if not page_data.content:
            content_ok += 1
            continue

        page_sig = get_signature(page_data.content)
        orig_sig = get_signature(original_pages[page_num])
        score = similarity(page_sig, orig_sig)

        if score >= 0.7:
            content_ok += 1
        else:
            content_mismatch += 1
            issues.append(f"Page {page_num}: content mismatch (score={score:.2f})")

    # ══════════════════════════════════════════════
    # 2. Find duplicates by content similarity
    # ══════════════════════════════════════════════
    duplicates_found = 0
    pages_list = [(pn, pd) for pn, pd in final_pages.items() if pd.content]

    for i, (page_num1, data1) in enumerate(pages_list):
        sig1 = get_signature(data1.content)
        for page_num2, data2 in pages_list[i + 1:]:
            sig2 = get_signature(data2.content)
            score = similarity(sig1, sig2)
            if score > 0.9:  # Nearly identical content
                duplicates_found += 1
                issues.append(
                    f"Duplicate content: pages {page_num1} and {page_num2} (score={score:.2f})"
                )

    # ══════════════════════════════════════════════
    # 3. Check coverage
    # ══════════════════════════════════════════════
    missing_pages = sorted(set(original_pages.keys()) - set(final_pages.keys()))
    extra_pages = sorted(set(final_pages.keys()) - set(original_pages.keys()))

    if missing_pages:
        issues.append(f"Missing pages: {missing_pages}")
    if extra_pages:
        issues.append(f"Extra pages: {extra_pages}")

    # ══════════════════════════════════════════════
    # Build result
    # ══════════════════════════════════════════════
    return DocumentValidationResult(
        valid=len(issues) == 0,
        total_pages=len(final_pages),
        content_ok=content_ok,
        content_mismatch=content_mismatch,
        duplicates_found=duplicates_found,
        missing_pages=missing_pages,
        extra_pages=extra_pages,
        issues=issues
    )


def parse_page_block(block: str, page_num: int) -> PageData:
    """
    Парсит блок одной страницы и извлекает метаданные (V1 format).

    Формат:
    [PAGE_START]
    [PAGE_TYPE: clinical]
    [CODES: E11.9 (ICD-10), J1950 (HCPCS)]
    [TOPICS: GLP-1 indications, metformin failure]
    [MEDICATIONS: semaglutide, dulaglutide]

    ## Content here...
    [PAGE_END]
    """
    page = PageData(page=page_num, page_type='clinical')
    
    # Extract PAGE_TYPE
    match = re.search(r'\[PAGE_TYPE:\s*([^\]]+)\]', block, re.IGNORECASE)
    if match:
        page.page_type = match.group(1).strip().lower()
    
    # Extract CODES
    match = re.search(r'\[CODES:\s*([^\]]+)\]', block, re.IGNORECASE)
    if match:
        page.codes = parse_code_string(match.group(1))
    
    # Extract TOPICS (with anchor support)
    match = re.search(r'\[TOPICS:\s*([^\]]+)\]', block, re.IGNORECASE)
    if match:
        page.topics = parse_topics_string(match.group(1))

    # Extract MEDICATIONS (with anchor support)
    match = re.search(r'\[MEDICATIONS:\s*([^\]]+)\]', block, re.IGNORECASE)
    if match:
        page.medications = parse_medications_string(match.group(1))
    
    # Extract SKIP reason
    match = re.search(r'\[SKIP:\s*([^\]]+)\]', block, re.IGNORECASE)
    if match:
        page.skip_reason = match.group(1).strip()
        page.content = None
        return page
    
    # Extract content (remove metadata tags)
    content = block
    content = re.sub(r'\[PAGE_TYPE:[^\]]+\]', '', content)
    content = re.sub(r'\[CODES:[^\]]+\]', '', content)
    content = re.sub(r'\[TOPICS:[^\]]+\]', '', content)
    content = re.sub(r'\[MEDICATIONS:[^\]]+\]', '', content)
    content = re.sub(r'\[SKIP:[^\]]+\]', '', content)
    content = content.strip()
    
    # Check if content is meaningful
    if content and len(content) > 30 and content.upper() != 'EMPTY':
        page.content = content
    else:
        page.content = None
        if not page.skip_reason:
            page.skip_reason = 'Empty or minimal content'
            page.page_type = 'empty'
    
    # Post-process: validate codes and auto-find anchors
    if page.content and page.codes:
        page.codes = _postprocess_codes(page.codes, page.content)

    return page


def _postprocess_codes(codes: List[CodeInfo], content: str) -> List[CodeInfo]:
    """
    Post-process extracted codes:
    1. Validate that codes actually appear in content (filter false positives)
    2. Auto-find anchors for codes with anchor=None
    """
    validated_codes = []
    content_upper = content.upper()

    for code_info in codes:
        code = code_info.code.upper()

        # Check if code appears in content
        if code not in content_upper:
            # Code doesn't appear - likely false positive, skip it
            continue

        # If anchor is missing, try to find it in content
        if code_info.anchor is None:
            anchor = _find_code_anchor(code_info.code, content)
            if anchor:
                code_info = CodeInfo(
                    code=code_info.code,
                    type=code_info.type,
                    context=code_info.context,
                    anchor=anchor
                )

        validated_codes.append(code_info)

    return validated_codes


def _find_code_anchor(code: str, content: str) -> Optional[str]:
    """
    Find anchor text for a code in content.
    Extracts a short phrase (5-60 chars) containing the code.
    """
    # Find the code in content (case-insensitive)
    pattern = re.compile(re.escape(code), re.IGNORECASE)
    match = pattern.search(content)

    if not match:
        return None

    start_pos = match.start()
    end_pos = match.end()

    # Strategy 1: Check if code is in parentheses like "text (CODE)"
    # Look for pattern: "descriptive text (CODE)" or "(CODE)"
    paren_pattern = re.compile(
        r'([^()\n]{3,50})\s*\(' + re.escape(code) + r'\)',
        re.IGNORECASE
    )
    paren_match = paren_pattern.search(content)
    if paren_match:
        # Found "text (CODE)" pattern - use "text (CODE)" as anchor
        anchor_start = paren_match.start()
        anchor_end = paren_match.end() + 1  # Include closing paren
        return content[anchor_start:anchor_end].strip().strip('",;')

    # Strategy 2: Look for "CODE ... text" pattern (code first, then description)
    after_pattern = re.compile(
        re.escape(code) + r'[\s,:\-]+([^.\n]{3,40})',
        re.IGNORECASE
    )
    after_match = after_pattern.search(content)
    if after_match:
        return content[after_match.start():after_match.end()].strip().strip('",;')

    # Strategy 3: Extract minimal context around code
    # Go back to find start of phrase (commas, parens, or sentence boundaries)
    context_start = start_pos
    for i in range(start_pos - 1, max(0, start_pos - 40), -1):
        if content[i] in '.,;()\n':
            context_start = i + 1
            break

    # Go forward to find end of phrase
    context_end = end_pos
    for i in range(end_pos, min(len(content), end_pos + 20)):
        if content[i] in '.,;()\n':
            context_end = i
            break

    anchor = content[context_start:context_end].strip().strip('",;: ')

    # If anchor is just the code, try to get a bit more context
    if anchor.upper() == code.upper():
        # Expand slightly
        context_start = max(0, start_pos - 30)
        context_end = min(len(content), end_pos + 10)
        anchor = content[context_start:context_end].strip()
        # Clean up
        anchor = anchor.strip('.,;:!? \t\n"')

    return anchor if len(anchor) >= len(code) else None


def parse_chunk_response(response_text: str, start_page: int, expected_count: int) -> List[PageData]:
    """
    Парсит ответ Gemini с маркерами и метаданными.
    Возвращает список PageData.
    """
    # Extract all blocks between markers
    pattern = r'\[PAGE_START\](.*?)\[PAGE_END\]'
    blocks = re.findall(pattern, response_text, re.DOTALL)
    
    results = []
    
    for i, block in enumerate(blocks):
        page_num = start_page + i
        page_data = parse_page_block(block, page_num)
        results.append(page_data)
    
    # Warning if count mismatch
    if len(blocks) != expected_count:
        print(f"⚠ WARNING: Expected {expected_count} pages, got {len(blocks)} blocks")
    
    return results


def merge_chunk_results(all_chunks: List[List[PageData]]) -> List[PageData]:
    """Объединяет результаты всех чанков и сортирует по номеру страницы"""
    all_pages = []
    for chunk in all_chunks:
        all_pages.extend(chunk)
    
    # Sort by page number
    all_pages.sort(key=lambda p: p.page)
    
    return all_pages


def build_document_data(
    file_hash: str,
    filename: str,
    total_pages: int,
    pages: List[PageData]
) -> DocumentData:
    """Создаёт финальную структуру документа"""
    return DocumentData(
        file_hash=file_hash,
        filename=filename,
        total_pages=total_pages,
        parsed_at=datetime.utcnow().isoformat() + 'Z',
        pages=pages
    )


def save_document_files(doc: DocumentData, output_dir: str) -> Tuple[str, str]:
    """
    Сохраняет документ в два файла:
    - content.txt — текст с ## Page N маркерами
    - content.json — структурированные данные
    
    Returns: (txt_path, json_path)
    """
    import os
    
    doc_dir = os.path.join(output_dir, doc.file_hash)
    os.makedirs(doc_dir, exist_ok=True)
    
    # Save TXT
    txt_path = os.path.join(doc_dir, 'content.txt')
    txt_content = []
    for page in doc.pages:
        if page.content:
            txt_content.append(f"## Page {page.page}\n{page.content}")
    
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write('\n\n'.join(txt_content))
    
    # Save JSON
    json_path = os.path.join(doc_dir, 'content.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(doc.to_dict(), f, indent=2, ensure_ascii=False)
    
    return txt_path, json_path


# ============================================================
# PROMPT TEMPLATE
# ============================================================

CHUNK_EXTRACTION_PROMPT = """Extract text and metadata from these {pages_count} PDF pages.

=== OUTPUT FORMAT ===

For EACH page in order, use this EXACT format:

[PAGE_START]
[PAGE_TYPE: clinical|administrative|reference|toc|empty]
[CODES: CODE1 (TYPE1), CODE2 (TYPE2), ...]
[TOPICS: topic1, topic2, topic3]
[MEDICATIONS: drug1, drug2]

Page content as plain text here...
[PAGE_END]

CODES format: each code MUST have type in parentheses.
TYPE is one of: ICD-10, HCPCS, CPT, NDC
Example: [CODES: E11.9 (ICD-10), J1950 (HCPCS), 99213 (CPT)]

For pages to skip, use:

[PAGE_START]
[PAGE_TYPE: reference]
[SKIP: References page - no clinical content]
[PAGE_END]

=== PAGE TYPES ===

- "clinical": Medical criteria, indications, contraindications, dosing, guidelines
- "administrative": Billing rules, PA requirements, quantity limits, step therapy
- "reference": Citations, bibliography, sources, footnotes
- "toc": Table of contents, index, glossary
- "empty": Blank, title page, copyright, acknowledgements

=== PAGES TO SKIP (use [SKIP: reason]) ===

- Table of Contents → [SKIP: Table of Contents]
- Index/Glossary → [SKIP: Index page]
- References/Citations → [SKIP: References page]
- Copyright/Title pages → [SKIP: Title/Copyright page]
- Blank pages → [SKIP: Blank page]
- Pages with only headers/footers → [SKIP: Header/footer only]

=== CODE EXTRACTION RULES ===

Extract medical codes with ANCHOR TEXT (exact quote from document).

Format: [CODES: CODE (TYPE: "anchor text"), CODE (TYPE: "anchor text"), ...]

TYPE must be ONE of these 4 values ONLY:
- ICD-10 (diagnosis codes starting with letter: E11.9, Z79.4, F32.1)
- HCPCS (letter + 4 digits: J1950, A4253, E0607)
- CPT (5 digits: 99213, 96372, 47533)
- NDC (drug codes: 0002-1433-80)

ANCHOR TEXT rules:
- Put the EXACT phrase from the document in quotes
- This is the text that mentions or implies the code
- Used for citation highlighting in PDF viewer
- Keep it short (3-15 words), enough to find in document

CORRECT examples:
[CODES: E11.9 (ICD-10: "diagnosis of type 2 diabetes"), J1950 (HCPCS: "semaglutide injection")]
[CODES: Z79.4 (ICD-10: "long-term insulin use"), 99213 (CPT: "established patient visit")]

For INFERRED codes (condition mentioned but code not written):
[CODES: E11.9 (ICD-10: "type 2 diabetes mellitus")]
[CODES: E78.2 (ICD-10: "atherosclerotic cardiovascular disease")]

For LITERAL codes (code appears in document text):
[CODES: J1950 (HCPCS: "J1950")]
[CODES: 99213 (CPT: "99213-99215")]

WRONG - do not put descriptions in TYPE:
[CODES: 99213 (Office visit)] ← WRONG, should be (CPT: "anchor")
[CODES: J1950 (semaglutide injection)] ← WRONG, should be (HCPCS: "anchor")

If no codes on page: [CODES: -]

=== TOPICS ===

Extract 2-5 key topics/themes with ANCHOR TEXT (exact quote from document).

Format: [TOPICS: "topic name" ("anchor text"), "topic name" ("anchor text"), ...]

ANCHOR TEXT rules:
- Put the EXACT phrase from the document in parentheses with quotes
- This is the text that discusses this topic
- Keep anchors short (5-20 words), enough to find in document

Examples:
[TOPICS: "GLP-1 indications" ("indicated for the treatment of type 2 diabetes"), "metformin failure" ("inadequate response or intolerance to metformin")]
[TOPICS: "contraindications" ("contraindicated in individuals with"), "dosing" ("1 mg/dose (4 mg prefilled pen)")]

If no clear topics: [TOPICS: -]

=== MEDICATIONS ===

Extract drug names (generic and brand) with ANCHOR TEXT (exact quote from document).

Format: [MEDICATIONS: "drug name" ("anchor text"), "drug name" ("anchor text"), ...]

ANCHOR TEXT rules:
- Put the EXACT phrase from the document in parentheses with quotes
- Include the context where medication is mentioned
- Keep anchors short (5-20 words)

Examples:
[MEDICATIONS: "semaglutide" ("Semaglutide (Ozempic): J1950"), "dulaglutide" ("Dulaglutide (Trulicity): J3490")]
[MEDICATIONS: "metformin" ("trial and inadequate response or intolerance to metformin")]

If no medications: [MEDICATIONS: -]

=== CONTENT RULES ===

1. Output EXACTLY {pages_count} page blocks
2. Preserve text EXACTLY as written (typos, spacing)
3. Convert tables to PLAIN TEXT - use bullet lists or "Key: Value" format, NO markdown tables
4. Remove printed page numbers (standalone "182", "45" at top/bottom)
5. Remove repeated headers/footers
6. Keep clinical content, criteria, and rules intact
7. NO HTML tags (<br>, etc.) - plain text only

=== TABLE CONVERSION ===

IMPORTANT: Do NOT use markdown table syntax (|---|). Convert tables to plain text:

Original table:
| Drug | Brand | Code |
|------|-------|------|
| Semaglutide | Ozempic | J1950 |

Convert to:
- Semaglutide (Ozempic): J1950
- Dulaglutide (Trulicity): J3490

Or use "Key: Value" format:
Drug: Semaglutide, Brand: Ozempic, Code: J1950

=== EXAMPLE OUTPUT ===

[PAGE_START]
[PAGE_TYPE: clinical]
[CODES: E11.9 (ICD-10: "type 2 diabetes mellitus"), J1950 (HCPCS: "Semaglutide (Ozempic): J1950"), J3490 (HCPCS: "Dulaglutide (Trulicity): J3490")]
[TOPICS: "GLP-1 indications" ("indicated for the treatment of type 2 diabetes mellitus"), "metformin failure" ("Metformin is contraindicated or not tolerated"), "HbA1c targets" ("HbA1c remains above target")]
[MEDICATIONS: "semaglutide" ("Semaglutide (Ozempic): J1950"), "dulaglutide" ("Dulaglutide (Trulicity): J3490"), "liraglutide" ("Liraglutide (Victoza): J3490")]

Glucagon-Like Peptide-1 (GLP-1) Receptor Agonists

Indications:
GLP-1 receptor agonists are indicated for the treatment of type 2 diabetes mellitus (E11.*) in adults when:
- Metformin is contraindicated or not tolerated
- HbA1c remains above target despite metformin monotherapy

Available GLP-1 agents:
- Semaglutide (Ozempic): J1950
- Dulaglutide (Trulicity): J3490
- Liraglutide (Victoza): J3490
[PAGE_END]

[PAGE_START]
[PAGE_TYPE: reference]
[SKIP: References page - no clinical content]
[PAGE_END]

=== PAGES IN THIS CHUNK ===

Total: {pages_count} pages
Starting from PDF page: {start_page}

Output EXACTLY {pages_count} [PAGE_START]...[PAGE_END] blocks.
"""


def get_chunk_prompt(pages_count: int, start_page: int) -> str:
    """Генерирует prompt для chunk extraction (V1)"""
    return CHUNK_EXTRACTION_PROMPT.format(
        pages_count=pages_count,
        start_page=start_page
    )


# ============================================================
# V2 PROMPT TEMPLATE - JSON format extraction
# ============================================================

CHUNK_EXTRACTION_PROMPT_V2 = """Extract ONLY metadata from these {pages_count} PDF pages.
Return a JSON array with one object per page.

=== JSON OUTPUT FORMAT ===

[
  {{
    "page": 1,
    "skip": "TOC",
    "page_type": null,
    "codes": [],
    "topics": [],
    "medications": []
  }},
  {{
    "page": 2,
    "skip": null,
    "page_type": "clinical",
    "codes": [
      {{
        "code": "E",
        "type": "ICD-10",
        "anchor_start": "GLP-1 receptor agonists are indicated",
        "anchor_end": "for the treatment of type 2 diabetes",
        "reason": "diabetes treatment guidelines"
      }}
    ],
    "topics": [
      {{
        "name": "Type 2 Diabetes Mellitus",
        "anchor_start": "indicated for adults with type 2",
        "anchor_end": "who have inadequate glycemic control",
        "reason": "primary condition discussed"
      }}
    ],
    "medications": [
      {{
        "name": "semaglutide",
        "anchor_start": "Semaglutide (Ozempic) is administered",
        "anchor_end": "once weekly by subcutaneous injection",
        "reason": "GLP-1 agonist drug mentioned"
      }}
    ]
  }}
]

=== FIELD TYPES ===

page: integer (page number starting from {start_page})
skip: string | null ("TOC", "References", "Acknowledgments", "Release notes", "Title page", "Blank", or null)
page_type: string | null ("clinical" or "administrative", null if skipped)
codes: array of objects
  - code: string (single letter/digit category: E, F, I, J, 9)
  - type: string ("ICD-10", "HCPCS", "CPT", or "NDC")
  - anchor_start: string | null (first 6-10 words of paragraph, VERBATIM)
  - anchor_end: string | null (last 6-10 words of same paragraph, VERBATIM)
  - reason: string (MANDATORY - why this code is relevant)
topics: array of objects
  - name: string (topic name from dictionary)
  - anchor_start: string | null
  - anchor_end: string | null
  - reason: string (MANDATORY)
medications: array of objects
  - name: string (drug name as mentioned: semaglutide, Ozempic, metformin)
  - anchor_start: string | null
  - anchor_end: string | null
  - reason: string (MANDATORY)

=== SKIP PAGES ===

Set "skip" field for: Table of Contents, Index, References, Bibliography,
Acknowledgments, Release notes, Changelog, Copyright page, Title page, Blank pages.
When skip is set, leave codes/topics/medications as empty arrays [].

=== ANCHOR RULES ===

Anchors are SEARCH STRINGS. Ctrl+F must find EXACT match in page text!
- anchor_start = first 6-10 words of relevant paragraph, VERBATIM
- anchor_end = last 6-10 words of SAME paragraph, VERBATIM
- Copy character-by-character - no paraphrasing, no adding words!
- If cannot find exact phrase → set to null

=== CODE CATEGORIES ===

{meta_categories}

Use SINGLE LETTER/DIGIT for "code" field: E, F, I, J, 9 (NOT E11, F32, J1950)
Match by MEANING: "diabetes" → E, "depression" → F, "injectable" → J

=== TOPICS DICTIONARY ===

{topics_dictionary}

ONLY use topic names from this dictionary. Skip unlisted topics.

=== MEDICATIONS ===

Extract SPECIFIC drug names mentioned in the text:
- Generic names: semaglutide, metformin, dulaglutide, insulin, aspirin
- Brand names: Ozempic, Trulicity, Victoza, Byetta, Januvia
- Drug classes when specific: GLP-1 receptor agonists, SGLT2 inhibitors, DPP-4 inhibitors

DO NOT extract:
- General terms: "medication", "drug", "therapy", "treatment"
- Code-only references: "J1950" without drug name

Examples of what TO extract:
- "semaglutide (Ozempic)" → name: "semaglutide"
- "metformin is contraindicated" → name: "metformin"
- "GLP-1 receptor agonists such as dulaglutide" → name: "dulaglutide"

=== OUTPUT ===

Return ONLY valid JSON array. No markdown code blocks. No explanations.
Array must have EXACTLY {pages_count} objects, one per page.
"""


def load_meta_categories_from_json(json_path: str = None) -> dict:
    """Load meta-categories from JSON file."""
    import json as json_module
    from pathlib import Path

    if json_path is None:
        # Default path
        json_path = Path(__file__).parent.parent.parent / "data" / "seed" / "meta_categories.json"

    with open(json_path, 'r', encoding='utf-8') as f:
        return json_module.load(f)


def format_meta_categories_for_prompt(meta_cats: dict) -> str:
    """Format meta-categories dictionary for injection into prompt."""
    lines = []

    for code_type, type_data in meta_cats.items():
        lines.append(f"\n[{code_type}] - {type_data.get('description', '')}")

        for meta_key, cat_data in type_data.get('categories', {}).items():
            name = cat_data.get('name', '')
            range_str = cat_data.get('range', '')
            includes = cat_data.get('includes', [])

            # Format: "E (E00-E89): Endocrine, nutritional... - diabetes, thyroid, obesity"
            includes_str = ', '.join(includes[:4]) if includes else ''
            lines.append(f"  {meta_key} ({range_str}): {name}")
            if includes_str:
                lines.append(f"      includes: {includes_str}")

    return '\n'.join(lines)


def format_topics_for_prompt(topics: list) -> str:
    """Format topics dictionary for injection into prompt."""
    lines = []
    by_category = {}

    for topic in topics:
        cat = topic.get('category', 'other')
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append(topic)

    for category in sorted(by_category.keys()):
        lines.append(f"\n[{category.upper()}]")
        for topic in by_category[category]:
            name = topic['name']
            aliases = topic.get('aliases', [])
            if aliases:
                aliases_str = ', '.join(aliases[:3])  # Limit to 3 aliases
                lines.append(f"- {name} (aliases: {aliases_str})")
            else:
                lines.append(f"- {name}")

    return '\n'.join(lines)


def get_chunk_prompt_v2(
    pages_count: int,
    start_page: int,
    topics: list,
    meta_categories: dict = None
) -> str:
    """
    Генерирует prompt для chunk extraction V2 (category-based).

    Args:
        pages_count: Number of pages in this chunk
        start_page: Starting page number
        topics: List of topics from topics_dictionary
        meta_categories: Meta-categories dict (loaded from JSON if None)
    """
    if meta_categories is None:
        meta_categories = load_meta_categories_from_json()

    topics_str = format_topics_for_prompt(topics)
    meta_cats_str = format_meta_categories_for_prompt(meta_categories)

    return CHUNK_EXTRACTION_PROMPT_V2.format(
        pages_count=pages_count,
        start_page=start_page,
        topics_dictionary=topics_str,
        meta_categories=meta_cats_str
    )


# ============================================================
# TESTS
# ============================================================

if __name__ == '__main__':
    # Test parsing
    test_response = """
[PAGE_START]
[PAGE_TYPE: clinical]
[CODES: E11.9 (ICD-10: T2DM), J1950 (HCPCS: semaglutide)]
[TOPICS: GLP-1 indications, metformin failure]
[MEDICATIONS: semaglutide, dulaglutide]

## GLP-1 Receptor Agonists

GLP-1 receptor agonists are indicated for T2DM treatment.

| Drug | Brand | Code |
|------|-------|------|
| Semaglutide | Ozempic | J1950 |
[PAGE_END]

[PAGE_START]
[PAGE_TYPE: clinical]
[CODES: E11.65 (ICD-10)]
[TOPICS: contraindications, side effects]
[MEDICATIONS: -]

## Contraindications

- History of MTC
- MEN 2 syndrome
[PAGE_END]

[PAGE_START]
[PAGE_TYPE: reference]
[SKIP: References page]
[PAGE_END]
"""
    
    pages = parse_chunk_response(test_response, start_page=1, expected_count=3)
    
    print("=== PARSED PAGES ===")
    for page in pages:
        print(f"\nPage {page.page} ({page.page_type}):")
        print(f"  Codes: {[(c.code, c.type) for c in page.codes]}")
        print(f"  Topics: {page.topics}")
        print(f"  Medications: {page.medications}")
        print(f"  Skip: {page.skip_reason}")
        print(f"  Content: {page.content[:100] if page.content else 'None'}...")
    
    # Build document
    doc = build_document_data(
        file_hash='test123',
        filename='test_document.pdf',
        total_pages=3,
        pages=pages
    )
    
    print("\n=== DOCUMENT SUMMARY ===")
    print(json.dumps(doc.summary, indent=2))