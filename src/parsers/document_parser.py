"""
Document Parser - извлекает текст и метаданные из PDF через Gemini.

Формат вывода Gemini:
[PAGE_START]
[PAGE_TYPE: clinical|administrative|reference|toc|empty]
[CODES: E11.9 (ICD-10), J1950 (HCPCS)]
[TOPICS: topic1, topic2]
[MEDICATIONS: drug1, drug2]
[SKIP: reason if skipped]

## Content here...
[PAGE_END]

Parser извлекает метаданные и формирует JSON структуру.
"""

import re
import json
import hashlib
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class CodeInfo:
    code: str
    type: str  # ICD-10, HCPCS, CPT, NDC
    context: Optional[str] = None


@dataclass
class PageData:
    page: int
    page_type: str  # clinical, administrative, reference, toc, empty
    content: Optional[str] = None
    codes: List[CodeInfo] = None
    topics: List[str] = None
    medications: List[str] = None
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
        all_topics = set()
        all_medications = set()
        content_pages = []
        skipped_pages = []
        
        for page in self.pages:
            if page.content:
                content_pages.append(page.page)
            else:
                skipped_pages.append(page.page)
            
            # Aggregate codes with pages
            for code_info in page.codes:
                key = code_info.code
                if key not in all_codes:
                    all_codes[key] = {
                        'code': code_info.code,
                        'type': code_info.type,
                        'pages': [],
                        'contexts': []
                    }
                if page.page not in all_codes[key]['pages']:
                    all_codes[key]['pages'].append(page.page)
                if code_info.context:
                    all_codes[key]['contexts'].append(code_info.context)
            
            all_topics.update(page.topics)
            all_medications.update(page.medications)
        
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
            'topics': sorted(all_topics),
            'medications': sorted(all_medications),
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
                    'topics': p.topics,
                    'medications': p.medications,
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
    Парсит строку кодов: "E11.9 (ICD-10), J1950 (HCPCS), 99213 (CPT)"
    Нормализует wildcard паттерны (*, -, x → %)
    """
    codes = []
    if not code_str or code_str.strip() == '-':
        return codes

    # Pattern: CODE (TYPE) или CODE (TYPE: context)
    pattern = r'([A-Z0-9\.\-\*]+)\s*\(([^:)]+)(?::\s*([^)]+))?\)'
    matches = re.findall(pattern, code_str, re.IGNORECASE)

    for match in matches:
        code = match[0].strip()
        code_type = match[1].strip().upper()
        context = match[2].strip() if len(match) > 2 and match[2] else None

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

        codes.append(CodeInfo(code=code, type=code_type, context=context))

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
            codes.append(CodeInfo(code=part, type=code_type, context=None))
    
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


def parse_page_block(block: str, page_num: int) -> PageData:
    """
    Парсит блок одной страницы и извлекает метаданные.
    
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
    
    # Extract TOPICS
    match = re.search(r'\[TOPICS:\s*([^\]]+)\]', block, re.IGNORECASE)
    if match:
        page.topics = parse_list_string(match.group(1))
    
    # Extract MEDICATIONS
    match = re.search(r'\[MEDICATIONS:\s*([^\]]+)\]', block, re.IGNORECASE)
    if match:
        page.medications = parse_list_string(match.group(1))
    
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
    
    return page


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

Extract medical codes in this EXACT format:
[CODES: CODE (TYPE), CODE (TYPE), ...]

TYPE must be ONE of these 4 values ONLY:
- ICD-10 (diagnosis codes starting with letter: E11.9, Z79.4, F32.1)
- HCPCS (letter + 4 digits: J1950, A4253, E0607)
- CPT (5 digits: 99213, 96372, 47533)
- NDC (drug codes: 0002-1433-80)

CORRECT examples:
[CODES: E11.9 (ICD-10), J1950 (HCPCS), 99213 (CPT)]
[CODES: Z79.4 (ICD-10), A4253 (HCPCS)]

WRONG - do not put descriptions in TYPE:
[CODES: 99213 (Office visit)] ← WRONG, should be (CPT)
[CODES: J1950 (semaglutide injection)] ← WRONG, should be (HCPCS)
[CODES: 47533 (BILIARY TRACT)] ← WRONG, should be (CPT)

If no codes on page: [CODES: -]

=== TOPICS ===

Extract 2-5 key topics/themes from the page content.
Examples: GLP-1 indications, metformin failure, HbA1c targets, contraindications, dosing

If no clear topics: [TOPICS: -]

=== MEDICATIONS ===

Extract drug names (generic and brand) mentioned on the page.
Examples: semaglutide, Ozempic, dulaglutide, Trulicity, metformin

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
[CODES: E11.9 (ICD-10), J1950 (HCPCS), J3490 (HCPCS)]
[TOPICS: GLP-1 indications, metformin failure, HbA1c targets]
[MEDICATIONS: semaglutide, dulaglutide, liraglutide]

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
    """Генерирует prompt для chunk extraction"""
    return CHUNK_EXTRACTION_PROMPT.format(
        pages_count=pages_count,
        start_page=start_page
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