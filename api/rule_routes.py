"""
Rule Generation API Routes

Endpoints для генерации правил валидации кодов.
Поддерживает иерархическую модель с наследованием документов и правил.

Иерархия:
    E%          → Chapter level
    E11.%       → Category level
    E11.9       → Specific code
    47531:47541 → Range

Документы наследуются сверху вниз:
    E11.9 total_docs = own(4) + E11.%(3) + E%(2) = 9

Правила ищутся снизу вверх (каскадно):
    E11.9 → E11.% → E% → None
"""

import os
import json
import re
import asyncio
from typing import List, Dict, Optional, Set, Tuple
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.utils.code_categories import get_code_category, group_codes_by_category, get_all_categories
from src.db.connection import get_db_connection

# NEW: Import generators
from src.generators import (
    build_sources_context,
    RuleGenerator,
)

router = APIRouter(prefix="/api/rules", tags=["rules"])

# Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RULES_DIR = os.path.join(BASE_DIR, "data", "rules")
DOCUMENTS_DIR = os.path.join(BASE_DIR, "data", "processed", "documents")

os.makedirs(RULES_DIR, exist_ok=True)


# ============================================================
# MODELS
# ============================================================

class GenerateRuleRequest(BaseModel):
    code: str = ""
    code_type: str = "ICD-10"
    document_ids: Optional[List[str]] = None  # If None, use all relevant docs
    parallel_validators: bool = True  # Run Mentor || RedTeam in parallel
    thinking_budget: int = 10000


class BatchGenerateRequest(BaseModel):
    codes: List[str]
    code_type: str = "ICD-10"


# ============================================================
# HELPERS - CODE HIERARCHY
# ============================================================

def get_wildcard_patterns_for_code(code: str) -> List[str]:
    """
    Генерирует wildcard паттерны которые могут покрывать данный код.

    Для E11.9 возвращает: ['E11.%', 'E1%.%', 'E%']
    Для J1950 возвращает: ['J195%', 'J19%', 'J1%', 'J%']
    """
    if not code:
        return []

    code = code.upper()
    patterns = []

    # Skip if already a pattern
    if '%' in code or ':' in code:
        return []

    # ICD-10 style codes (E11.9, F32.1)
    if '.' in code:
        parts = code.split('.')
        base = parts[0]
        patterns.append(f"{base}.%")
        for i in range(len(base) - 1, 0, -1):
            patterns.append(f"{base[:i]}%.%")
        if len(base) >= 1:
            patterns.append(f"{base[0]}%")
    else:
        # HCPCS/CPT style (J1950, 99213)
        for i in range(len(code) - 1, 0, -1):
            patterns.append(f"{code[:i]}%")

    return list(dict.fromkeys(patterns))


def is_wildcard_code(code: str) -> bool:
    """Проверяет является ли код wildcard паттерном."""
    return code and '%' in code


def is_range_code(code: str) -> bool:
    """Проверяет является ли код диапазоном (47531:47541)."""
    return code and ':' in code


def code_matches_pattern(code: str, pattern: str) -> bool:
    """
    Проверяет соответствует ли код паттерну.
    Поддерживает wildcard (%) и range (:).
    """
    if not code or not pattern:
        return False

    code = code.upper()
    pattern = pattern.upper()

    if pattern == code:
        return True

    # Range match
    if ':' in pattern:
        parts = pattern.split(':')
        if len(parts) == 2:
            start, end = parts
            try:
                if code.isdigit() and start.isdigit() and end.isdigit():
                    return int(start) <= int(code) <= int(end)
                # Alphanumeric - extract common prefix
                common = ""
                for c1, c2, c3 in zip(code, start, end):
                    if c1 == c2 == c3 and c1.isalpha():
                        common += c1
                    else:
                        break
                if common:
                    c_suf = code[len(common):]
                    s_suf = start[len(common):]
                    e_suf = end[len(common):]
                    if all(x.replace('.','').isdigit() for x in [c_suf, s_suf, e_suf]):
                        c_num = float(c_suf) if '.' in c_suf else int(c_suf)
                        s_num = float(s_suf) if '.' in s_suf else int(s_suf)
                        e_num = float(e_suf) if '.' in e_suf else int(e_suf)
                        return s_num <= c_num <= e_num
                return start <= code <= end
            except:
                return start <= code <= end

    # Wildcard match
    if '%' in pattern:
        regex = '^' + pattern.replace('%', '.*').replace('_', '.') + '$'
        return bool(re.match(regex, code))

    return False


def get_rule_cascade(code: str) -> List[str]:
    """
    Возвращает список паттернов для каскадного поиска правила.
    От специфичного к общему.

    E11.9 → ['E11.9', 'E11.%', 'E1%.%', 'E%']
    """
    cascade = [code.upper()]
    cascade.extend(get_wildcard_patterns_for_code(code))
    return cascade


def find_matching_ranges(code: str, all_ranges: List[str]) -> List[str]:
    """Находит все range паттерны покрывающие код."""
    return [r for r in all_ranges if code_matches_pattern(code, r)]


def get_all_codes_from_db() -> List[Dict]:
    """
    Получает все коды из базы данных с информацией о документах.
    Поддерживает иерархическое наследование:
    - wildcard patterns (E11.%)
    - range patterns (47531:47541)

    Для каждого конкретного кода добавляет документы из:
    1. explicit match (E11.9)
    2. wildcard parents (E11.%, E%)
    3. range patterns (47531:47541)

    Исключает policy документы.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get all code patterns from DB
    cursor.execute("""
        SELECT 
            dc.code_pattern as code,
            dc.code_type,
            dc.document_id,
            d.filename
        FROM document_codes dc
        JOIN documents d ON dc.document_id = d.file_hash
        WHERE d.parsed_at IS NOT NULL
          AND (d.doc_type IS NULL OR d.doc_type != 'policy')
        GROUP BY dc.code_pattern, dc.code_type, dc.document_id
        ORDER BY dc.code_pattern
    """)

    rows = cursor.fetchall()
    conn.close()

    # Classify codes into categories
    codes_map = {}         # Specific codes
    wildcard_codes = {}    # Wildcard patterns (E11.%)
    range_codes = {}       # Range patterns (47531:47541)

    for row in rows:
        code = row[0].upper()
        code_type = row[1] or 'ICD-10'
        doc_id = row[2]
        filename = row[3]

        doc_info = {
            'id': doc_id,
            'filename': filename,
            'via_pattern': None
        }

        if is_wildcard_code(code):
            if code not in wildcard_codes:
                wildcard_codes[code] = {'type': code_type, 'documents': []}
            wildcard_codes[code]['documents'].append(doc_info)

        elif is_range_code(code):
            if code not in range_codes:
                range_codes[code] = {'type': code_type, 'documents': []}
            range_codes[code]['documents'].append(doc_info)

        else:
            # Specific code
            if code not in codes_map:
                codes_map[code] = {
                    'code': code,
                    'type': code_type,
                    'documents': [],
                    'inherited_documents': [],
                    'total_docs': 0
                }
            codes_map[code]['documents'].append(doc_info)

    # Add wildcard patterns as "codes" too (for UI hierarchy)
    for pattern, data in wildcard_codes.items():
        codes_map[pattern] = {
            'code': pattern,
            'type': data['type'],
            'documents': data['documents'],
            'inherited_documents': [],
            'total_docs': len(data['documents']),
            'is_wildcard': True
        }

    # Add ranges as "codes" too
    for pattern, data in range_codes.items():
        codes_map[pattern] = {
            'code': pattern,
            'type': data['type'],
            'documents': data['documents'],
            'inherited_documents': [],
            'total_docs': len(data['documents']),
            'is_range': True
        }

    # Calculate inheritance for specific codes
    all_ranges = list(range_codes.keys())

    for code, data in codes_map.items():
        # Skip patterns - they don't inherit
        if is_wildcard_code(code) or is_range_code(code):
            continue

        seen_doc_ids = {d['id'] for d in data['documents']}

        # Inherit from wildcard parents
        patterns = get_wildcard_patterns_for_code(code)
        for pattern in patterns:
            if pattern in wildcard_codes:
                for doc in wildcard_codes[pattern]['documents']:
                    if doc['id'] not in seen_doc_ids:
                        data['inherited_documents'].append({
                            'id': doc['id'],
                            'filename': doc['filename'],
                            'via_pattern': pattern
                        })
                        seen_doc_ids.add(doc['id'])

        # Inherit from matching ranges
        matching_ranges = find_matching_ranges(code, all_ranges)
        for range_pattern in matching_ranges:
            for doc in range_codes[range_pattern]['documents']:
                if doc['id'] not in seen_doc_ids:
                    data['inherited_documents'].append({
                        'id': doc['id'],
                        'filename': doc['filename'],
                        'via_pattern': range_pattern
                    })
                    seen_doc_ids.add(doc['id'])

        # Calculate total
        data['total_docs'] = len(data['documents']) + len(data['inherited_documents'])

    return list(codes_map.values())


def get_rule_status(code: str, cascade: bool = True) -> Dict:
    """
    Проверяет статус правила для кода.

    Args:
        code: Код для проверки
        cascade: Если True, ищет правило каскадно (E11.9 → E11.% → E%)

    Returns:
        {
            'has_rule': bool,
            'is_mock': bool,
            'is_inherited': bool,      # True если правило от родительского паттерна
            'matched_pattern': str,    # какой паттерн сработал
            'version': int,
            'created_at': str,
            'path': str
        }
    """
    # Determine search patterns
    if cascade:
        patterns_to_check = get_rule_cascade(code)
    else:
        patterns_to_check = [code.upper()]

    for pattern in patterns_to_check:
        result = _check_rule_exists(pattern)
        if result['has_rule']:
            result['is_inherited'] = pattern.upper() != code.upper()
            result['matched_pattern'] = pattern
            return result

    return {
        'has_rule': False,
        'is_mock': False,
        'is_inherited': False,
        'matched_pattern': None
    }


def _check_rule_exists(pattern: str) -> Dict:
    """Проверяет существование правила для конкретного паттерна."""
    # Normalize pattern for filesystem
    safe_pattern = pattern.replace(".", "_").replace("/", "_").replace("%", "X").replace(":", "-")

    # Check versioned structure first
    versioned_path = os.path.join(RULES_DIR, safe_pattern)

    if os.path.exists(versioned_path) and os.path.isdir(versioned_path):
        versions = [d for d in os.listdir(versioned_path) if d.startswith('v')]
        if versions:
            latest = sorted(versions, key=lambda x: int(x[1:]))[-1]
            rule_path = os.path.join(versioned_path, latest, "rule.json")
            if os.path.exists(rule_path):
                try:
                    with open(rule_path, 'r') as f:
                        rule = json.load(f)
                    return {
                        'has_rule': True,
                        'is_mock': rule.get('is_mock', False),
                        'version': rule.get('version', 1),
                        'created_at': rule.get('created_at'),
                        'validation_status': rule.get('validation_status', 'unknown'),
                        'path': rule_path
                    }
                except:
                    pass

    # Fallback to old flat structure
    rule_path = os.path.join(RULES_DIR, f"{safe_pattern}.json")

    if os.path.exists(rule_path):
        try:
            with open(rule_path, 'r') as f:
                rule = json.load(f)
            return {
                'has_rule': True,
                'is_mock': rule.get('is_mock', False),
                'version': rule.get('version', '1.0'),
                'created_at': rule.get('created_at'),
                'updated_at': rule.get('updated_at'),
                'path': rule_path
            }
        except:
            pass

    return {'has_rule': False, 'is_mock': False}


def find_rule_for_code(code: str) -> Optional[Dict]:
    """
    Находит и загружает правило для кода (каскадный поиск).

    Returns:
        {
            'pattern': str,           # какой паттерн сработал
            'is_inherited': bool,
            'rule_data': dict,        # содержимое правила
            'path': str
        }
        или None если правило не найдено
    """
    status = get_rule_status(code, cascade=True)

    if not status['has_rule']:
        return None

    try:
        with open(status['path'], 'r') as f:
            rule_data = json.load(f)

        return {
            'pattern': status['matched_pattern'],
            'is_inherited': status['is_inherited'],
            'rule_data': rule_data,
            'path': status['path']
        }
    except:
        return None


def get_guideline_text_for_code(code: str, document_ids: Optional[List[str]] = None) -> str:
    """
    Собирает текст гайдлайнов для кода из всех релевантных документов.
    DEPRECATED: Use build_sources_context() instead for multi-doc support.
    """
    sources_ctx = build_sources_context(code, document_ids)
    return sources_ctx.sources_text


# ============================================================
# ENDPOINTS
# ============================================================

@router.get("/categories")
async def get_categories():
    """
    Получает список категорий с количеством кодов и статусом покрытия.
    """
    all_codes = get_all_codes_from_db()
    grouped = group_codes_by_category(all_codes)

    categories = []
    for category_name, codes in grouped.items():
        # Count rules
        codes_with_rules = sum(1 for c in codes if get_rule_status(c['code'])['has_rule'])

        # Get color from first code's category_info (safe access)
        category_info = codes[0].get('category_info', {}) if codes else {}
        color = category_info.get('color', '#6B7280')

        categories.append({
            'name': category_name,
            'color': color,
            'total_codes': len(codes),
            'codes_with_rules': codes_with_rules,
            'coverage_percent': round(codes_with_rules / len(codes) * 100) if codes else 0
        })

    # Sort by name
    categories.sort(key=lambda x: x['name'])

    return {
        'categories': categories,
        'total_codes': len(all_codes),
        'total_with_rules': sum(c['codes_with_rules'] for c in categories)
    }


@router.get("/categories/{category_name}/codes")
async def get_codes_by_category(category_name: str):
    """
    Получает коды в категории с информацией о документах и статусе правил.
    Разделяет на diagnoses (ICD-10) и procedures (CPT/HCPCS).
    """
    all_codes = get_all_codes_from_db()
    grouped = group_codes_by_category(all_codes)

    if category_name not in grouped:
        raise HTTPException(status_code=404, detail=f"Category '{category_name}' not found")

    codes = grouped[category_name]

    # Enrich with rule status and split by type
    diagnoses = []
    procedures = []

    for code_info in codes:
        rule_status = get_rule_status(code_info['code'])
        enriched = {
            **code_info,
            'rule_status': rule_status
        }

        # ICD-10 = diagnosis, CPT/HCPCS = procedure
        code_type = code_info.get('type', 'ICD-10')
        if code_type in ['CPT', 'HCPCS']:
            procedures.append(enriched)
        else:
            diagnoses.append(enriched)

    # Sort by code
    diagnoses.sort(key=lambda x: x['code'])
    procedures.sort(key=lambda x: x['code'])

    all_codes_enriched = diagnoses + procedures

    return {
        'category': category_name,
        'diagnoses': diagnoses,
        'procedures': procedures,
        'total': len(all_codes_enriched),
        'total_diagnoses': len(diagnoses),
        'total_procedures': len(procedures),
        'with_rules': sum(1 for c in all_codes_enriched if c['rule_status']['has_rule'])
    }


@router.get("/codes/{code}")
async def get_code_details(code: str):
    """
    Получает детальную информацию о коде (исключая policy документы).
    Включает документы из wildcard паттернов (E11.% покрывает E11.9).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get wildcard patterns for this code
    wildcard_patterns = get_wildcard_patterns_for_code(code)
    pattern_placeholders = ','.join('?' * len(wildcard_patterns)) if wildcard_patterns else "''"

    # Search for exact match + wildcard patterns
    query = f"""
        SELECT DISTINCT
            dc.code_pattern,
            dc.code_type,
            dc.description,
            dc.document_id,
            d.filename,
            d.file_hash
        FROM document_codes dc
        JOIN documents d ON dc.document_id = d.file_hash
        WHERE (dc.code_pattern = ? OR dc.code_pattern IN ({pattern_placeholders}))
          AND (d.doc_type IS NULL OR d.doc_type != 'policy')
        ORDER BY d.filename
    """

    cursor.execute(query, [code] + wildcard_patterns)

    rows = cursor.fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Code '{code}' not found")

    # Group by document and load page info from content.json
    documents = {}
    contexts = []
    code_type = rows[0][1]

    for row in rows:
        matched_pattern = row[0]  # The pattern that matched (could be exact or wildcard)
        file_hash = row[5]
        is_wildcard = matched_pattern != code.upper() and '%' in matched_pattern

        if file_hash not in documents:
            documents[file_hash] = {
                'id': row[3],
                'filename': row[4],
                'file_hash': file_hash,
                'doc_id': file_hash[:8] if file_hash else None,
                'pages': [],
                'via_wildcard': matched_pattern if is_wildcard else None
            }

            # Load content.json to get pages with this code or matching wildcard
            content_path = os.path.join(DOCUMENTS_DIR, file_hash, 'content.json')
            if os.path.exists(content_path):
                with open(content_path, 'r', encoding='utf-8') as f:
                    doc_content = json.load(f)

                for page_data in doc_content.get('pages', []):
                    for code_info in page_data.get('codes', []):
                        page_code = code_info.get('code', '').upper()
                        # Match exact code or the wildcard pattern from this document
                        if page_code == code.upper() or page_code == matched_pattern:
                            documents[file_hash]['pages'].append(page_data['page'])
                            if code_info.get('context'):
                                contexts.append({
                                    'page': page_data['page'],
                                    'context': code_info['context'],
                                    'document': row[4],
                                    'via_wildcard': matched_pattern if is_wildcard else None
                                })
                            break

    category_info = get_code_category(code)
    rule_status = get_rule_status(code)

    return {
        'code': code,
        'type': code_type,
        'category': category_info,
        'documents': list(documents.values()),
        'contexts': contexts[:10],
        'rule_status': rule_status,
        'total_pages': sum(len(d['pages']) for d in documents.values())
    }


@router.get("/codes/{code}/sources")
async def get_code_sources(code: str, document_ids: Optional[str] = None):
    """
    Получает форматированные sources для кода (с doc_id headers).
    """
    doc_ids = document_ids.split(',') if document_ids else None
    sources_ctx = build_sources_context(code, doc_ids)

    if not sources_ctx.sources_text:
        raise HTTPException(status_code=404, detail=f"No sources found for code '{code}'")

    return {
        'code': code,
        'sources_text': sources_ctx.sources_text,
        'documents': [
            {
                'doc_id': doc.doc_id,
                'filename': doc.filename,
                'pages': doc.pages
            }
            for doc in sources_ctx.source_documents
        ],
        'total_pages': sources_ctx.total_pages
    }


@router.get("/codes/{code}/guideline")
async def get_code_guideline(code: str, document_ids: Optional[str] = None):
    """
    Получает текст гайдлайна для кода.
    DEPRECATED: Use /codes/{code}/sources instead.
    """
    doc_ids = document_ids.split(',') if document_ids else None
    sources_ctx = build_sources_context(code, doc_ids)

    if not sources_ctx.sources_text:
        raise HTTPException(status_code=404, detail=f"No guideline text found for code '{code}'")

    return {
        'code': code,
        'guideline': sources_ctx.sources_text,
        'length': len(sources_ctx.sources_text),
        'documents': [doc.doc_id for doc in sources_ctx.source_documents]
    }


@router.get("/codes/{code}/rule")
async def get_code_rule(code: str, cascade: bool = True):
    """
    Получает правило для кода.

    Args:
        code: Код (E11.9, 47535)
        cascade: Если True, ищет каскадно (E11.9 → E11.% → E%)

    Returns:
        {
            'code': str,              # запрошенный код
            'matched_pattern': str,   # паттерн который сработал
            'is_inherited': bool,     # True если правило от родителя
            'rule': dict              # содержимое правила
        }
    """
    result = find_rule_for_code(code)

    if not result:
        # If cascade didn't find anything, try exact match only
        if cascade:
            raise HTTPException(
                status_code=404,
                detail=f"No rule found for code '{code}' (checked cascade: {get_rule_cascade(code)})"
            )
        raise HTTPException(status_code=404, detail=f"No rule found for code '{code}'")

    return {
        'code': code,
        'matched_pattern': result['pattern'],
        'is_inherited': result['is_inherited'],
        'rule': result['rule_data']
    }


@router.get("/hierarchy")
async def get_codes_hierarchy():
    """
    Получает иерархическое представление всех кодов.

    Группирует коды по prefix (E11, F32, etc.) и показывает:
    - Wildcard patterns (E11.%)
    - Range patterns (47531:47541)
    - Specific codes с inherited документами

    Returns:
        {
            'groups': [
                {
                    'prefix': 'E11',
                    'name': 'Type 2 Diabetes',
                    'total_docs': 8,
                    'patterns': [...],    # wildcards/ranges
                    'codes': [...]        # specific codes
                }
            ]
        }
    """
    all_codes = get_all_codes_from_db()

    # Group by prefix
    groups = {}

    for code_info in all_codes:
        code = code_info['code']

        # Determine prefix
        if '.' in code:
            prefix = code.split('.')[0]
        elif ':' in code:
            # Range - use start of range
            prefix = code.split(':')[0][:3] if len(code.split(':')[0]) >= 3 else code.split(':')[0]
        else:
            prefix = code[:3] if len(code) >= 3 else code

        if prefix not in groups:
            groups[prefix] = {
                'prefix': prefix,
                'patterns': [],
                'codes': [],
                'doc_ids': set()
            }

        # Add to appropriate list
        if is_wildcard_code(code) or is_range_code(code):
            groups[prefix]['patterns'].append(code_info)
        else:
            groups[prefix]['codes'].append(code_info)

        # Collect unique docs
        for doc in code_info.get('documents', []):
            groups[prefix]['doc_ids'].add(doc['id'])

    # Format output
    result = []
    for prefix, data in sorted(groups.items()):
        # Get category name from first code
        first_code = data['codes'][0] if data['codes'] else data['patterns'][0] if data['patterns'] else None
        category_info = get_code_category(first_code['code']) if first_code else {}

        result.append({
            'prefix': prefix,
            'name': category_info.get('name', prefix),
            'color': category_info.get('color', '#6B7280'),
            'total_docs': len(data['doc_ids']),
            'patterns': sorted(data['patterns'], key=lambda x: x['code']),
            'codes': sorted(data['codes'], key=lambda x: x['code']),
            'total_patterns': len(data['patterns']),
            'total_specific': len(data['codes'])
        })

    return {
        'groups': result,
        'total_groups': len(result),
        'total_patterns': sum(g['total_patterns'] for g in result),
        'total_codes': sum(g['total_specific'] for g in result)
    }


@router.get("/codes/{code}/generation-log")
async def get_generation_log(code: str, version: Optional[int] = None):
    """
    Получает лог генерации для кода.
    """
    code_dir = code.replace(".", "_").replace("/", "_")
    versioned_path = os.path.join(RULES_DIR, code_dir)

    if not os.path.exists(versioned_path):
        raise HTTPException(status_code=404, detail=f"No generation log found for code '{code}'")

    versions = [d for d in os.listdir(versioned_path) if d.startswith('v')]
    if not versions:
        raise HTTPException(status_code=404, detail=f"No versions found for code '{code}'")

    if version:
        target_version = f"v{version}"
    else:
        target_version = sorted(versions, key=lambda x: int(x[1:]))[-1]

    log_path = os.path.join(versioned_path, target_version, "generation_log.json")

    if not os.path.exists(log_path):
        raise HTTPException(status_code=404, detail=f"Generation log not found for {code} {target_version}")

    with open(log_path, 'r') as f:
        return json.load(f)


# ============================================================
# GENERATION ENDPOINT
# ============================================================

@router.post("/generate/{code}")
async def generate_rule_endpoint(code: str, request: GenerateRuleRequest):
    """
    Генерирует правило для кода с SSE стримингом прогресса.

    Pipeline: Draft → Validation (Mentor + RedTeam) → Arbitration → Final

    SSE Event Format:
    {
        "step": "draft|mentor|redteam|arbitration|finalization|pipeline",
        "type": "status|thought|content|verification|done|error",
        "content": "...",
        "full_text": "...",  // on done
        "duration_ms": 15000  // on done
    }
    """
    # Check sources exist
    sources_ctx = build_sources_context(code, request.document_ids)

    if not sources_ctx.sources_text:
        raise HTTPException(
            status_code=400,
            detail=f"No source documents found for code '{code}'. Upload relevant documents first."
        )

    async def event_stream():
        generator = RuleGenerator(thinking_budget=request.thinking_budget)

        async for event in generator.stream_pipeline(
            code=code,
            document_ids=request.document_ids,
            code_type=request.code_type,
            parallel_validators=request.parallel_validators
        ):
            yield f"data: {event}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.delete("/codes/{code}/rule")
async def delete_rule(code: str):
    """
    Удаляет правило для кода.
    """
    deleted = False

    # Delete versioned structure
    code_dir = code.replace(".", "_").replace("/", "_")
    versioned_path = os.path.join(RULES_DIR, code_dir)

    if os.path.exists(versioned_path):
        import shutil
        shutil.rmtree(versioned_path)
        deleted = True

    # Delete flat structure
    rule_path = os.path.join(RULES_DIR, f"{code.replace('.', '_')}.json")

    if os.path.exists(rule_path):
        os.remove(rule_path)
        deleted = True

    if not deleted:
        raise HTTPException(status_code=404, detail=f"No rule found for code '{code}'")

    return {'status': 'deleted', 'code': code}


@router.delete("/mock")
async def clear_mock_rules():
    """
    Удаляет все mock-правила (is_mock=True).
    Используется для перезапуска генерации.
    """
    deleted = []
    kept = []

    if not os.path.exists(RULES_DIR):
        return {'deleted': 0, 'kept': 0, 'codes': []}

    for filename in os.listdir(RULES_DIR):
        if not filename.endswith('.json'):
            continue

        rule_path = os.path.join(RULES_DIR, filename)
        try:
            with open(rule_path, 'r') as f:
                rule = json.load(f)

            if rule.get('is_mock', False):
                os.remove(rule_path)
                deleted.append(rule.get('code', filename))
            else:
                kept.append(rule.get('code', filename))
        except Exception as e:
            print(f"Error processing {filename}: {e}")

    return {
        'deleted': len(deleted),
        'kept': len(kept),
        'deleted_codes': deleted
    }


@router.get("/stats")
async def get_rules_stats():
    """
    Статистика по правилам.
    """
    total = 0
    mock_count = 0
    real_count = 0

    if os.path.exists(RULES_DIR):
        for filename in os.listdir(RULES_DIR):
            if not filename.endswith('.json'):
                continue

            total += 1
            rule_path = os.path.join(RULES_DIR, filename)
            try:
                with open(rule_path, 'r') as f:
                    rule = json.load(f)
                if rule.get('is_mock', False):
                    mock_count += 1
                else:
                    real_count += 1
            except:
                pass

    return {
        'total': total,
        'mock': mock_count,
        'real': real_count
    }