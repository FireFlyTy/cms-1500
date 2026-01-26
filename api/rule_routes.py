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
    CMSRuleGenerator,
    HierarchyRuleGenerator,
)

router = APIRouter(prefix="/api/rules", tags=["rules"])

# Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RULES_DIR = os.path.join(BASE_DIR, "data", "processed", "rules")
DOCUMENTS_DIR = os.path.join(BASE_DIR, "data", "processed", "documents")

os.makedirs(RULES_DIR, exist_ok=True)


# ============================================================
# CACHES (for fast lookups)
# ============================================================

_rules_cache = {}
_rules_cache_time = 0
_CACHE_TTL = 30  # seconds

# Codes cache
_codes_cache = []
_codes_cache_time = 0


def _scan_rules_directory() -> dict:
    """Scan RULES_DIR once and build cache of all existing rules."""
    cache = {}

    if not os.path.exists(RULES_DIR):
        return cache

    for code_dir in os.listdir(RULES_DIR):
        code_path = os.path.join(RULES_DIR, code_dir)
        if not os.path.isdir(code_path):
            continue

        # Check for guideline rule versions (v1, v2, etc.)
        guideline_versions = [d for d in os.listdir(code_path)
                             if d.startswith('v') and d[1:].isdigit()]

        # Check for CMS rule versions
        cms_path = os.path.join(code_path, "cms")
        cms_versions = []
        if os.path.exists(cms_path) and os.path.isdir(cms_path):
            cms_versions = [d for d in os.listdir(cms_path)
                          if d.startswith('v') and d[1:].isdigit()]

        if guideline_versions or cms_versions:
            # Convert dir name back to code pattern
            # E11_9 → E11.9, E11_% → E11.%, E00_E89 → E00:E89
            pattern = code_dir

            # Detect range pattern (E00_E89 where both parts are code-like)
            if '_' in pattern and '%' not in pattern:
                parts = pattern.split('_')
                if len(parts) == 2:
                    # Check if both parts look like ICD-10 chapter codes (letter + digits)
                    import re
                    if (re.match(r'^[A-Z]\d+$', parts[0]) and
                        re.match(r'^[A-Z]\d+$', parts[1])):
                        pattern = f"{parts[0]}:{parts[1]}"

            # For non-range patterns, convert _ back to .
            if ':' not in pattern:
                # Trailing _ means wildcard % (E10_A_ → E10.A%)
                if pattern.endswith('_'):
                    pattern = pattern[:-1] + '%'
                pattern = pattern.replace("_", ".")
                # Fix wildcards: E11.% not E11..
                if pattern.endswith('.%'):
                    pattern = pattern[:-2] + '%'
                elif '.%' in pattern:
                    pattern = pattern.replace('.%', '%')

            # Read code_type from rule file
            code_type = None
            # Try CMS rule first (has code_type)
            if cms_versions:
                latest_cms = sorted(cms_versions, key=lambda x: int(x[1:]))[-1]
                cms_rule_path = os.path.join(cms_path, latest_cms, "cms_rule.json")
                if os.path.exists(cms_rule_path):
                    try:
                        with open(cms_rule_path, 'r') as f:
                            rule_data = json.load(f)
                            code_type = rule_data.get('code_type')
                    except:
                        pass
            # Try guideline rule if no code_type yet
            if not code_type and guideline_versions:
                latest_gl = sorted(guideline_versions, key=lambda x: int(x[1:]))[-1]
                gl_rule_path = os.path.join(code_path, latest_gl, "rule.json")
                if os.path.exists(gl_rule_path):
                    try:
                        with open(gl_rule_path, 'r') as f:
                            rule_data = json.load(f)
                            code_type = rule_data.get('code_type')
                    except:
                        pass

            cache[pattern.upper()] = {
                'guideline_versions': sorted(guideline_versions, key=lambda x: int(x[1:])) if guideline_versions else [],
                'cms_versions': sorted(cms_versions, key=lambda x: int(x[1:])) if cms_versions else [],
                'path': code_path,
                'code_type': code_type
            }

    return cache


def _get_rules_cache() -> dict:
    """Get rules cache, refreshing if stale."""
    global _rules_cache, _rules_cache_time

    import time
    now = time.time()

    if now - _rules_cache_time > _CACHE_TTL:
        _rules_cache = _scan_rules_directory()
        _rules_cache_time = now

    return _rules_cache


def invalidate_rules_cache():
    """Call this after creating/deleting rules."""
    global _rules_cache_time, _codes_cache_time
    _rules_cache_time = 0
    _codes_cache_time = 0


# ============================================================
# MODELS
# ============================================================

class GenerateRuleRequest(BaseModel):
    code: str = ""
    code_type: str = "ICD-10"
    document_ids: Optional[List[str]] = None  # If None, use all relevant docs
    parallel_validators: bool = True  # Run Mentor || RedTeam in parallel
    thinking_budget: int = 10000
    model: Optional[str] = None  # "gemini" or "gpt-4.1" (env var RULE_GENERATOR_MODEL if not set)
    force_regenerate: bool = False  # Regenerate even if rule exists
    json_validators: bool = False  # JSON output for validators (faster) - env var RULE_GENERATOR_JSON_VALIDATORS


class BatchGenerateRequest(BaseModel):
    codes: List[str]
    code_type: str = "ICD-10"
    model: Optional[str] = None  # "gemini" or "gpt-4.1"
    force_regenerate: bool = False


class GenerateCMSRuleRequest(BaseModel):
    code: str = ""
    code_type: Optional[str] = None  # Auto-detect if not provided
    thinking_budget: int = 8000
    model: Optional[str] = None  # "gemini" or "gpt-4.1"
    force_regenerate: bool = False  # Regenerate even if rule exists


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
                # Pure numeric codes (CPT)
                if code.isdigit() and start.isdigit() and end.isdigit():
                    return int(start) <= int(code) <= int(end)

                # Alphanumeric - extract common prefix
                common = ""
                for c1, c2, c3 in zip(code, start, end):
                    if c1 == c2 == c3 and c1.isalpha():
                        common += c1
                    else:
                        break

                # Must have same prefix to match (e.g., E11.0:E11.9 only matches E11.x)
                if common:
                    c_suf = code[len(common):]
                    s_suf = start[len(common):]
                    e_suf = end[len(common):]
                    if all(x.replace('.','').isdigit() for x in [c_suf, s_suf, e_suf]):
                        c_num = float(c_suf) if '.' in c_suf else int(c_suf)
                        s_num = float(s_suf) if '.' in s_suf else int(s_suf)
                        e_num = float(e_suf) if '.' in e_suf else int(e_suf)
                        return s_num <= c_num <= e_num

                # No common prefix - check if codes have same structure
                # (same first character type: letter vs digit)
                if code[0].isalpha() != start[0].isalpha():
                    return False  # Different code types (ICD vs CPT)
                if code[0].isalpha() != end[0].isalpha():
                    return False

                # Check code structure matches (ICD-10 has dots, HCPCS doesn't)
                code_has_dot = '.' in code
                range_has_dot = '.' in start or '.' in end
                if code_has_dot != range_has_dot:
                    return False  # Different code structures (ICD-10 vs HCPCS)

                # For ICD-10 ranges without dots in pattern (E00:E89),
                # only match ICD-10 codes (which have dots)
                if not range_has_dot and code[0].isalpha() and len(start) <= 3 and len(end) <= 3:
                    # This is likely an ICD-10 chapter range like E00:E89
                    # Should only match codes that start with this prefix
                    if not (code.startswith(start[0]) and len(code) <= 7):
                        return False
                    # ICD-10 codes typically have format like E11.9, E00.0
                    # HCPCS E-codes are like E0781, E2101 (no dots, 5 chars)
                    if len(code) == 5 and code[1:].isdigit():
                        return False  # This is HCPCS, not ICD-10

                return start <= code <= end
            except:
                return False  # Don't match on errors

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
    Получает все коды из таблицы code_hierarchy.
    Фильтрует по 5 демо-категориям болезней.
    Проверяет статус правил из rules_hierarchy.
    Наследует документы от метакатегорий.
    Результат кэшируется на 30 секунд.
    """
    global _codes_cache, _codes_cache_time

    import time
    now = time.time()

    # Return cached result if still valid
    if _codes_cache and (now - _codes_cache_time) < _CACHE_TTL:
        return _codes_cache

    from src.utils.code_categories import get_code_category, is_ignored_code

    conn = get_db_connection()
    cursor = conn.cursor()

    # Get codes from code_hierarchy (level 1+ = category, subcategory, code)
    cursor.execute("""
        SELECT DISTINCT
            ch.pattern,
            ch.code_type,
            ch.level,
            ch.parent_pattern,
            ch.description
        FROM code_hierarchy ch
        WHERE ch.level >= 1
        ORDER BY ch.pattern
    """)
    hierarchy_rows = cursor.fetchall()

    # Get rules from rules_hierarchy
    cursor.execute("""
        SELECT pattern, code_type, rule_type, rule_id, status
        FROM rules_hierarchy
    """)
    rules_rows = cursor.fetchall()

    # Get documents linked to meta-categories (E, F, I, etc.)
    cursor.execute("""
        SELECT dc.code_pattern, dc.code_type, dc.document_id, d.filename
        FROM document_codes dc
        JOIN documents d ON dc.document_id = d.file_hash
        WHERE d.parsed_at IS NOT NULL
          AND d.doc_type IN ('clinical_guideline', 'codebook')
          AND LENGTH(dc.code_pattern) <= 2
    """)
    doc_rows = cursor.fetchall()

    conn.close()

    # Get rules cache from filesystem (for CMS status)
    rules_cache = _get_rules_cache()

    # Build document lookup: meta_category:code_type -> [{id, filename}]
    docs_by_meta = {}
    for row in doc_rows:
        meta_cat, code_type, doc_id, filename = row
        key = f"{meta_cat.upper()}:{code_type}"
        if key not in docs_by_meta:
            docs_by_meta[key] = []
        docs_by_meta[key].append({'id': doc_id, 'filename': filename})

    # Build rules lookup: pattern:code_type -> {guideline: rule_id, cms1500: rule_id}
    rules_lookup = {}
    for row in rules_rows:
        pattern, code_type, rule_type, rule_id, status = row
        key = f"{pattern}:{code_type}"
        if key not in rules_lookup:
            rules_lookup[key] = {}
        rules_lookup[key][rule_type] = {'rule_id': rule_id, 'status': status}

    # Build codes list, filtering by demo categories
    codes_list = []
    seen_codes = set()

    for row in hierarchy_rows:
        pattern, code_type, level, parent_pattern, description = row

        # Skip if already seen
        if pattern in seen_codes:
            continue

        # Skip ignored codes (modifiers, too short)
        if is_ignored_code(pattern, code_type):
            continue

        # Check if code belongs to a demo category
        cat_info = get_code_category(pattern)
        if not cat_info['category']:
            continue

        seen_codes.add(pattern)

        # Check rule status from DB
        key = f"{pattern}:{code_type}"
        rule_info = rules_lookup.get(key, {})

        # Also check filesystem cache for rules
        file_cache = rules_cache.get(pattern.upper(), {})
        guideline_versions = file_cache.get('guideline_versions', [])
        cms_versions = file_cache.get('cms_versions', [])
        has_guideline_file = bool(guideline_versions)
        has_cms_file = bool(cms_versions)

        # Get latest versions
        guideline_version = int(guideline_versions[-1][1:]) if guideline_versions else None
        cms_version = int(cms_versions[-1][1:]) if cms_versions else None

        # Get documents from meta-category (first letter of code)
        meta_category = pattern[0].upper() if pattern else ""
        meta_key = f"{meta_category}:{code_type}"
        inherited_docs = docs_by_meta.get(meta_key, [])

        codes_list.append({
            'code': pattern,
            'type': code_type,
            'level': level,
            'parent': parent_pattern,
            'description': description or '',
            'documents': [],
            'inherited_documents': inherited_docs,
            'total_docs': len(inherited_docs),
            'has_guideline': 'guideline' in rule_info or has_guideline_file,
            'has_cms1500': 'cms1500' in rule_info or has_cms_file,
            'guideline_rule_id': rule_info.get('guideline', {}).get('rule_id'),
            'cms1500_rule_id': rule_info.get('cms1500', {}).get('rule_id'),
            'guideline_version': guideline_version,
            'cms_version': cms_version,
        })

    # Cache the result
    _codes_cache = codes_list
    _codes_cache_time = now

    return codes_list


def get_rule_status(code: str, cascade: bool = True, rule_type: str = "guideline") -> Dict:
    """
    Проверяет статус правила для кода из rules_hierarchy.

    Args:
        code: Код для проверки
        cascade: Если True, ищет правило каскадно (E11.9 → E11 → E)
        rule_type: "guideline" или "cms1500"

    Returns:
        {
            'has_rule': bool,
            'is_inherited': bool,      # True если правило от родительского паттерна
            'matched_pattern': str,    # какой паттерн сработал
            'rule_id': int,
            'path': str
        }
    """
    from src.generators.hierarchy_rule_generator import get_hierarchy_patterns

    # Determine search patterns
    if cascade:
        patterns_to_check = get_hierarchy_patterns(code, "ICD-10")
    else:
        patterns_to_check = [code.upper()]

    conn = get_db_connection()
    cursor = conn.cursor()

    for pattern in patterns_to_check:
        cursor.execute("""
            SELECT rule_id, status FROM rules_hierarchy
            WHERE pattern = ? AND rule_type = ?
        """, (pattern, rule_type))
        row = cursor.fetchone()

        if row:
            rule_id, status = row
            # Get path from rules table
            cursor.execute("SELECT rule_path FROM rules WHERE id = ?", (rule_id,))
            path_row = cursor.fetchone()
            path = path_row[0] if path_row else None

            conn.close()
            return {
                'has_rule': True,
                'is_inherited': pattern.upper() != code.upper(),
                'matched_pattern': pattern,
                'rule_id': rule_id,
                'status': status,
                'path': path
            }

    conn.close()
    return {
        'has_rule': False,
        'is_inherited': False,
        'matched_pattern': None,
        'rule_id': None,
        'path': None
    }


def _check_rule_exists(pattern: str) -> Dict:
    """Проверяет существование правила для конкретного паттерна в БД."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT rule_id, status FROM rules_hierarchy
        WHERE pattern = ? AND rule_type = 'guideline'
    """, (pattern.upper(),))
    row = cursor.fetchone()

    if row:
        rule_id, status = row
        cursor.execute("SELECT rule_path FROM rules WHERE id = ?", (rule_id,))
        path_row = cursor.fetchone()
        conn.close()

        return {
            'has_rule': True,
            'rule_id': rule_id,
            'status': status,
            'path': path_row[0] if path_row else None
        }

    conn.close()
    return {'has_rule': False}


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
        # Path from DB is directory, need to add rule.json
        rule_path = status['path']
        if rule_path and os.path.isdir(rule_path):
            rule_path = os.path.join(rule_path, 'rule.json')

        with open(rule_path, 'r') as f:
            rule_data = json.load(f)

        return {
            'pattern': status['matched_pattern'],
            'is_inherited': status['is_inherited'],
            'rule_data': rule_data,
            'path': rule_path
        }
    except Exception as e:
        print(f"Error loading rule for {code}: {e}")
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
async def get_categories(rule_type: Optional[str] = "guideline"):
    """
    Получает список категорий с количеством кодов и статусом покрытия.

    Args:
        rule_type: "guideline" (default) or "cms" - which rule type to count
    """
    all_codes = get_all_codes_from_db()
    grouped = group_codes_by_category(all_codes)

    categories = []
    for category_name, codes in grouped.items():
        # Count rules - use pre-fetched data from get_all_codes_from_db()
        if rule_type == "cms":
            codes_with_rules = sum(1 for c in codes if c.get('has_cms1500'))
        else:
            codes_with_rules = sum(1 for c in codes if c.get('has_guideline'))

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


@router.get("/categories-by-type")
async def get_categories_by_code_type(rule_type: Optional[str] = "cms"):
    """
    Получает категории сгруппированные по code_type (ICD-10, CPT, HCPCS).
    Использует meta_category из code_hierarchy.

    Returns:
        {
            "ICD-10": {
                "total_codes": 100,
                "codes_with_rules": 20,
                "categories": [
                    {"name": "E", "description": "Endocrine...", "total": 30, "with_rules": 10},
                    ...
                ]
            },
            ...
        }
    """
    from src.parsers.document_parser import load_meta_categories_from_json

    conn = get_db_connection()
    cursor = conn.cursor()

    # Get all codes with meta_category from code_hierarchy
    cursor.execute("""
        SELECT
            ch.code_type,
            ch.meta_category,
            ch.pattern,
            ch.level,
            ch.description
        FROM code_hierarchy ch
        WHERE ch.level >= 2
        ORDER BY ch.code_type, ch.meta_category, ch.pattern
    """)
    hierarchy_rows = cursor.fetchall()

    # Get rules status (status can be 'ready' or 'active')
    cursor.execute("""
        SELECT pattern, code_type, rule_type, status
        FROM rules_hierarchy
        WHERE status IN ('active', 'ready')
    """)
    rules_rows = cursor.fetchall()

    # Get NCCI codes (for CPT/HCPCS sources)
    cursor.execute("SELECT DISTINCT code FROM ncci_mue_pra WHERE mue_value IS NOT NULL")
    ncci_mue = set(row[0] for row in cursor.fetchall())
    cursor.execute("SELECT DISTINCT column1 FROM ncci_ptp UNION SELECT DISTINCT column2 FROM ncci_ptp")
    ncci_ptp = set(row[0] for row in cursor.fetchall())
    ncci_codes = ncci_mue | ncci_ptp

    conn.close()

    # Get rules from filesystem cache too
    rules_cache = _get_rules_cache()

    # Build rules lookup from DB
    # Note: rule_type in DB is 'cms1500' or 'guideline', normalize for API
    rules_lookup = {}
    for pattern, code_type, r_type, status in rules_rows:
        key = f"{pattern}:{code_type}"
        if key not in rules_lookup:
            rules_lookup[key] = {}
        # Normalize: cms1500 -> cms
        normalized_type = 'cms' if r_type == 'cms1500' else r_type
        rules_lookup[key][normalized_type] = True

    # Add rules from filesystem - use code_type from rule file
    for pattern, file_info in rules_cache.items():
        fs_code_type = file_info.get('code_type')
        if fs_code_type:
            key = f"{pattern}:{fs_code_type}"
            if key not in rules_lookup:
                rules_lookup[key] = {}
            if file_info.get('guideline_versions'):
                rules_lookup[key]['guideline'] = True
            if file_info.get('cms_versions'):
                rules_lookup[key]['cms'] = True

    # Load meta_categories for descriptions
    meta_cats = load_meta_categories_from_json()

    # Group by code_type -> meta_category
    result = {}

    for code_type, meta_cat, pattern, level, description in hierarchy_rows:
        if not meta_cat:
            continue

        if code_type not in result:
            result[code_type] = {
                "total_codes": 0,
                "codes_with_rules": 0,
                "codes_with_sources": 0,
                "codes_ready": 0,
                "categories": {}
            }

        if meta_cat not in result[code_type]["categories"]:
            # Get description from meta_categories.json
            mc_key = code_type if code_type != "ICD-10" else "ICD-10"
            cat_info = meta_cats.get(mc_key, {}).get("categories", {}).get(meta_cat, {})
            cat_description = cat_info.get("name", description or meta_cat)

            result[code_type]["categories"][meta_cat] = {
                "name": meta_cat,
                "description": cat_description,
                "total": 0,
                "with_rules": 0,
                "with_sources": 0,
                "ready": 0
            }

        # Count codes (level 2+ are actual codes, level 1 is meta-category)
        result[code_type]["total_codes"] += 1
        result[code_type]["categories"][meta_cat]["total"] += 1

        # Check if has rule and sources
        key = f"{pattern}:{code_type}"
        rule_info = rules_lookup.get(key, {})
        has_rule = rule_info.get(rule_type if rule_type else "cms", False)
        has_guideline = rule_info.get('guideline', False)
        has_ncci = pattern in ncci_codes if code_type in ('CPT', 'HCPCS') else False
        has_sources = has_guideline or has_ncci

        if has_rule:
            result[code_type]["codes_with_rules"] += 1
            result[code_type]["categories"][meta_cat]["with_rules"] += 1

        if has_sources:
            result[code_type]["codes_with_sources"] += 1
            result[code_type]["categories"][meta_cat]["with_sources"] += 1
            if not has_rule:
                result[code_type]["codes_ready"] += 1
                result[code_type]["categories"][meta_cat]["ready"] += 1

    # Convert categories dict to sorted list
    for code_type in result:
        cats_dict = result[code_type]["categories"]
        cats_list = sorted(cats_dict.values(), key=lambda x: x["name"])
        result[code_type]["categories"] = cats_list

    return result


@router.get("/codes-by-meta/{code_type}/{meta_category}")
async def get_codes_by_meta_category(code_type: str, meta_category: str):
    """
    Получает коды для конкретной meta_category (E, F, I, J, 9, etc.)

    Args:
        code_type: ICD-10, CPT, HCPCS
        meta_category: E, F, I (for ICD-10) or J, A (for HCPCS) etc.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get codes from code_hierarchy
    cursor.execute("""
        SELECT
            ch.pattern,
            ch.code_type,
            ch.level,
            ch.description,
            ch.meta_category
        FROM code_hierarchy ch
        WHERE ch.code_type = ? AND ch.meta_category = ? AND ch.level >= 2
        ORDER BY ch.pattern
    """, (code_type, meta_category))
    hierarchy_rows = cursor.fetchall()

    # Get rules status (status can be 'ready' or 'active') with path for version
    cursor.execute("""
        SELECT rh.pattern, rh.code_type, rh.rule_type, rh.rule_id, rh.status, r.rule_path
        FROM rules_hierarchy rh
        LEFT JOIN rules r ON rh.rule_id = r.id
        WHERE rh.code_type = ? AND rh.status IN ('active', 'ready')
    """, (code_type,))
    rules_rows = cursor.fetchall()

    # Get documents linked to codes
    cursor.execute("""
        SELECT dc.code_pattern, dc.document_id, d.filename
        FROM document_codes dc
        JOIN documents d ON dc.document_id = d.file_hash
        WHERE dc.code_type = ? AND d.parsed_at IS NOT NULL
    """, (code_type,))
    doc_rows = cursor.fetchall()
    conn.close()

    # Get rules from filesystem cache too
    rules_cache = _get_rules_cache()

    # Build lookups from DB
    # Note: rule_type in DB is 'cms1500' or 'guideline', normalize for consistency
    rules_lookup = {}
    for pattern, c_type, r_type, rule_id, status, rule_path in rules_rows:
        if pattern not in rules_lookup:
            rules_lookup[pattern] = {}
        # Extract version from rule_path (e.g., .../v1 -> 1)
        version = None
        if rule_path:
            ver_dir = os.path.basename(rule_path)
            if ver_dir.startswith('v') and ver_dir[1:].isdigit():
                version = int(ver_dir[1:])
        # Normalize: cms1500 -> cms
        normalized_type = 'cms' if r_type == 'cms1500' else r_type
        rules_lookup[pattern][normalized_type] = {'rule_id': rule_id, 'status': status, 'version': version}

    # Add rules from filesystem - use code_type from rule file
    for fs_pattern, file_info in rules_cache.items():
        fs_code_type = file_info.get('code_type')
        if fs_code_type == code_type:
            if fs_pattern not in rules_lookup:
                rules_lookup[fs_pattern] = {}
            if file_info.get('guideline_versions'):
                if 'guideline' not in rules_lookup[fs_pattern]:
                    versions = file_info['guideline_versions']
                    rules_lookup[fs_pattern]['guideline'] = {'rule_id': None, 'version': int(versions[-1][1:]) if versions else 1}
            if file_info.get('cms_versions'):
                if 'cms' not in rules_lookup[fs_pattern]:
                    versions = file_info['cms_versions']
                    rules_lookup[fs_pattern]['cms'] = {'rule_id': None, 'version': int(versions[-1][1:]) if versions else 1}

    docs_lookup = {}
    for pattern, doc_id, filename in doc_rows:
        if pattern not in docs_lookup:
            docs_lookup[pattern] = []
        docs_lookup[pattern].append({'id': doc_id, 'filename': filename})

    # For CPT/HCPCS: get NCCI data availability
    ncci_lookup = {}
    if code_type in ('CPT', 'HCPCS'):
        conn2 = get_db_connection()
        cursor2 = conn2.cursor()
        # Get all patterns that have MUE data
        cursor2.execute("SELECT code FROM ncci_mue_pra WHERE mue_value IS NOT NULL")
        for row in cursor2.fetchall():
            ncci_lookup[row[0]] = {'has_mue': True}
        # Get all patterns that have PTP data
        cursor2.execute("SELECT DISTINCT column1 FROM ncci_ptp")
        for row in cursor2.fetchall():
            if row[0] not in ncci_lookup:
                ncci_lookup[row[0]] = {}
            ncci_lookup[row[0]]['has_ptp'] = True
        cursor2.execute("SELECT DISTINCT column2 FROM ncci_ptp")
        for row in cursor2.fetchall():
            if row[0] not in ncci_lookup:
                ncci_lookup[row[0]] = {}
            ncci_lookup[row[0]]['has_ptp'] = True
        conn2.close()

    # Build result
    diagnoses = []
    procedures = []

    for pattern, c_type, level, description, meta_cat in hierarchy_rows:
        rule_info = rules_lookup.get(pattern, {})
        docs = docs_lookup.get(pattern, [])
        ncci_info = ncci_lookup.get(pattern, {})

        # Check parent rules for inheritance indicator
        has_guideline = 'guideline' in rule_info
        has_cms = 'cms' in rule_info
        has_ncci = bool(ncci_info.get('has_mue') or ncci_info.get('has_ptp'))

        code_data = {
            'code': pattern,
            'type': c_type,
            'description': description,
            'level': level,
            'documents': docs,
            'has_ncci': has_ncci,
            'rule_status': {
                'has_rule': has_guideline,
                'has_cms1500': has_cms,
                'guideline_rule_id': rule_info.get('guideline', {}).get('rule_id'),
                'cms1500_rule_id': rule_info.get('cms', {}).get('rule_id'),
                'guideline_version': rule_info.get('guideline', {}).get('version'),
                'cms_version': rule_info.get('cms', {}).get('version'),
            }
        }

        if c_type == 'ICD-10':
            diagnoses.append(code_data)
        else:
            procedures.append(code_data)

    return {
        'code_type': code_type,
        'meta_category': meta_category,
        'diagnoses': diagnoses,
        'procedures': procedures,
        'total': len(diagnoses) + len(procedures)
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
        # Use pre-fetched rule status from get_all_codes_from_db()
        guideline_ver = code_info.get('guideline_version')
        rule_status = {
            'has_rule': code_info.get('has_guideline', False),
            'has_cms1500': code_info.get('has_cms1500', False),
            'rule_id': code_info.get('guideline_rule_id'),
            'cms1500_rule_id': code_info.get('cms1500_rule_id'),
            'version': guideline_ver,  # backwards compatibility
            'guideline_version': guideline_ver,
            'cms_version': code_info.get('cms_version'),
        }
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
    code_dir = code.replace(".", "_").replace("/", "_").replace(":", "_").replace("-", "_")
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
    Использует HierarchyRuleGenerator для каскадной генерации.

    Pipeline: Draft → Validation (Mentor + RedTeam) → Arbitration → Final
    Cascade: E → E11 → E11.9 (top-down with inheritance)

    SSE Event Format:
    {
        "step": "guideline|draft|mentor|redteam|arbitration|finalization|pipeline",
        "type": "plan|generating|status|thought|content|verification|done|complete|error",
        "content": "...",
        "patterns_to_generate": [...],  // on plan
        "existing_patterns": [...],     // on plan
        "pattern": "E11",               // on generating
        "parent_rule": "E",             // on generating (if inheriting)
        "full_text": "...",             // on done
        "duration_ms": 15000            // on done
    }
    """
    # Check sources exist for the target code
    sources_ctx = build_sources_context(code, request.document_ids, code_type=request.code_type)

    if not sources_ctx.sources_text:
        raise HTTPException(
            status_code=400,
            detail=f"No source documents found for code '{code}'. Upload relevant documents first."
        )

    async def event_stream():
        generator = HierarchyRuleGenerator(
            thinking_budget=request.thinking_budget,
            model=request.model,
            json_validators=request.json_validators
        )

        async for event in generator.generate_guideline(
            code=code,
            code_type=request.code_type,
            document_ids=request.document_ids,
            force_regenerate=request.force_regenerate
        ):
            yield f"data: {event}\n\n"

        # Invalidate cache after generation
        invalidate_rules_cache()

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
    code_dir = code.replace(".", "_").replace("/", "_").replace(":", "_").replace("-", "_")
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

    invalidate_rules_cache()
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


# ============================================================
# CMS-1500 CLAIM RULES ENDPOINTS
# ============================================================

def get_cms_rule_status(code: str) -> Dict:
    """
    Проверяет статус CMS правила для кода (использует кэш).

    Returns:
        {
            'has_rule': bool,
            'version': int,
            'created_at': str,
            'path': str,
            'sources': {...}
        }
    """
    cache = _get_rules_cache()
    code_upper = code.upper()

    # Check cache first (fast path)
    if code_upper in cache:
        cached = cache[code_upper]
        cms_versions = cached.get('cms_versions', [])
        if cms_versions:
            latest = cms_versions[-1]
            return {
                'has_rule': True,
                'version': int(latest[1:]),
                'created_at': None,  # Don't load JSON just to check
                'path': os.path.join(cached['path'], 'cms', latest, "cms_rule.json"),
                'sources': {},
                'stats': {}
            }

    # Cache miss - check filesystem
    code_dir = code.replace(".", "_").replace("/", "_").replace(":", "_").replace("-", "_")
    cms_path = os.path.join(RULES_DIR, code_dir, "cms")

    if not os.path.exists(cms_path):
        return {'has_rule': False}

    versions = [d for d in os.listdir(cms_path) if d.startswith('v') and d[1:].isdigit()]
    if not versions:
        return {'has_rule': False}

    latest = sorted(versions, key=lambda x: int(x[1:]))[-1]
    rule_path = os.path.join(cms_path, latest, "cms_rule.json")

    if not os.path.exists(rule_path):
        return {'has_rule': False}

    return {
        'has_rule': True,
        'version': int(latest[1:]),
        'created_at': None,
        'path': rule_path,
        'sources': {},
        'stats': {}
    }


@router.get("/codes/{code}/cms-status")
async def get_code_cms_status(code: str):
    """
    Получает статус CMS правила для кода.
    """
    status = get_cms_rule_status(code)
    guideline_status = get_rule_status(code)

    return {
        'code': code,
        'cms_rule': status,
        'guideline_rule': {
            'has_rule': guideline_status['has_rule'],
            'version': guideline_status.get('version'),
            'is_inherited': guideline_status.get('is_inherited', False)
        }
    }


@router.get("/codes/{code}/cms-rule")
async def get_code_cms_rule(code: str, version: Optional[int] = None):
    """
    Получает CMS правило для кода.

    Args:
        code: Код (E11.9, 99213, G0101)
        version: Версия правила (по умолчанию - последняя)
    """
    code_dir = code.replace(".", "_").replace("/", "_").replace(":", "_").replace("-", "_")
    cms_path = os.path.join(RULES_DIR, code_dir, "cms")

    if not os.path.exists(cms_path):
        raise HTTPException(status_code=404, detail=f"No CMS rule found for code '{code}'")

    versions = [d for d in os.listdir(cms_path) if d.startswith('v') and d[1:].isdigit()]
    if not versions:
        raise HTTPException(status_code=404, detail=f"No CMS rule versions found for code '{code}'")

    if version:
        target_version = f"v{version}"
        if target_version not in versions:
            raise HTTPException(status_code=404, detail=f"Version {version} not found for code '{code}'")
    else:
        target_version = sorted(versions, key=lambda x: int(x[1:]))[-1]

    # Load JSON rule
    json_path = os.path.join(cms_path, target_version, "cms_rule.json")
    md_path = os.path.join(cms_path, target_version, "cms_rule.md")

    if not os.path.exists(json_path):
        raise HTTPException(status_code=404, detail=f"CMS rule JSON not found for {code} {target_version}")

    with open(json_path, 'r', encoding='utf-8') as f:
        rule_json = json.load(f)

    # Load markdown
    rule_md = None
    if os.path.exists(md_path):
        with open(md_path, 'r', encoding='utf-8') as f:
            rule_md = f.read()

    return {
        'code': code,
        'version': int(target_version[1:]),
        'available_versions': sorted([int(v[1:]) for v in versions]),
        'rule': rule_json,
        'markdown': rule_md
    }


@router.get("/codes/{code}/cms-generation-log")
async def get_cms_generation_log(code: str, version: Optional[int] = None):
    """
    Получает лог генерации CMS правила.
    """
    code_dir = code.replace(".", "_").replace("/", "_").replace(":", "_").replace("-", "_")
    cms_path = os.path.join(RULES_DIR, code_dir, "cms")

    if not os.path.exists(cms_path):
        raise HTTPException(status_code=404, detail=f"No CMS rule found for code '{code}'")

    versions = [d for d in os.listdir(cms_path) if d.startswith('v') and d[1:].isdigit()]
    if not versions:
        raise HTTPException(status_code=404, detail=f"No versions found for code '{code}'")

    if version:
        target_version = f"v{version}"
    else:
        target_version = sorted(versions, key=lambda x: int(x[1:]))[-1]

    log_path = os.path.join(cms_path, target_version, "generation_log.json")

    if not os.path.exists(log_path):
        raise HTTPException(status_code=404, detail=f"Generation log not found for {code} {target_version}")

    with open(log_path, 'r', encoding='utf-8') as f:
        return json.load(f)


@router.post("/generate-cms/{code}")
async def generate_cms_rule_endpoint(code: str, request: GenerateCMSRuleRequest):
    """
    Генерирует CMS-1500 claim validation правило для кода.
    Использует HierarchyRuleGenerator для каскадной генерации.

    Pipeline:
    1. Check prerequisite (guideline rule must exist)
    2. Generate cascade: E → E11 → E11.9 (top-down with inheritance)
    3. Each level: Load parent CMS rule + guideline rule + NCCI edits
    4. Transform to CMS rules (Markdown)
    5. Parse to structured JSON

    SSE Event Format:
    {
        "step": "cms1500|transform|parse|pipeline",
        "type": "plan|generating|status|thought|content|done|complete|error",
        "content": "...",
        "patterns_to_generate": [...],  // on plan
        "existing_patterns": [...],     // on plan
        "pattern": "E11",               // on generating
        "parent_rule": "E",             // on generating (if inheriting)
        "prerequisite_met": bool,       // on error (if prerequisite failed)
        "full_text": "...",             // on done
        "duration_ms": 15000            // on done
    }
    """
    async def event_stream():
        generator = HierarchyRuleGenerator(thinking_budget=request.thinking_budget, model=request.model)

        async for event in generator.generate_cms1500(
            code=code,
            code_type=request.code_type or "ICD-10",
            force_regenerate=request.force_regenerate
        ):
            yield f"data: {event}\n\n"

        # Invalidate cache after generation
        invalidate_rules_cache()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.delete("/codes/{code}/cms-rule")
async def delete_cms_rule(code: str):
    """
    Удаляет CMS правило для кода.
    """
    code_dir = code.replace(".", "_").replace("/", "_").replace(":", "_").replace("-", "_")
    cms_path = os.path.join(RULES_DIR, code_dir, "cms")

    if not os.path.exists(cms_path):
        raise HTTPException(status_code=404, detail=f"No CMS rule found for code '{code}'")

    import shutil
    shutil.rmtree(cms_path)

    invalidate_rules_cache()
    return {'status': 'deleted', 'code': code, 'type': 'cms_rule'}