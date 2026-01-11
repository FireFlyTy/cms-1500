"""
Code Hierarchy Module

Управляет иерархией медицинских кодов для наследования правил и документов.

Иерархия для ICD-10:
    E%          → Chapter level (all E codes)
    E11.%       → Category level (all E11.x)
    E11.9       → Specific code

Иерархия для CPT/HCPCS:
    4%          → Broad category
    47%         → Sub-category
    475%        → Narrower
    47531:47541 → Range
    47533       → Specific code

Пример наследования документов:
    E11.9 total_docs = own_docs(4) + E11.%(3) + E%(2) = 9

Пример каскадного поиска правил:
    E11.9 → E11.% → E% → None
"""

import re
import os
import json
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass


@dataclass
class CodeNode:
    """Узел иерархии кодов."""
    pattern: str              # E11.9, E11.%, 47531:47541
    parent_pattern: Optional[str]  # E11.%, None
    level: int                # 1=chapter, 2=category, 3=specific
    code_type: str            # ICD-10, CPT, HCPCS
    own_documents: List[str]  # doc_ids с explicit match
    inherited_documents: List[str]  # doc_ids inherited from parents
    
    @property
    def total_documents(self) -> List[str]:
        """Все документы (own + inherited), без дубликатов."""
        seen = set()
        result = []
        for doc_id in self.own_documents + self.inherited_documents:
            if doc_id not in seen:
                seen.add(doc_id)
                result.append(doc_id)
        return result
    
    @property
    def is_wildcard(self) -> bool:
        return '%' in self.pattern
    
    @property
    def is_range(self) -> bool:
        return ':' in self.pattern
    
    @property
    def is_specific(self) -> bool:
        return not self.is_wildcard and not self.is_range


def get_parent_patterns(code: str) -> List[str]:
    """
    Возвращает все родительские паттерны для кода (от ближайшего к самому общему).
    
    E11.9  → ['E11.%', 'E1%.%', 'E%']
    E11.65 → ['E11.%', 'E1%.%', 'E%']
    J1950  → ['J195%', 'J19%', 'J1%', 'J%']
    47533  → ['4753%', '475%', '47%', '4%']  # также проверяется range matching отдельно
    """
    if not code:
        return []
    
    code = code.upper()
    patterns = []
    
    # Skip if already a wildcard or range
    if '%' in code or ':' in code:
        return []
    
    # ICD-10 style codes (E11.9, F32.1, Z79.4)
    if '.' in code:
        parts = code.split('.')
        base = parts[0]  # E11
        
        # Add base.% pattern (most specific parent)
        patterns.append(f"{base}.%")
        
        # Add progressively shorter patterns
        for i in range(len(base) - 1, 0, -1):
            patterns.append(f"{base[:i]}%.%")
        
        # Add single letter pattern
        if len(base) >= 1:
            patterns.append(f"{base[0]}%")
    
    else:
        # HCPCS/CPT style codes (J1950, 99213)
        for i in range(len(code) - 1, 0, -1):
            patterns.append(f"{code[:i]}%")
    
    # Remove duplicates while preserving order
    return list(dict.fromkeys(patterns))


def get_child_pattern(parent_pattern: str) -> Optional[str]:
    """
    Возвращает один уровень вниз от паттерна.
    
    E%     → E_.% (любая буква + цифра + .)
    E1%.%  → E1_.%
    E11.%  → None (это уже самый детальный wildcard)
    """
    # This is complex - skip for now, children found via DB
    return None


def code_matches_pattern(code: str, pattern: str) -> bool:
    """
    Проверяет соответствует ли конкретный код паттерну.
    
    Поддерживает:
    - Exact: E11.9 == E11.9
    - Wildcard: E11.9 matches E11.%
    - Range: 47535 matches 47531:47541
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
        start, end = pattern.split(':')
        return _code_in_range(code, start, end)
    
    # Wildcard match
    if '%' in pattern:
        regex_pattern = '^' + pattern.replace('%', '.*').replace('_', '.') + '$'
        return bool(re.match(regex_pattern, code))
    
    return False


def _code_in_range(code: str, start: str, end: str) -> bool:
    """Проверяет входит ли код в диапазон."""
    try:
        # Try numeric comparison first (CPT codes)
        if code.isdigit() and start.isdigit() and end.isdigit():
            return int(start) <= int(code) <= int(end)
        
        # Alphanumeric comparison (ICD-10 style)
        # Extract common prefix
        common_prefix = ""
        for c1, c2, c3 in zip(code, start, end):
            if c1 == c2 == c3 and c1.isalpha():
                common_prefix += c1
            else:
                break
        
        if common_prefix:
            code_suffix = code[len(common_prefix):]
            start_suffix = start[len(common_prefix):]
            end_suffix = end[len(common_prefix):]
            
            if all(s.replace('.', '').isdigit() for s in [code_suffix, start_suffix, end_suffix]):
                code_num = float(code_suffix) if '.' in code_suffix else int(code_suffix)
                start_num = float(start_suffix) if '.' in start_suffix else int(start_suffix)
                end_num = float(end_suffix) if '.' in end_suffix else int(end_suffix)
                return start_num <= code_num <= end_num
        
        # Fallback to string comparison
        return start <= code <= end
        
    except (ValueError, TypeError):
        return start <= code <= end


def find_matching_ranges(code: str, all_ranges: List[str]) -> List[str]:
    """
    Находит все range паттерны которые покрывают данный код.
    
    find_matching_ranges("47535", ["47531:47541", "99213:99215"]) → ["47531:47541"]
    """
    matches = []
    for range_pattern in all_ranges:
        if ':' in range_pattern and code_matches_pattern(code, range_pattern):
            matches.append(range_pattern)
    return matches


# ============================================================
# RULE CASCADE LOOKUP
# ============================================================

def get_rule_cascade(code: str) -> List[str]:
    """
    Возвращает список паттернов для каскадного поиска правила.
    
    Порядок: от самого специфичного к самому общему.
    
    E11.9 → ['E11.9', 'E11.%', 'E1%.%', 'E%']
    47535 → ['47535', '4753%', '475%', '47%', '4%']
    
    Note: Range patterns добавляются отдельно через find_matching_ranges()
    """
    if not code:
        return []
    
    cascade = [code.upper()]
    cascade.extend(get_parent_patterns(code))
    
    return cascade


def find_rule_for_code(code: str, rules_dir: str, all_ranges: Optional[List[str]] = None) -> Optional[Dict]:
    """
    Находит правило для кода, используя каскадный поиск.
    
    1. Ищет exact match (E11.9)
    2. Ищет range match (47531:47541)
    3. Ищет wildcard parents (E11.%, E%)
    
    Returns:
        {
            'pattern': 'E11.%',          # какой паттерн сработал
            'rule_path': '/path/to/rule',
            'is_inherited': True,         # наследованное или exact
            'rule_data': {...}
        }
    """
    cascade = get_rule_cascade(code)
    
    # Add matching ranges to cascade (after exact, before wildcards)
    if all_ranges:
        matching_ranges = find_matching_ranges(code, all_ranges)
        if matching_ranges:
            # Insert ranges after exact match, before wildcards
            cascade = [cascade[0]] + matching_ranges + cascade[1:]
    
    for pattern in cascade:
        rule_path = _get_rule_path(pattern, rules_dir)
        if rule_path and os.path.exists(rule_path):
            try:
                with open(rule_path, 'r', encoding='utf-8') as f:
                    rule_data = json.load(f)
                
                return {
                    'pattern': pattern,
                    'rule_path': rule_path,
                    'is_inherited': pattern.upper() != code.upper(),
                    'rule_data': rule_data
                }
            except Exception:
                continue
    
    return None


def _get_rule_path(pattern: str, rules_dir: str) -> Optional[str]:
    """Возвращает путь к файлу правила для паттерна."""
    # Normalize pattern for filesystem
    safe_pattern = pattern.replace('.', '_').replace('%', 'X').replace(':', '-').replace('/', '_')
    
    # Check versioned structure first
    pattern_dir = os.path.join(rules_dir, safe_pattern)
    if os.path.isdir(pattern_dir):
        # Find latest version
        versions = []
        for f in os.listdir(pattern_dir):
            if f.startswith('v') and f.endswith('.json'):
                try:
                    v = int(f[1:-5])
                    versions.append(v)
                except ValueError:
                    pass
        if versions:
            return os.path.join(pattern_dir, f'v{max(versions)}.json')
    
    # Check flat structure
    flat_path = os.path.join(rules_dir, f'{safe_pattern}.json')
    if os.path.exists(flat_path):
        return flat_path
    
    return None


# ============================================================
# DOCUMENT INHERITANCE
# ============================================================

def calculate_inherited_documents(
    code: str,
    own_docs: Set[str],
    parent_docs: Dict[str, Set[str]],
    range_docs: Dict[str, Set[str]] = None
) -> Set[str]:
    """
    Вычисляет inherited документы для кода.
    
    Args:
        code: Код (E11.9)
        own_docs: Документы с explicit match
        parent_docs: {pattern: set(doc_ids)} для каждого родителя
        range_docs: {range_pattern: set(doc_ids)} для диапазонов
    
    Returns:
        Set of inherited doc_ids (excluding own_docs)
    """
    inherited = set()
    
    # Add from wildcard parents
    for pattern, docs in parent_docs.items():
        if code_matches_pattern(code, pattern):
            inherited.update(docs)
    
    # Add from ranges
    if range_docs:
        for range_pattern, docs in range_docs.items():
            if code_matches_pattern(code, range_pattern):
                inherited.update(docs)
    
    # Remove own docs
    inherited -= own_docs
    
    return inherited


# ============================================================
# HIERARCHY BUILDING
# ============================================================

def build_code_hierarchy(
    codes_with_docs: List[Dict],
    include_synthetic_parents: bool = True
) -> Dict[str, CodeNode]:
    """
    Строит полную иерархию кодов из списка кодов с документами.
    
    Args:
        codes_with_docs: [
            {'code': 'E11.9', 'type': 'ICD-10', 'documents': ['doc1', 'doc2']},
            {'code': 'E11.%', 'type': 'ICD-10', 'documents': ['doc3']},
            ...
        ]
        include_synthetic_parents: создавать ли узлы для неявных родителей
    
    Returns:
        {pattern: CodeNode, ...}
    """
    hierarchy = {}
    
    # First pass: create nodes for all explicit codes
    wildcards = {}  # pattern → docs
    ranges = {}     # range → docs
    specifics = {}  # code → docs
    
    for item in codes_with_docs:
        code = item['code'].upper()
        code_type = item.get('type', 'Unknown')
        docs = set(item.get('documents', []))
        
        if '%' in code:
            wildcards[code] = docs
        elif ':' in code:
            ranges[code] = docs
        else:
            specifics[code] = docs
    
    # Build parent_docs lookup
    parent_docs = {**wildcards}
    range_docs = {**ranges}
    
    # Create nodes for specific codes
    for code, docs in specifics.items():
        code_type = next(
            (item['type'] for item in codes_with_docs if item['code'].upper() == code),
            'Unknown'
        )
        parents = get_parent_patterns(code)
        parent_pattern = parents[0] if parents else None
        
        inherited = calculate_inherited_documents(code, docs, parent_docs, range_docs)
        
        hierarchy[code] = CodeNode(
            pattern=code,
            parent_pattern=parent_pattern,
            level=3,
            code_type=code_type,
            own_documents=list(docs),
            inherited_documents=list(inherited)
        )
    
    # Create nodes for wildcards
    for pattern, docs in wildcards.items():
        code_type = next(
            (item['type'] for item in codes_with_docs if item['code'].upper() == pattern),
            'Unknown'
        )
        parents = get_parent_patterns(pattern.replace('%', 'X'))  # Fake specific code
        parent_pattern = None
        for p in parents:
            if p != pattern and p in wildcards:
                parent_pattern = p
                break
        
        # Wildcards inherit from broader wildcards
        broader_wildcards = {p: d for p, d in wildcards.items() if p != pattern}
        inherited = calculate_inherited_documents(
            pattern.replace('%', '0'),  # Fake specific for matching
            docs, 
            broader_wildcards, 
            range_docs
        )
        
        hierarchy[pattern] = CodeNode(
            pattern=pattern,
            parent_pattern=parent_pattern,
            level=2 if pattern.count('%') == 1 else 1,
            code_type=code_type,
            own_documents=list(docs),
            inherited_documents=list(inherited)
        )
    
    # Create nodes for ranges
    for range_pattern, docs in ranges.items():
        code_type = next(
            (item['type'] for item in codes_with_docs if item['code'].upper() == range_pattern),
            'Unknown'
        )
        
        hierarchy[range_pattern] = CodeNode(
            pattern=range_pattern,
            parent_pattern=None,
            level=2,
            code_type=code_type,
            own_documents=list(docs),
            inherited_documents=[]
        )
    
    return hierarchy


# ============================================================
# EXPORTS
# ============================================================

__all__ = [
    'CodeNode',
    'get_parent_patterns',
    'get_rule_cascade',
    'code_matches_pattern',
    'find_matching_ranges',
    'find_rule_for_code',
    'calculate_inherited_documents',
    'build_code_hierarchy',
]
