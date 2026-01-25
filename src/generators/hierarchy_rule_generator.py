"""
Hierarchical Rule Generator

Two generation modes:

## GUIDELINE MODE
Generate rule from clinical guidelines.
- Check parents: E11.65 → E11.6 → E11 → E
- Generate missing parents first (top-down): E → E11 → E11.6 → E11.65
- Each level gets its own rule

## CMS1500 MODE
Generate CMS-1500 billing instructions.
- Prerequisite: Guideline rule must exist for EXACT code (no parent lookup)
- Generate cascade (top-down): E → E11 → E11.6 → E11.65

## RULE APPLICATION (inheritance)
When applying rules to a claim:
- Lookup: E11.65 → E11.6 → E11 → E
- Merge: E + E11 + E11.6 + E11.65 = composite rule

Usage:
    generator = HierarchyRuleGenerator()

    # Generate guideline rule (with parent cascade)
    async for event in generator.generate_guideline("E11.65"):
        yield event

    # Generate CMS-1500 (requires guideline prerequisite)
    async for event in generator.generate_cms1500("E11.65"):
        yield event

    # Apply rules (inheritance merge)
    rules = generator.get_applicable_rules("E11.65")
"""

import os
import json
from typing import List, Dict, Optional, Tuple, AsyncGenerator
from dataclasses import dataclass
from datetime import datetime

from .rule_generator import RuleGenerator, PipelineResult
from .context_builder import build_sources_context, SourcesContext, SourceDocument
from src.db.connection import get_db_connection


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class HierarchyLevel:
    """A level in the code hierarchy."""
    pattern: str           # E11, E11.6, E11.65
    level: int             # 1=category, 2=subcategory, 3=code
    pattern_type: str      # 'category', 'subcategory', 'code'
    description: str = ""
    has_guideline: bool = False
    has_cms1500: bool = False
    guideline_rule_id: Optional[int] = None
    cms1500_rule_id: Optional[int] = None


@dataclass
class RuleLookupResult:
    """Result of rule lookup in hierarchy."""
    code: str              # Original code queried
    rule_type: str         # 'guideline' or 'cms1500'
    found: bool            # Whether a rule was found
    rule_pattern: str = "" # Pattern that has the rule (may be parent)
    rule_id: int = 0
    inheritance_level: int = 0  # 0=exact, 1=parent, 2=grandparent, etc.
    hierarchy: List[HierarchyLevel] = None

    def __post_init__(self):
        if self.hierarchy is None:
            self.hierarchy = []


@dataclass
class GenerationPlan:
    """Plan for cascade generation."""
    target_code: str
    rule_type: str  # 'guideline' or 'cms1500'
    patterns_to_generate: List[str]  # Ordered top-down: [E, E11, E11.6, E11.65]
    existing_patterns: List[str]     # Already have rules
    prerequisite_met: bool = True
    prerequisite_error: str = ""


# ============================================================
# HIERARCHY UTILITIES
# ============================================================

def get_meta_category(code: str) -> str:
    """
    Extract meta-category (first letter) from code.

    E11.65 → E
    F32.1 → F
    99213 → 9
    J1950 → J
    """
    if not code:
        return ""
    return code[0].upper()


def get_hierarchy_patterns(code: str, code_type: str = "ICD-10") -> List[str]:
    """
    Get all hierarchy patterns from code up to meta-category.

    ICD-10 examples:
        E11.65 → ['E11.65', 'E11.6', 'E11', 'E']
        E11.9  → ['E11.9', 'E11', 'E']
        E11    → ['E11', 'E']
        E      → ['E']

    CPT/HCPCS examples:
        J1950 → ['J1950', 'J195', 'J19', 'J1', 'J']
    """
    if not code:
        return []

    code = code.upper()
    patterns = [code]

    # Single character = meta-category, nothing more to add
    if len(code) == 1:
        return patterns

    if code_type == "ICD-10":
        if '.' in code:
            # ICD-10 with suffix: E11.65 → E11.6 → E11 → E
            parts = code.split('.')
            base = parts[0]  # E11
            suffix = parts[1] if len(parts) > 1 else ""

            # Add intermediate suffix patterns (E11.65 → E11.6)
            for i in range(len(suffix) - 1, 0, -1):
                patterns.append(f"{base}.{suffix[:i]}")

            # Add base category (E11)
            patterns.append(base)

            # Add meta-category (E)
            meta = get_meta_category(code)
            if meta and meta != base:
                patterns.append(meta)
        else:
            # ICD-10 category without suffix: E11 → E
            meta = get_meta_category(code)
            if meta and meta != code:
                patterns.append(meta)

    elif code_type in ("CPT", "HCPCS"):
        # CPT/HCPCS: J1950 → J195 → J19 → J1 → J
        for i in range(len(code) - 1, 0, -1):
            patterns.append(code[:i])

    else:
        # Unknown format - just add meta-category
        meta = get_meta_category(code)
        if meta and meta != code:
            patterns.append(meta)

    return patterns


def get_pattern_type(pattern: str, code_type: str = "ICD-10") -> str:
    """
    Determine the type of pattern.

    E → meta_category
    E11 → category
    E11.6 → subcategory
    E11.65 → code
    """
    if not pattern:
        return "unknown"

    if len(pattern) == 1:
        return "meta_category"

    if code_type == "ICD-10":
        if '.' not in pattern:
            return "category"
        parts = pattern.split('.')
        suffix_len = len(parts[1]) if len(parts) > 1 else 0
        if suffix_len == 1:
            return "subcategory"
        elif suffix_len >= 2:
            return "code"
        return "category"
    else:
        # CPT/HCPCS - by length
        if len(pattern) <= 2:
            return "category"
        elif len(pattern) <= 4:
            return "subcategory"
        else:
            return "code"


# ============================================================
# HIERARCHY RULE GENERATOR
# ============================================================

class HierarchyRuleGenerator:
    """
    Generates rules with hierarchy awareness.

    Two modes:
    1. GUIDELINE: Generate from clinical guidelines
       - Check parents, generate missing parents first
       - Top-down cascade: E → E11 → E11.6 → E11.65

    2. CMS1500: Generate CMS-1500 billing instructions
       - Prerequisite: guideline must exist for EXACT code
       - Top-down cascade: E → E11 → E11.6 → E11.65
    """

    # Default model - can be overridden via env var or constructor
    DEFAULT_MODEL = os.getenv("RULE_GENERATOR_MODEL", "gemini")  # "gemini" or "gpt-4.1"

    def __init__(self, thinking_budget: int = 10000, model: str = None):
        self.thinking_budget = thinking_budget
        self.model = model or self.DEFAULT_MODEL
        self._conn = None

    def _get_conn(self):
        """Get or create database connection."""
        if self._conn is None:
            self._conn = get_db_connection()
        return self._conn

    # ============================================================
    # RULE LOOKUP
    # ============================================================

    def has_rule(
        self,
        pattern: str,
        rule_type: str,
        code_type: str = "ICD-10"
    ) -> Tuple[bool, Optional[int]]:
        """
        Check if a specific pattern has a rule of given type.

        Returns:
            (has_rule, rule_id)
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT rule_id, has_own_rule
            FROM rules_hierarchy
            WHERE pattern = ? AND code_type = ? AND rule_type = ?
        """, (pattern.upper(), code_type, rule_type))

        row = cursor.fetchone()
        if row and row[1]:  # has_own_rule = 1
            return True, row[0]
        return False, None

    def is_pattern_covered(
        self,
        pattern: str,
        rule_type: str,
        code_type: str = "ICD-10"
    ) -> Tuple[bool, str]:
        """
        Check if pattern is covered (has own rule OR same_as_parent).

        Returns:
            (is_covered, status) - status: 'ready', 'same_as_parent', or 'not_covered'
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT has_own_rule, status, inherits_from
            FROM rules_hierarchy
            WHERE pattern = ? AND code_type = ? AND rule_type = ?
        """, (pattern.upper(), code_type, rule_type))

        row = cursor.fetchone()
        if not row:
            return False, 'not_covered'

        has_own = row[0] == 1
        status = row[1] or 'pending'

        if has_own and status == 'ready':
            return True, 'ready'
        elif status == 'same_as_parent':
            return True, 'same_as_parent'

        return False, status

    def get_hierarchy_from_db(
        self,
        pattern: str,
        code_type: str = "ICD-10"
    ) -> List[str]:
        """
        Get hierarchy patterns from code_hierarchy table.

        Returns patterns from specific to general: [E11.65, E11.6, E11, E]
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        patterns = []
        current = pattern.upper()

        while current:
            patterns.append(current)

            # Get parent from code_hierarchy
            cursor.execute("""
                SELECT parent_pattern FROM code_hierarchy
                WHERE pattern = ? AND code_type = ?
            """, (current, code_type))

            row = cursor.fetchone()
            if row and row[0]:
                current = row[0]
            else:
                break

        return patterns

    def register_same_as_parent(
        self,
        pattern: str,
        rule_type: str,
        code_type: str
    ) -> None:
        """
        Register pattern as same_as_parent (no own rule, inherits from parent).
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        ptype = get_pattern_type(pattern, code_type)
        hierarchy = self.get_hierarchy_from_db(pattern, code_type)
        parent_pattern = hierarchy[1] if len(hierarchy) > 1 else None

        cursor.execute("""
            INSERT INTO rules_hierarchy
            (pattern, pattern_type, code_type, rule_type, parent_pattern, has_own_rule, inherits_from, status)
            VALUES (?, ?, ?, ?, ?, 0, ?, 'same_as_parent')
            ON CONFLICT(code_type, pattern, rule_type) DO UPDATE SET
                has_own_rule = 0,
                inherits_from = excluded.inherits_from,
                status = 'same_as_parent',
                rule_id = NULL
        """, (pattern.upper(), ptype, code_type, rule_type, parent_pattern, parent_pattern))

        conn.commit()

    def find_applicable_rule(
        self,
        code: str,
        rule_type: str = "guideline",
        code_type: str = "ICD-10"
    ) -> RuleLookupResult:
        """
        Find applicable rule for a code by checking hierarchy.

        Cascade lookup order:
        1. Exact code (E11.65)
        2. Subcategory (E11.6)
        3. Category (E11)
        4. Meta-category (E)

        Returns:
            RuleLookupResult with rule info if found
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        patterns = get_hierarchy_patterns(code, code_type)
        hierarchy = []

        result = RuleLookupResult(
            code=code,
            rule_type=rule_type,
            found=False,
            hierarchy=hierarchy
        )

        # Check each level
        for i, pattern in enumerate(patterns):
            ptype = get_pattern_type(pattern, code_type)

            # Check rules_hierarchy for both types
            cursor.execute("""
                SELECT rule_type, rule_id, has_own_rule
                FROM rules_hierarchy
                WHERE pattern = ? AND code_type = ?
            """, (pattern.upper(), code_type))

            rows = cursor.fetchall()

            has_guideline = False
            has_cms1500 = False
            guideline_id = None
            cms1500_id = None

            for row in rows:
                if row[0] == 'guideline' and row[2]:
                    has_guideline = True
                    guideline_id = row[1]
                elif row[0] == 'cms1500' and row[2]:
                    has_cms1500 = True
                    cms1500_id = row[1]

            # Get description from code_hierarchy
            cursor.execute("""
                SELECT description FROM code_hierarchy
                WHERE pattern = ? AND code_type = ?
            """, (pattern.upper(), code_type))
            desc_row = cursor.fetchone()
            description = desc_row[0] if desc_row else ""

            hierarchy.append(HierarchyLevel(
                pattern=pattern,
                level=i + 1,
                pattern_type=ptype,
                description=description,
                has_guideline=has_guideline,
                has_cms1500=has_cms1500,
                guideline_rule_id=guideline_id,
                cms1500_rule_id=cms1500_id
            ))

            # Check if this level has the requested rule type
            has_target = has_guideline if rule_type == "guideline" else has_cms1500
            target_id = guideline_id if rule_type == "guideline" else cms1500_id

            if has_target and not result.found:
                result.found = True
                result.rule_pattern = pattern
                result.rule_id = target_id
                result.inheritance_level = i

        result.hierarchy = hierarchy
        return result

    # ============================================================
    # GENERATION PLANNING
    # ============================================================

    def plan_guideline_generation(
        self,
        code: str,
        code_type: str = "ICD-10"
    ) -> GenerationPlan:
        """
        Plan guideline generation with parent cascade.

        Logic:
        1. Get hierarchy from code_hierarchy: E11.65 → E11.6 → E11 → E
        2. Check which levels are covered (has_own_rule OR same_as_parent)
        3. Generate missing levels top-down: E → E11 → E11.6 → E11.65
        """
        # Use hierarchy from DB (code_hierarchy table)
        patterns = self.get_hierarchy_from_db(code, code_type)
        if not patterns:
            # Fallback to computed patterns
            patterns = get_hierarchy_patterns(code, code_type)

        existing = []
        missing = []

        for pattern in patterns:
            covered, status = self.is_pattern_covered(pattern, "guideline", code_type)
            if covered:
                existing.append((pattern, status))
            else:
                missing.append(pattern)

        # Reverse missing for top-down order: [E, E11, E11.6, E11.65]
        missing_topdown = list(reversed(missing))

        return GenerationPlan(
            target_code=code,
            rule_type="guideline",
            patterns_to_generate=missing_topdown,
            existing_patterns=[p for p, _ in existing],
            prerequisite_met=True
        )

    def plan_cms1500_generation(
        self,
        code: str,
        code_type: str = "ICD-10"
    ) -> GenerationPlan:
        """
        Plan CMS-1500 generation.

        Prerequisite: Guideline rule must exist for EXACT code.
        Then generate CMS-1500 for entire chain top-down.
        """
        # Use hierarchy from DB
        patterns = self.get_hierarchy_from_db(code, code_type)
        if not patterns:
            patterns = get_hierarchy_patterns(code, code_type)

        # Check prerequisite: guideline for exact code (covered = has_own OR same_as_parent)
        guideline_covered, _ = self.is_pattern_covered(code, "guideline", code_type)

        if not guideline_covered:
            return GenerationPlan(
                target_code=code,
                rule_type="cms1500",
                patterns_to_generate=[],
                existing_patterns=[],
                prerequisite_met=False,
                prerequisite_error=f"Guideline rule required for {code} before generating CMS-1500"
            )

        # Check which levels have cms1500 rules (covered = has_own OR same_as_parent)
        existing = []
        missing = []

        for pattern in patterns:
            covered, status = self.is_pattern_covered(pattern, "cms1500", code_type)
            if covered:
                existing.append((pattern, status))
            else:
                missing.append(pattern)

        # Reverse missing for top-down order
        missing_topdown = list(reversed(missing))

        return GenerationPlan(
            target_code=code,
            rule_type="cms1500",
            patterns_to_generate=missing_topdown,
            existing_patterns=existing,
            prerequisite_met=True
        )

    def get_documents_for_code(
        self,
        code: str,
        code_type: str = "ICD-10",
        use_meta_category: bool = True
    ) -> Tuple[List[Dict], str]:
        """
        Find documents relevant to a code.

        Strategy:
        1. Direct match on code pattern
        2. If use_meta_category=True and no direct matches, use meta-category

        Returns:
            (list of document info, search_strategy)
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        meta_cat = get_meta_category(code)

        # First, try direct match
        cursor.execute("""
            SELECT DISTINCT
                dc.document_id,
                d.filename,
                d.file_hash,
                dc.code_pattern,
                d.doc_type
            FROM document_codes dc
            JOIN documents d ON dc.document_id = d.file_hash
            WHERE dc.code_pattern = ?
              AND dc.code_type = ?
              AND (d.doc_type IS NULL OR d.doc_type != 'policy')
        """, (code, code_type))

        rows = cursor.fetchall()

        if rows:
            return [
                {
                    'document_id': r[0],
                    'filename': r[1],
                    'file_hash': r[2],
                    'code_pattern': r[3],
                    'doc_type': r[4]
                }
                for r in rows
            ], "direct_match"

        # Try meta-category match
        if use_meta_category and meta_cat:
            cursor.execute("""
                SELECT DISTINCT
                    dc.document_id,
                    d.filename,
                    d.file_hash,
                    dc.code_pattern,
                    d.doc_type
                FROM document_codes dc
                JOIN documents d ON dc.document_id = d.file_hash
                WHERE dc.code_pattern = ?
                  AND dc.code_type = ?
                  AND (d.doc_type IS NULL OR d.doc_type != 'policy')
            """, (meta_cat, code_type))

            rows = cursor.fetchall()

            if rows:
                return [
                    {
                        'document_id': r[0],
                        'filename': r[1],
                        'file_hash': r[2],
                        'code_pattern': r[3],
                        'doc_type': r[4]
                    }
                    for r in rows
                ], "meta_category"

        return [], "no_match"

    def determine_generation_level(
        self,
        code: str,
        code_type: str = "ICD-10"
    ) -> Tuple[str, str]:
        """
        Determine at which level to generate the rule.

        Logic:
        1. If code is a meta-category (single letter), generate at meta level
        2. If code has siblings with same documents, generate at parent level
        3. Otherwise, generate at code level

        Returns:
            (pattern_to_generate, reason)
        """
        patterns = get_hierarchy_patterns(code, code_type)

        # If code is already a category or meta-category, generate there
        if len(patterns) <= 2:  # meta-category or category only
            return code, "direct_code"

        # Get documents for the code
        docs, strategy = self.get_documents_for_code(code, code_type)

        if not docs:
            # No documents - generate at meta-category level
            meta = get_meta_category(code)
            return meta, "no_docs_fallback_to_meta"

        # For now, generate at the requested code level
        # Future: check if siblings share same documents
        return code, "code_level"

    def register_rule(
        self,
        pattern: str,
        rule_type: str,
        code_type: str,
        rule_id: int
    ) -> None:
        """
        Register a rule in the hierarchy with status='ready'.

        Args:
            pattern: Code pattern (E11, E11.65, etc.)
            rule_type: 'guideline' or 'cms1500'
            code_type: 'ICD-10', 'CPT', 'HCPCS'
            rule_id: ID in rules table
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        ptype = get_pattern_type(pattern, code_type)

        # Get parent from DB hierarchy
        hierarchy = self.get_hierarchy_from_db(pattern, code_type)
        parent_pattern = hierarchy[1] if len(hierarchy) > 1 else None

        # Upsert this pattern's entry with status='ready'
        cursor.execute("""
            INSERT INTO rules_hierarchy (pattern, pattern_type, code_type, rule_type, parent_pattern, rule_id, has_own_rule, inherits_from, status)
            VALUES (?, ?, ?, ?, ?, ?, 1, NULL, 'ready')
            ON CONFLICT(code_type, pattern, rule_type) DO UPDATE SET
                rule_id = excluded.rule_id,
                has_own_rule = 1,
                inherits_from = NULL,
                status = 'ready'
        """, (pattern.upper(), ptype, code_type, rule_type, parent_pattern, rule_id))

        # Update children to inherit from this pattern (if they don't have own rules)
        cursor.execute("""
            UPDATE rules_hierarchy
            SET inherits_from = ?
            WHERE code_type = ?
              AND rule_type = ?
              AND parent_pattern = ?
              AND has_own_rule = 0
        """, (pattern.upper(), code_type, rule_type, pattern.upper()))

        conn.commit()

    # ============================================================
    # PARENT RULE RETRIEVAL
    # ============================================================

    def get_parent_rule_content(
        self,
        pattern: str,
        rule_type: str,
        code_type: str = "ICD-10"
    ) -> Optional[Dict]:
        """
        Get the immediate parent's rule content.

        For E11.6 → returns E11's rule
        For E11 → returns E's rule
        For E → returns None
        """
        patterns = get_hierarchy_patterns(pattern, code_type)

        if len(patterns) < 2:
            return None  # No parent (meta-category)

        parent_pattern = patterns[1]
        has, rule_id = self.has_rule(parent_pattern, rule_type, code_type)

        if not has or not rule_id:
            return None

        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT code, rule_path, source_documents
            FROM rules WHERE id = ?
        """, (rule_id,))

        row = cursor.fetchone()
        if not row:
            return None

        rule_content = None
        rule_markdown = None

        if row[1] and os.path.exists(row[1]):
            # rule_path can be a directory (e.g., .../v1) or file (e.g., .../v1/rule.json)
            rule_dir = row[1] if os.path.isdir(row[1]) else os.path.dirname(row[1])

            if rule_type == "cms1500":
                # CMS rules: load markdown for prompt context
                cms_md = os.path.join(rule_dir, "cms_rule.md")
                cms_json = os.path.join(rule_dir, "cms_rule.json")
                if os.path.exists(cms_md):
                    with open(cms_md, 'r', encoding='utf-8') as f:
                        rule_markdown = f.read()
                if os.path.exists(cms_json):
                    with open(cms_json, 'r', encoding='utf-8') as f:
                        rule_content = json.load(f)
            else:
                # Guideline rules: load rule.json
                rule_json = os.path.join(rule_dir, "rule.json")
                if os.path.exists(rule_json):
                    with open(rule_json, 'r', encoding='utf-8') as f:
                        rule_content = json.load(f)

        return {
            "pattern": parent_pattern,
            "rule_id": rule_id,
            "rule_type": rule_type,
            "content": rule_content,
            "markdown": rule_markdown  # Full markdown text for CMS inheritance
        }

    def get_guideline_for_pattern(
        self,
        pattern: str,
        code_type: str = "ICD-10"
    ) -> Optional[Dict]:
        """
        Get guideline rule for a specific pattern.
        Used when generating CMS-1500 (needs guideline as input).
        """
        has, rule_id = self.has_rule(pattern, "guideline", code_type)

        if not has or not rule_id:
            return None

        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT code, rule_path
            FROM rules WHERE id = ?
        """, (rule_id,))

        row = cursor.fetchone()
        if not row:
            return None

        rule_content = None
        if row[1] and os.path.exists(row[1]):
            rule_json = os.path.join(os.path.dirname(row[1]), "rule.json")
            if os.path.exists(rule_json):
                with open(rule_json, 'r', encoding='utf-8') as f:
                    rule_content = json.load(f)

        return {
            "pattern": pattern,
            "rule_id": rule_id,
            "content": rule_content
        }

    # ============================================================
    # GUIDELINE GENERATION
    # ============================================================

    async def generate_guideline(
        self,
        code: str,
        code_type: str = "ICD-10",
        document_ids: Optional[List[str]] = None,
        force_regenerate: bool = False
    ) -> AsyncGenerator[str, None]:
        """
        Generate guideline rule with parent cascade and inheritance.

        Each level uses:
        - Parent's guideline rule (if exists)
        - + Documents for current level

        E → documents(E)
        E11 → rule(E) + documents(E11)
        E11.6 → rule(E11) + documents(E11.6)

        Yields:
            JSON SSE events
        """
        import json

        # Step 1: Plan
        plan = self.plan_guideline_generation(code, code_type)

        if not plan.patterns_to_generate:
            lookup = self.find_applicable_rule(code, "guideline", code_type)
            yield json.dumps({
                "step": "guideline",
                "type": "exists",
                "content": f"Guideline rule already exists for {code}",
                "rule_id": lookup.rule_id,
                "existing_patterns": plan.existing_patterns
            }, ensure_ascii=False)
            return

        yield json.dumps({
            "step": "guideline",
            "type": "plan",
            "content": f"Will generate {len(plan.patterns_to_generate)} rules with inheritance",
            "patterns_to_generate": plan.patterns_to_generate,
            "existing_patterns": plan.existing_patterns
        }, ensure_ascii=False)

        # Step 2: Generate each level top-down with inheritance
        for pattern in plan.patterns_to_generate:
            if not force_regenerate:
                has, _ = self.has_rule(pattern, "guideline", code_type)
                if has:
                    yield json.dumps({
                        "step": "guideline",
                        "type": "skip",
                        "content": f"Rule already exists for {pattern}"
                    }, ensure_ascii=False)
                    continue

            # Get parent rule (will be None for meta-category)
            parent_rule = self.get_parent_rule_content(pattern, "guideline", code_type)

            yield json.dumps({
                "step": "guideline",
                "type": "generating",
                "content": f"Generating guideline for {pattern}",
                "pattern": pattern,
                "parent_rule": parent_rule["pattern"] if parent_rule else None
            }, ensure_ascii=False)

            # Generate with parent rule context
            async for event in self._generate_single_rule(
                pattern=pattern,
                rule_type="guideline",
                code_type=code_type,
                document_ids=document_ids,
                parent_rule=parent_rule
            ):
                yield event

        yield json.dumps({
            "step": "guideline",
            "type": "complete",
            "content": f"Generated guidelines for {len(plan.patterns_to_generate)} levels",
            "target_code": code
        }, ensure_ascii=False)

    # ============================================================
    # CMS-1500 GENERATION
    # ============================================================

    async def generate_cms1500(
        self,
        code: str,
        code_type: str = "ICD-10",
        document_ids: Optional[List[str]] = None,
        force_regenerate: bool = False
    ) -> AsyncGenerator[str, None]:
        """
        Generate CMS-1500 rule with parent cascade and inheritance.

        Each level uses:
        - Parent's CMS-1500 rule (if exists)
        - + Current pattern's guideline rule
        - + NCCI edits

        CMS1500(E) → guideline(E) + NCCI
        CMS1500(E11) → CMS1500(E) + guideline(E11) + NCCI
        CMS1500(E11.6) → CMS1500(E11) + guideline(E11.6) + NCCI

        Prerequisite: Guideline rule must exist for EXACT target code.

        Yields:
            JSON SSE events
        """
        import json

        # Step 1: Plan (includes prerequisite check)
        plan = self.plan_cms1500_generation(code, code_type)

        if not plan.prerequisite_met:
            yield json.dumps({
                "step": "cms1500",
                "type": "error",
                "content": plan.prerequisite_error,
                "prerequisite_met": False
            }, ensure_ascii=False)
            return

        # If force_regenerate, add target code to patterns_to_generate
        if force_regenerate and code not in plan.patterns_to_generate:
            plan.patterns_to_generate.append(code)

        if not plan.patterns_to_generate:
            lookup = self.find_applicable_rule(code, "cms1500", code_type)
            yield json.dumps({
                "step": "cms1500",
                "type": "exists",
                "content": f"CMS-1500 rule already exists for {code}",
                "rule_id": lookup.rule_id,
                "existing_patterns": plan.existing_patterns
            }, ensure_ascii=False)
            return

        yield json.dumps({
            "step": "cms1500",
            "type": "plan",
            "content": f"Will generate {len(plan.patterns_to_generate)} CMS-1500 rules with inheritance",
            "patterns_to_generate": plan.patterns_to_generate,
            "existing_patterns": plan.existing_patterns
        }, ensure_ascii=False)

        # Cache for just-generated rules in this cascade (pattern -> markdown content)
        cascade_cache: Dict[str, str] = {}

        # Step 2: Generate each level top-down with inheritance
        for pattern in plan.patterns_to_generate:
            if not force_regenerate:
                has, _ = self.has_rule(pattern, "cms1500", code_type)
                if has:
                    yield json.dumps({
                        "step": "cms1500",
                        "type": "skip",
                        "content": f"CMS-1500 rule already exists for {pattern}"
                    }, ensure_ascii=False)
                    continue

            # Get parent CMS-1500 rule - check cascade cache first
            parent_cms1500 = None
            parent_patterns = get_hierarchy_patterns(pattern, code_type)
            if len(parent_patterns) > 1:
                parent_pattern = parent_patterns[1]
                if parent_pattern in cascade_cache:
                    # Use just-generated rule from this cascade
                    parent_cms1500 = {
                        "pattern": parent_pattern,
                        "markdown": cascade_cache[parent_pattern],
                        "content": None
                    }
                    yield json.dumps({
                        "step": "cms1500",
                        "type": "status",
                        "content": f"Using just-generated parent rule from {parent_pattern}"
                    }, ensure_ascii=False)
                else:
                    # Fall back to database lookup
                    parent_cms1500 = self.get_parent_rule_content(pattern, "cms1500", code_type)
                    if parent_cms1500 and parent_cms1500.get("markdown"):
                        yield json.dumps({
                            "step": "cms1500",
                            "type": "status",
                            "content": f"Found parent CMS rule from DB: {parent_pattern} ({len(parent_cms1500['markdown'])} chars)"
                        }, ensure_ascii=False)
                    else:
                        yield json.dumps({
                            "step": "cms1500",
                            "type": "status",
                            "content": f"No parent CMS rule found for {parent_pattern} in DB"
                        }, ensure_ascii=False)

            # Get guideline for this pattern
            guideline = self.get_guideline_for_pattern(pattern, code_type)

            yield json.dumps({
                "step": "cms1500",
                "type": "generating",
                "content": f"Generating CMS-1500 for {pattern}",
                "pattern": pattern,
                "parent_cms1500": parent_cms1500["pattern"] if parent_cms1500 else None,
                "guideline": guideline["pattern"] if guideline else None
            }, ensure_ascii=False)

            # Generate with parent CMS-1500 + guideline context
            generated_markdown = None
            async for event in self._generate_single_rule(
                pattern=pattern,
                rule_type="cms1500",
                code_type=code_type,
                document_ids=document_ids,
                parent_rule=parent_cms1500,
                guideline_rule=guideline
            ):
                yield event
                # Capture the generated markdown for cascade cache
                try:
                    evt = json.loads(event)
                    if evt.get("step") == "transform" and evt.get("type") == "done":
                        generated_markdown = evt.get("full_text", "")
                except:
                    pass

            # Store in cascade cache for child patterns
            if generated_markdown:
                cascade_cache[pattern.upper()] = generated_markdown

        yield json.dumps({
            "step": "cms1500",
            "type": "complete",
            "content": f"Generated CMS-1500 rules for {len(plan.patterns_to_generate)} levels",
            "target_code": code
        }, ensure_ascii=False)

    # ============================================================
    # SINGLE RULE GENERATION
    # ============================================================

    async def _generate_single_rule(
        self,
        pattern: str,
        rule_type: str,
        code_type: str,
        document_ids: Optional[List[str]] = None,
        parent_rule: Optional[Dict] = None,
        guideline_rule: Optional[Dict] = None
    ) -> AsyncGenerator[str, None]:
        """
        Generate a single rule at a specific pattern level.

        For GUIDELINE:
            context = parent_rule(guideline) + documents

        For CMS-1500:
            context = parent_rule(cms1500) + guideline_rule + NCCI

        Args:
            pattern: Code pattern to generate rule for
            rule_type: 'guideline' or 'cms1500'
            code_type: 'ICD-10', 'CPT', 'HCPCS'
            document_ids: Specific documents (optional)
            parent_rule: Parent's rule of same type (for inheritance)
            guideline_rule: Guideline for this pattern (CMS-1500 only)
        """
        import json

        # Find documents
        docs, strategy = self.get_documents_for_code(pattern, code_type)

        if not docs and strategy == "no_match":
            meta = get_meta_category(pattern)
            docs, strategy = self.get_documents_for_code(meta, code_type, use_meta_category=False)

        yield json.dumps({
            "step": rule_type,
            "type": "documents",
            "content": f"Found {len(docs)} documents via {strategy}",
            "pattern": pattern,
            "documents": [{"filename": d['filename'], "doc_type": d.get('doc_type')} for d in docs[:5]]
        }, ensure_ascii=False)

        # For guideline: documents are required
        # For cms1500: guideline is required, documents optional (NCCI may suffice)
        if rule_type == "guideline" and not docs:
            yield json.dumps({
                "step": rule_type,
                "type": "error",
                "content": f"No documents found for {pattern}",
                "pattern": pattern
            }, ensure_ascii=False)
            return

        # Build context and generate
        doc_ids = document_ids or [d['document_id'] for d in docs] if docs else []

        # Prepare inheritance context
        inheritance_context = self._build_inheritance_context(
            pattern=pattern,
            rule_type=rule_type,
            parent_rule=parent_rule,
            guideline_rule=guideline_rule
        )

        if inheritance_context:
            yield json.dumps({
                "step": rule_type,
                "type": "inheritance",
                "content": f"Using inherited rules in context",
                "pattern": pattern,
                "parent_rule": parent_rule["pattern"] if parent_rule else None,
                "guideline_rule": guideline_rule["pattern"] if guideline_rule else None
            }, ensure_ascii=False)

        save_path = None
        source_documents = [d['document_id'] for d in docs] if docs else []

        # Use different generators for guideline vs CMS-1500
        if rule_type == "guideline":
            # Guideline: 5-step pipeline (draft → mentor → redteam → arbitration → finalization)
            generator = RuleGenerator(thinking_budget=self.thinking_budget, model=self.model)
            async for event_json in generator.stream_pipeline(
                code=pattern,
                document_ids=doc_ids if doc_ids else None,
                code_type=code_type,
                parallel_validators=True,  # Mentor + RedTeam in parallel
                inheritance_context=inheritance_context
            ):
                yield event_json

                # Capture save_path from pipeline done event
                try:
                    event = json.loads(event_json)
                    if event.get("step") == "pipeline" and event.get("type") == "done":
                        content = event.get("content", "")
                        if "Saved to " in content:
                            save_path = content.split("Saved to ")[-1]
                except:
                    pass
        else:
            # CMS-1500: 2-step pipeline (transform + parse) using CMSRuleGenerator
            from .cms_generator import CMSRuleGenerator
            cms_gen = CMSRuleGenerator(thinking_budget=self.thinking_budget, model=self.model)

            # Get parent CMS-1500 rule content for inheritance
            # Use markdown (full text) for prompt context
            parent_cms1500_content = None
            if parent_rule:
                # Prefer markdown (full formatted rules) over JSON
                if parent_rule.get("markdown"):
                    parent_cms1500_content = parent_rule["markdown"]
                elif parent_rule.get("content"):
                    # Fallback: convert JSON to readable format
                    content = parent_rule["content"]
                    if isinstance(content, dict):
                        rules = content.get("validatable_rules", [])
                        parent_cms1500_content = f"Parent CMS-1500 rules for {parent_rule.get('pattern')}:\n"
                        for r in rules:
                            parent_cms1500_content += f"- {r.get('id')}: {r.get('message', r.get('description', ''))}\n"
                    else:
                        parent_cms1500_content = str(content)

            async for event_json in cms_gen.stream_pipeline(
                code=pattern,
                code_type=code_type,
                parent_cms1500_rule=parent_cms1500_content
            ):
                yield event_json

                # Capture save_path from pipeline done event
                try:
                    event = json.loads(event_json)
                    if event.get("step") == "pipeline" and event.get("type") == "done":
                        content = event.get("content", "")
                        if "Saved to " in content:
                            save_path = content.split("Saved to ")[-1]
                except:
                    pass

        # Insert into rules table (guideline only - CMS-1500 already saves via CMSRuleGenerator)
        conn = self._get_conn()
        cursor = conn.cursor()

        if rule_type == "guideline" and save_path:
            from datetime import datetime
            cursor.execute("""
                INSERT INTO rules (code, code_type, rule_level, status, rule_path, source_documents, generated_at)
                VALUES (?, ?, ?, 'ready', ?, ?, ?)
            """, (
                pattern,
                code_type,
                get_pattern_type(pattern, code_type),
                save_path,
                json.dumps(source_documents),
                datetime.now().isoformat()
            ))
            conn.commit()
            rule_id = cursor.lastrowid
        else:
            # CMS-1500 - find the rule that was just saved by CMSRuleGenerator
            cursor.execute("""
                SELECT id FROM rules
                WHERE code = ? AND code_type = ? AND rule_level = 'cms'
                ORDER BY id DESC LIMIT 1
            """, (pattern, code_type))
            row = cursor.fetchone()
            rule_id = row[0] if row else None

        if rule_id:
            self.register_rule(pattern, rule_type, code_type, rule_id)

            yield json.dumps({
                "step": rule_type,
                "type": "registered",
                "content": f"Registered {rule_type} rule",
                "pattern": pattern,
                "rule_id": rule_id
            }, ensure_ascii=False)
        else:
            yield json.dumps({
                "step": rule_type,
                "type": "error",
                "content": f"No save_path returned from pipeline for {pattern}",
                "pattern": pattern
            }, ensure_ascii=False)

    def _build_inheritance_context(
        self,
        pattern: str,
        rule_type: str,
        parent_rule: Optional[Dict] = None,
        guideline_rule: Optional[Dict] = None
    ) -> Optional[str]:
        """
        Build inheritance context string for prompt.

        For GUIDELINE:
            === PARENT RULE: E11 ===
            [content of E11 guideline]

        For CMS-1500:
            === PARENT CMS-1500 RULE: E11 ===
            [content of E11 cms1500]

            === GUIDELINE FOR E11.6 ===
            [content of E11.6 guideline]
        """
        parts = []

        if parent_rule and parent_rule.get("content"):
            content = parent_rule["content"]
            if isinstance(content, dict):
                content = content.get("content", str(content))

            if rule_type == "guideline":
                parts.append(f"=== PARENT GUIDELINE RULE: {parent_rule['pattern']} ===")
            else:
                parts.append(f"=== PARENT CMS-1500 RULE: {parent_rule['pattern']} ===")
            parts.append(str(content))
            parts.append("")

        if guideline_rule and guideline_rule.get("content"):
            content = guideline_rule["content"]
            if isinstance(content, dict):
                content = content.get("content", str(content))

            parts.append(f"=== GUIDELINE FOR {pattern} ===")
            parts.append(str(content))
            parts.append("")

        return "\n".join(parts) if parts else None

    # ============================================================
    # RULE APPLICATION (Inheritance)
    # ============================================================

    def get_applicable_rules(
        self,
        code: str,
        rule_type: str = "guideline",
        code_type: str = "ICD-10"
    ) -> List[Dict]:
        """
        Get all applicable rules for a code with inheritance.

        Returns rules from most general to most specific:
        [E_rule, E11_rule, E11.6_rule, E11.65_rule]

        Use this when applying rules to a claim - merge in order.
        """
        patterns = get_hierarchy_patterns(code, code_type)
        rules = []

        conn = self._get_conn()
        cursor = conn.cursor()

        # Reverse to get top-down order (E → E11 → E11.6 → E11.65)
        for pattern in reversed(patterns):
            has, rule_id = self.has_rule(pattern, rule_type, code_type)
            if has and rule_id:
                cursor.execute("""
                    SELECT code, rule_path, source_documents, generated_at
                    FROM rules WHERE id = ?
                """, (rule_id,))

                row = cursor.fetchone()
                if row:
                    rule_content = None
                    if row[1] and os.path.exists(row[1]):
                        rule_json = os.path.join(os.path.dirname(row[1]), "rule.json")
                        if os.path.exists(rule_json):
                            with open(rule_json, 'r', encoding='utf-8') as f:
                                rule_content = json.load(f)

                    rules.append({
                        "pattern": pattern,
                        "rule_id": rule_id,
                        "rule_type": rule_type,
                        "source_documents": json.loads(row[2]) if row[2] else [],
                        "generated_at": row[3],
                        "content": rule_content
                    })

        return rules

    def get_merged_rule(
        self,
        code: str,
        rule_type: str = "guideline",
        code_type: str = "ICD-10"
    ) -> Optional[Dict]:
        """
        Get merged/composite rule for a code.

        Merges rules from E → E11 → E11.6 → E11.65
        with more specific rules taking precedence.

        Returns:
            Merged rule dict or None if no rules found
        """
        rules = self.get_applicable_rules(code, rule_type, code_type)

        if not rules:
            return None

        # For now, return the most specific rule
        # TODO: Implement actual merging logic based on rule structure
        most_specific = rules[-1]

        return {
            "requested_code": code,
            "rule_type": rule_type,
            "applied_patterns": [r["pattern"] for r in rules],
            "most_specific_pattern": most_specific["pattern"],
            "rule_id": most_specific["rule_id"],
            "content": most_specific["content"],
            "inheritance_chain": rules
        }

    # ============================================================
    # LEGACY COMPATIBILITY
    # ============================================================

    async def stream_hierarchy_pipeline(
        self,
        code: str,
        code_type: str = "ICD-10",
        document_ids: Optional[List[str]] = None,
        force_regenerate: bool = False,
        rule_type: str = "guideline"
    ) -> AsyncGenerator[str, None]:
        """
        Legacy method - routes to generate_guideline or generate_cms1500.
        """
        if rule_type == "cms1500":
            async for event in self.generate_cms1500(code, code_type, document_ids, force_regenerate):
                yield event
        else:
            async for event in self.generate_guideline(code, code_type, document_ids, force_regenerate):
                yield event

    def get_hierarchy_tree(
        self,
        meta_category: str,
        code_type: str = "ICD-10"
    ) -> List[Dict]:
        """
        Get the full hierarchy tree for a meta-category.

        Returns list of patterns with their rule status.
        Useful for UI to show which levels have rules.
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        # Get all patterns in this meta-category
        cursor.execute("""
            SELECT
                ch.pattern,
                ch.level,
                ch.description,
                ch.meta_category,
                rh.rule_id,
                rh.has_own_rule,
                rh.inherits_from
            FROM code_hierarchy ch
            LEFT JOIN rules_hierarchy rh ON ch.pattern = rh.pattern AND ch.code_type = rh.code_type
            WHERE ch.meta_category = ? AND ch.code_type = ?
            ORDER BY ch.pattern
        """, (meta_category.upper(), code_type))

        rows = cursor.fetchall()

        return [
            {
                "pattern": r[0],
                "level": r[1],
                "description": r[2],
                "meta_category": r[3],
                "rule_id": r[4],
                "has_own_rule": bool(r[5]),
                "inherits_from": r[6]
            }
            for r in rows
        ]

    def get_effective_rule_for_code(
        self,
        code: str,
        code_type: str = "ICD-10"
    ) -> Optional[Dict]:
        """
        Get the effective rule for a code (including inherited).

        Returns the rule content and info about inheritance.
        """
        lookup = self.find_applicable_rule(code, code_type)

        if not lookup.found:
            return None

        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT code, rule_path, source_documents, generated_at
            FROM rules WHERE id = ?
        """, (lookup.rule_id,))

        row = cursor.fetchone()
        if not row:
            return None

        rule_path = row[1]

        # Load rule content
        rule_content = None
        if rule_path and os.path.exists(rule_path):
            rule_json = os.path.join(os.path.dirname(rule_path), "rule.json")
            if os.path.exists(rule_json):
                with open(rule_json, 'r', encoding='utf-8') as f:
                    rule_content = json.load(f)

        return {
            "requested_code": code,
            "rule_pattern": lookup.rule_pattern,
            "rule_id": lookup.rule_id,
            "inherited": lookup.inheritance_level > 0,
            "inheritance_level": lookup.inheritance_level,
            "source_documents": json.loads(row[2]) if row[2] else [],
            "generated_at": row[3],
            "content": rule_content
        }


# ============================================================
# CONVENIENCE FUNCTIONS
# ============================================================

def find_rule_for_code(
    code: str,
    rule_type: str = "guideline",
    code_type: str = "ICD-10"
) -> Optional[Dict]:
    """
    Find applicable rule for a code with inheritance.

    Returns merged rule info if found, None otherwise.
    """
    generator = HierarchyRuleGenerator()
    return generator.get_merged_rule(code, rule_type, code_type)


def get_rules_for_code(
    code: str,
    rule_type: str = "guideline",
    code_type: str = "ICD-10"
) -> List[Dict]:
    """
    Get all applicable rules for a code (inheritance chain).

    Returns list from most general to most specific.
    """
    generator = HierarchyRuleGenerator()
    return generator.get_applicable_rules(code, rule_type, code_type)


async def generate_guideline_rule(
    code: str,
    code_type: str = "ICD-10",
    document_ids: Optional[List[str]] = None,
    thinking_budget: int = 10000,
    model: str = None
) -> AsyncGenerator[str, None]:
    """
    Generate guideline rule with parent cascade.

    Usage:
        async for event in generate_guideline_rule("E11.65"):
            yield f"data: {event}\\n\\n"
    """
    generator = HierarchyRuleGenerator(thinking_budget=thinking_budget, model=model)
    async for event in generator.generate_guideline(code, code_type, document_ids):
        yield event


async def generate_cms1500_rule(
    code: str,
    code_type: str = "ICD-10",
    document_ids: Optional[List[str]] = None,
    thinking_budget: int = 10000,
    model: str = None
) -> AsyncGenerator[str, None]:
    """
    Generate CMS-1500 rule with parent cascade.

    Prerequisite: Guideline rule must exist for exact code.

    Usage:
        async for event in generate_cms1500_rule("E11.65"):
            yield f"data: {event}\\n\\n"
    """
    generator = HierarchyRuleGenerator(thinking_budget=thinking_budget, model=model)
    async for event in generator.generate_cms1500(code, code_type, document_ids):
        yield event


# Legacy alias
async def generate_hierarchy_rule(
    code: str,
    code_type: str = "ICD-10",
    document_ids: Optional[List[str]] = None,
    thinking_budget: int = 10000,
    rule_type: str = "guideline",
    model: str = None
) -> AsyncGenerator[str, None]:
    """Legacy function - use generate_guideline_rule or generate_cms1500_rule instead."""
    generator = HierarchyRuleGenerator(thinking_budget=thinking_budget, model=model)
    async for event in generator.stream_hierarchy_pipeline(code, code_type, document_ids, rule_type=rule_type):
        yield event
