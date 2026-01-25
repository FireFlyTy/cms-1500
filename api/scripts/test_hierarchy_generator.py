"""
Test script for hierarchy_rule_generator.py

Tests:
1. Utility functions (no DB needed)
2. Planning logic (needs rules_hierarchy table)
3. Document lookup (needs documents with meta-categories)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.generators.hierarchy_rule_generator import (
    get_meta_category,
    get_hierarchy_patterns,
    get_pattern_type,
    HierarchyRuleGenerator,
    GenerationPlan
)


def test_utilities():
    """Test utility functions - no DB needed."""
    print("=" * 60)
    print("TEST 1: Utility Functions")
    print("=" * 60)

    # get_meta_category
    tests = [
        ("E11.65", "E"),
        ("F32.1", "F"),
        ("A00.9", "A"),
        ("J1950", "J"),
        ("99213", "9"),
        ("E", "E"),
    ]

    print("\nget_meta_category:")
    for code, expected in tests:
        result = get_meta_category(code)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {code} → {result} (expected: {expected})")

    # get_hierarchy_patterns
    print("\nget_hierarchy_patterns:")

    patterns_tests = [
        ("E11.65", "ICD-10", ["E11.65", "E11.6", "E11", "E"]),
        ("E11.9", "ICD-10", ["E11.9", "E11", "E"]),
        ("E11", "ICD-10", ["E11", "E"]),
        ("E", "ICD-10", ["E"]),
        ("F32.10", "ICD-10", ["F32.10", "F32.1", "F32", "F"]),
        ("J1950", "HCPCS", ["J1950", "J195", "J19", "J1", "J"]),
    ]

    for code, code_type, expected in patterns_tests:
        result = get_hierarchy_patterns(code, code_type)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {code} ({code_type})")
        print(f"      → {result}")
        if result != expected:
            print(f"      expected: {expected}")

    # get_pattern_type
    print("\nget_pattern_type:")

    type_tests = [
        ("E", "ICD-10", "meta_category"),
        ("E11", "ICD-10", "category"),
        ("E11.6", "ICD-10", "subcategory"),
        ("E11.65", "ICD-10", "code"),
        ("J", "HCPCS", "meta_category"),
        ("J1", "HCPCS", "category"),
    ]

    for pattern, code_type, expected in type_tests:
        result = get_pattern_type(pattern, code_type)
        status = "✓" if result == expected else "✗"
        print(f"  {status} {pattern} ({code_type}) → {result}")


def test_planning():
    """Test planning logic - needs DB with rules_hierarchy table."""
    print("\n" + "=" * 60)
    print("TEST 2: Planning Logic")
    print("=" * 60)

    try:
        generator = HierarchyRuleGenerator()

        # Test guideline planning
        print("\nGuideline planning for E11.65:")
        plan = generator.plan_guideline_generation("E11.65", "ICD-10")
        print(f"  Target: {plan.target_code}")
        print(f"  To generate: {plan.patterns_to_generate}")
        print(f"  Existing: {plan.existing_patterns}")
        print(f"  Prerequisite met: {plan.prerequisite_met}")

        # Test CMS-1500 planning
        print("\nCMS-1500 planning for E11.65:")
        plan = generator.plan_cms1500_generation("E11.65", "ICD-10")
        print(f"  Target: {plan.target_code}")
        print(f"  Prerequisite met: {plan.prerequisite_met}")
        if not plan.prerequisite_met:
            print(f"  Error: {plan.prerequisite_error}")
        print(f"  To generate: {plan.patterns_to_generate}")

        # Test with different code
        print("\nGuideline planning for F32.1:")
        plan = generator.plan_guideline_generation("F32.1", "ICD-10")
        print(f"  To generate: {plan.patterns_to_generate}")

    except Exception as e:
        print(f"  Error: {e}")
        print("  (This is expected if rules_hierarchy table doesn't exist yet)")


def test_document_lookup():
    """Test document lookup - needs documents with meta-categories."""
    print("\n" + "=" * 60)
    print("TEST 3: Document Lookup")
    print("=" * 60)

    try:
        generator = HierarchyRuleGenerator()

        # Test document lookup for meta-category
        for code in ["E", "R", "E11.65"]:
            docs, strategy = generator.get_documents_for_code(code, "ICD-10")
            print(f"\n  {code}: {len(docs)} documents via {strategy}")
            for doc in docs[:3]:
                print(f"    - {doc['filename']} ({doc.get('doc_type', 'unknown')})")

    except Exception as e:
        print(f"  Error: {e}")


def test_rule_lookup():
    """Test rule lookup - needs rules_hierarchy with some data."""
    print("\n" + "=" * 60)
    print("TEST 4: Rule Lookup")
    print("=" * 60)

    try:
        generator = HierarchyRuleGenerator()

        # Test lookup
        for code in ["E11.65", "E11", "E"]:
            result = generator.find_applicable_rule(code, "guideline", "ICD-10")
            print(f"\n  {code} (guideline):")
            print(f"    Found: {result.found}")
            if result.found:
                print(f"    Pattern: {result.rule_pattern}")
                print(f"    Inheritance level: {result.inheritance_level}")
            print(f"    Hierarchy:")
            for level in result.hierarchy:
                g = "G" if level.has_guideline else "-"
                c = "C" if level.has_cms1500 else "-"
                print(f"      {level.pattern}: [{g}{c}] {level.pattern_type}")

    except Exception as e:
        print(f"  Error: {e}")


def ensure_table_exists():
    """Create or update rules_hierarchy table."""
    print("\n" + "=" * 60)
    print("Ensuring rules_hierarchy table exists...")
    print("=" * 60)

    try:
        from src.db.connection import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='rules_hierarchy'
        """)
        table_exists = cursor.fetchone() is not None

        if table_exists:
            # Check if columns exist
            cursor.execute("PRAGMA table_info(rules_hierarchy)")
            columns = [row[1] for row in cursor.fetchall()]

            if 'rule_type' not in columns:
                print("  Adding rule_type column...")
                cursor.execute("""
                    ALTER TABLE rules_hierarchy
                    ADD COLUMN rule_type TEXT NOT NULL DEFAULT 'guideline'
                """)
                print("  ✓ rule_type column added")

            if 'status' not in columns:
                print("  Adding status column...")
                cursor.execute("""
                    ALTER TABLE rules_hierarchy
                    ADD COLUMN status TEXT DEFAULT 'pending'
                """)
                print("  ✓ status column added")

            if 'created_at' not in columns:
                print("  Adding created_at column...")
                cursor.execute("""
                    ALTER TABLE rules_hierarchy
                    ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """)
                print("  ✓ created_at column added")
        else:
            # Create new table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rules_hierarchy (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pattern TEXT NOT NULL,
                    pattern_type TEXT NOT NULL,
                    code_type TEXT NOT NULL,
                    rule_type TEXT NOT NULL DEFAULT 'guideline',
                    parent_pattern TEXT,
                    rule_id INTEGER,
                    has_own_rule INTEGER DEFAULT 0,
                    inherits_from TEXT,
                    UNIQUE(code_type, pattern, rule_type)
                )
            """)
            print("  ✓ Table created")

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rules_hierarchy_pattern
            ON rules_hierarchy(pattern)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rules_hierarchy_rule_type
            ON rules_hierarchy(rule_type)
        """)

        conn.commit()
        print("  ✓ Table ready")

        # Check if table has data
        cursor.execute("SELECT COUNT(*) FROM rules_hierarchy")
        count = cursor.fetchone()[0]
        print(f"  Existing records: {count}")

        conn.close()
        return True

    except Exception as e:
        print(f"  Error: {e}")
        return False


if __name__ == "__main__":
    print("Hierarchy Rule Generator Tests")
    print("=" * 60)

    # Test 1: Utilities (no DB)
    test_utilities()

    # Ensure table exists before DB tests
    if ensure_table_exists():
        # Test 2: Planning
        test_planning()

        # Test 3: Document lookup
        test_document_lookup()

        # Test 4: Rule lookup
        test_rule_lookup()

    print("\n" + "=" * 60)
    print("Tests complete!")
    print("=" * 60)
