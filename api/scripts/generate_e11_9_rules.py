#!/usr/bin/env python3
"""
Generate rules for ICD-10 codes with hierarchy cascade.

Usage:
    python api/scripts/generate_e11_9_rules.py --code E00.2 --force
    python api/scripts/generate_e11_9_rules.py --code E11.9 --guideline-only
    python api/scripts/generate_e11_9_rules.py --code E11.65 --cms-only
"""

import sys
import os
import json
import asyncio
import argparse
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.generators.hierarchy_rule_generator import HierarchyRuleGenerator
from src.db.connection import get_db_connection

REPORTS_DIR = Path(__file__).parent.parent.parent / "data" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def reset_rule_status(code: str, rule_type: str = "guideline", reset_parents: bool = False):
    """Reset rule status to allow regeneration.

    Args:
        code: Target code (e.g., E00.2)
        rule_type: 'guideline' or 'cms1500'
        reset_parents: If True, reset entire hierarchy. If False, reset only target code.
    """
    from src.generators.hierarchy_rule_generator import get_hierarchy_patterns

    conn = get_db_connection()
    cursor = conn.cursor()

    if reset_parents:
        patterns = get_hierarchy_patterns(code, "ICD-10")
    else:
        patterns = [code.upper()]

    for pattern in patterns:
        cursor.execute("""
            UPDATE rules_hierarchy
            SET status = 'pending', has_own_rule = 0
            WHERE pattern = ? AND rule_type = ?
        """, (pattern, rule_type))
        if cursor.rowcount > 0:
            print(f"  Reset {pattern} [{rule_type}]", flush=True)

    conn.commit()
    conn.close()


def check_db_before(code: str):
    """Check DB state before generation."""
    from src.generators.hierarchy_rule_generator import get_hierarchy_patterns

    print("\n=== DB STATE BEFORE ===", flush=True)
    conn = get_db_connection()
    cursor = conn.cursor()

    patterns = get_hierarchy_patterns(code, "ICD-10")
    placeholders = ','.join('?' * len(patterns))

    cursor.execute(f"""
        SELECT pattern, rule_type, has_own_rule, status
        FROM rules_hierarchy
        WHERE pattern IN ({placeholders})
        ORDER BY LENGTH(pattern)
    """, patterns)
    rows = cursor.fetchall()

    if rows:
        print("rules_hierarchy:")
        for r in rows:
            print(f"  {r[0]} [{r[1]}]: own={r[2]}, status={r[3]}")
    else:
        print("rules_hierarchy: empty (good - clean test)")

    conn.close()


def check_db_after(code: str):
    """Check DB state after generation."""
    from src.generators.hierarchy_rule_generator import get_hierarchy_patterns

    print("\n=== DB STATE AFTER ===", flush=True)
    conn = get_db_connection()
    cursor = conn.cursor()

    patterns = get_hierarchy_patterns(code, "ICD-10")
    placeholders = ','.join('?' * len(patterns))

    # rules_hierarchy
    cursor.execute(f"""
        SELECT pattern, rule_type, has_own_rule, rule_id, status
        FROM rules_hierarchy
        WHERE pattern IN ({placeholders})
        ORDER BY LENGTH(pattern), rule_type
    """, patterns)
    rows = cursor.fetchall()

    print("\nrules_hierarchy:")
    for r in rows:
        print(f"  {r[0]} [{r[1]}]: own={r[2]}, rule_id={r[3]}, status={r[4]}")

    # rules
    cursor.execute(f"""
        SELECT id, code, rule_level, status, rule_path
        FROM rules
        WHERE code IN ({placeholders})
        ORDER BY id
    """, patterns)
    rows = cursor.fetchall()

    print("\nrules:")
    for r in rows:
        print(f"  [{r[0]}] {r[1]}: level={r[2]}, status={r[3]}")
        if r[4]:
            print(f"       path: {r[4]}")

    conn.close()


async def generate_guidelines(code: str, code_type: str = "ICD-10", force: bool = False):
    """Generate guideline rules with cascade."""
    print(f"\n{'='*60}", flush=True)
    print(f"GENERATING GUIDELINE: {code}" + (" (FORCE)" if force else ""), flush=True)
    print(f"{'='*60}", flush=True)

    generator = HierarchyRuleGenerator()

    # Show plan
    plan = generator.plan_guideline_generation(code, code_type)
    print(f"Plan: generate {plan.patterns_to_generate}", flush=True)
    print(f"Existing: {plan.existing_patterns}", flush=True)

    events = []
    current_step = ""
    step_events = 0
    async for event_json in generator.generate_guideline(code, code_type, force_regenerate=force):
        event = json.loads(event_json)
        events.append(event)

        etype = event.get("type", "")
        step = event.get("step", "")
        content = event.get("content", "")
        pattern = event.get("pattern", "")

        # Track step progress
        if step and step != current_step:
            if current_step:
                print(f"  [{current_step}] completed ({step_events} events)", flush=True)
            current_step = step
            step_events = 0
        step_events += 1

        if etype == "plan":
            print(f"\n[PLAN] {content}", flush=True)
        elif etype == "generating":
            print(f"\n[{pattern}] Generating...", flush=True)
            if event.get("parent_rule"):
                print(f"  Parent: {event.get('parent_rule')}", flush=True)
        elif etype == "documents":
            print(f"  Documents: {content}", flush=True)
        elif etype == "status":
            print(f"  [{step}] {content}", flush=True)
        elif etype == "done":
            print(f"  [{step}] ✓ Done", flush=True)
        elif etype == "registered":
            print(f"  ✅ Registered rule_id={event.get('rule_id')}", flush=True)
        elif etype == "complete":
            print(f"\n[COMPLETE] {content}", flush=True)
        elif etype == "error":
            print(f"\n❌ ERROR: {content}", flush=True)
        elif etype == "exists":
            print(f"\n[EXISTS] {content}", flush=True)

    return events


async def generate_cms1500(code: str, code_type: str = "ICD-10", force: bool = False):
    """Generate CMS-1500 rules with cascade."""
    print(f"\n{'='*60}", flush=True)
    print(f"GENERATING CMS-1500: {code}" + (" (FORCE)" if force else ""), flush=True)
    print(f"{'='*60}", flush=True)

    generator = HierarchyRuleGenerator()

    # Show plan
    plan = generator.plan_cms1500_generation(code, code_type)
    print(f"Plan: generate {plan.patterns_to_generate}", flush=True)
    print(f"Prerequisite met: {plan.prerequisite_met}", flush=True)
    if not plan.prerequisite_met:
        print(f"Error: {plan.prerequisite_error}", flush=True)
        return []

    events = []
    current_step = ""
    step_events = 0
    async for event_json in generator.generate_cms1500(code, code_type, force_regenerate=force):
        event = json.loads(event_json)
        events.append(event)

        etype = event.get("type", "")
        step = event.get("step", "")
        content = event.get("content", "")
        pattern = event.get("pattern", "")

        # Track step progress
        if step and step != current_step:
            if current_step:
                print(f"  [{current_step}] completed ({step_events} events)", flush=True)
            current_step = step
            step_events = 0
        step_events += 1

        if etype == "plan":
            print(f"\n[PLAN] {content}", flush=True)
        elif etype == "generating":
            print(f"\n[{pattern}] Generating CMS-1500...", flush=True)
        elif etype == "documents":
            print(f"  Documents: {content}", flush=True)
        elif etype == "status":
            print(f"  [{step}] {content}", flush=True)
        elif etype == "done":
            print(f"  [{step}] ✓ Done", flush=True)
        elif etype == "registered":
            print(f"  ✅ Registered rule_id={event.get('rule_id')}", flush=True)
        elif etype == "complete":
            print(f"\n[COMPLETE] {content}", flush=True)
        elif etype == "error":
            print(f"\n❌ ERROR: {content}", flush=True)

    return events


async def main():
    parser = argparse.ArgumentParser(description="Generate rules for ICD-10 codes with hierarchy cascade")
    parser.add_argument("--code", type=str, default="E11.9", help="ICD-10 code to generate rules for")
    parser.add_argument("--force", action="store_true", help="Force regeneration of target code only")
    parser.add_argument("--force-all", action="store_true", help="Force regeneration of entire hierarchy (E, E00, E00.2)")
    parser.add_argument("--guideline-only", action="store_true", help="Generate only guideline rules")
    parser.add_argument("--cms-only", action="store_true", help="Generate only CMS-1500 rules")
    args = parser.parse_args()

    code = args.code.upper()
    code_type = "ICD-10"

    force = args.force or args.force_all
    reset_parents = args.force_all

    print("="*60)
    print(f"RULE GENERATION: {code}")
    print("="*60)
    print(f"Started: {datetime.now().isoformat()}", flush=True)
    print(f"Force: {force}" + (" (entire hierarchy)" if reset_parents else " (target only)" if force else ""), flush=True)
    print(f"Guideline only: {args.guideline_only}", flush=True)
    print(f"CMS only: {args.cms_only}", flush=True)

    # Reset status if force
    if force:
        print("\n=== RESETTING STATUS ===", flush=True)
        if not args.cms_only:
            reset_rule_status(code, "guideline", reset_parents=reset_parents)
        if not args.guideline_only:
            reset_rule_status(code, "cms1500", reset_parents=reset_parents)

    # Check before
    check_db_before(code)

    results = {
        "code": code,
        "code_type": code_type,
        "force": force,
        "reset_parents": reset_parents,
        "started_at": datetime.now().isoformat(),
        "guideline_events": [],
        "cms1500_events": []
    }

    # Step 1: Generate guidelines
    if not args.cms_only:
        guideline_events = await generate_guidelines(code, code_type, force=force)
        results["guideline_events"] = guideline_events

    # Step 2: Generate CMS-1500 (requires guideline)
    if not args.guideline_only:
        cms1500_events = await generate_cms1500(code, code_type, force=force)
        results["cms1500_events"] = cms1500_events

    results["finished_at"] = datetime.now().isoformat()

    # Check after
    check_db_after(code)

    # Save report
    code_safe = code.replace(".", "_")
    report_path = REPORTS_DIR / f"{code_safe}_rules_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)

    print(f"\n✅ Report saved: {report_path}", flush=True)
    print(f"\nFinished: {datetime.now().isoformat()}")


if __name__ == "__main__":
    asyncio.run(main())
