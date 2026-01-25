#!/usr/bin/env python3
"""
Generate rules for E11.9 with cascade: E → E11 → E11.9

Test script for hierarchy_rule_generator.py
"""

import sys
import os
import json
import asyncio
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Use GPT-4.1 instead of Gemini (Gemini was hanging)
os.environ["AI_PROVIDER"] = "openai"
os.environ["OPENAI_MODEL"] = "gpt-4.1"

from src.generators.hierarchy_rule_generator import HierarchyRuleGenerator
from src.db.connection import get_db_connection

REPORTS_DIR = Path(__file__).parent.parent.parent / "data" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def check_db_before():
    """Check DB state before generation."""
    print("\n=== DB STATE BEFORE ===", flush=True)
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT pattern, rule_type, has_own_rule, status
        FROM rules_hierarchy
        WHERE pattern IN ('E', 'E11', 'E11.9')
        ORDER BY LENGTH(pattern)
    """)
    rows = cursor.fetchall()

    if rows:
        print("rules_hierarchy:")
        for r in rows:
            print(f"  {r[0]} [{r[1]}]: own={r[2]}, status={r[3]}")
    else:
        print("rules_hierarchy: empty (good - clean test)")

    conn.close()


def check_db_after():
    """Check DB state after generation."""
    print("\n=== DB STATE AFTER ===", flush=True)
    conn = get_db_connection()
    cursor = conn.cursor()

    # rules_hierarchy
    cursor.execute("""
        SELECT pattern, rule_type, has_own_rule, rule_id, status
        FROM rules_hierarchy
        WHERE pattern IN ('E', 'E11', 'E11.9')
        ORDER BY LENGTH(pattern), rule_type
    """)
    rows = cursor.fetchall()

    print("\nrules_hierarchy:")
    for r in rows:
        print(f"  {r[0]} [{r[1]}]: own={r[2]}, rule_id={r[3]}, status={r[4]}")

    # rules
    cursor.execute("""
        SELECT id, code, rule_level, status, rule_path
        FROM rules
        WHERE code IN ('E', 'E11', 'E11.9')
        ORDER BY id
    """)
    rows = cursor.fetchall()

    print("\nrules:")
    for r in rows:
        print(f"  [{r[0]}] {r[1]}: level={r[2]}, status={r[3]}")
        if r[4]:
            print(f"       path: {r[4]}")

    conn.close()


async def generate_guidelines(code: str, code_type: str = "ICD-10"):
    """Generate guideline rules with cascade."""
    print(f"\n{'='*60}", flush=True)
    print(f"GENERATING GUIDELINE: {code}", flush=True)
    print(f"{'='*60}", flush=True)

    generator = HierarchyRuleGenerator()

    # Show plan
    plan = generator.plan_guideline_generation(code, code_type)
    print(f"Plan: generate {plan.patterns_to_generate}", flush=True)
    print(f"Existing: {plan.existing_patterns}", flush=True)

    events = []
    current_step = ""
    step_events = 0
    async for event_json in generator.generate_guideline(code, code_type):
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


async def generate_cms1500(code: str, code_type: str = "ICD-10"):
    """Generate CMS-1500 rules with cascade."""
    print(f"\n{'='*60}", flush=True)
    print(f"GENERATING CMS-1500: {code}", flush=True)
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
    async for event_json in generator.generate_cms1500(code, code_type):
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
    print("="*60)
    print("E11.9 RULE GENERATION TEST")
    print("="*60)
    print(f"Started: {datetime.now().isoformat()}", flush=True)

    code = "E11.9"
    code_type = "ICD-10"

    # Check before
    check_db_before()

    results = {
        "code": code,
        "code_type": code_type,
        "started_at": datetime.now().isoformat(),
        "guideline_events": [],
        "cms1500_events": []
    }

    # Step 1: Generate guidelines (E → E11 → E11.9)
    guideline_events = await generate_guidelines(code, code_type)
    results["guideline_events"] = guideline_events

    # Step 2: Generate CMS-1500 (requires guideline)
    cms1500_events = await generate_cms1500(code, code_type)
    results["cms1500_events"] = cms1500_events

    results["finished_at"] = datetime.now().isoformat()

    # Check after
    check_db_after()

    # Save report
    report_path = REPORTS_DIR / f"e11_9_rules_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)

    print(f"\n✅ Report saved: {report_path}", flush=True)
    print(f"\nFinished: {datetime.now().isoformat()}")


if __name__ == "__main__":
    asyncio.run(main())
