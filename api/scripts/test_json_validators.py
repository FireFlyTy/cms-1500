#!/usr/bin/env python3
"""
Test script for JSON validators mode.

Compares generation with json_validators=True vs json_validators=False.

Usage:
    python api/scripts/test_json_validators.py E ICD-10
    python api/scripts/test_json_validators.py E ICD-10 --json-only
    python api/scripts/test_json_validators.py E ICD-10 --markdown-only
"""

import asyncio
import argparse
import json
import time
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.generators.hierarchy_rule_generator import HierarchyRuleGenerator


async def run_generation(code: str, code_type: str, json_validators: bool, force_regenerate: bool = True):
    """Run generation and collect metrics."""

    mode = "JSON" if json_validators else "Markdown"
    print(f"\n{'='*60}")
    print(f"Running {mode} mode for {code} ({code_type})")
    print(f"{'='*60}\n")

    generator = HierarchyRuleGenerator(
        thinking_budget=10000,
        model="gemini",  # или "gpt-4.1-mini"
        json_validators=json_validators
    )

    start_time = time.time()

    step_times = {}
    step_outputs = {}
    current_step = None
    step_start = None

    try:
        async for event_json in generator.generate_guideline(
            code=code,
            code_type=code_type,
            force_regenerate=force_regenerate
        ):
            try:
                event = json.loads(event_json)
                step = event.get("step", "unknown")
                event_type = event.get("type", "")
                content = event.get("content", "")

                # Track step timing
                if event_type == "status" and step != current_step:
                    if current_step and step_start:
                        step_times[current_step] = time.time() - step_start
                    current_step = step
                    step_start = time.time()
                    print(f"[{step}] {content}")

                elif event_type == "done":
                    if step_start:
                        step_times[step] = time.time() - step_start
                    duration = event.get("duration_ms", 0)
                    full_text = event.get("full_text", "")
                    step_outputs[step] = full_text
                    print(f"[{step}] DONE - {duration}ms, {len(full_text)} chars")

                    # For JSON mode, try to parse and show structure
                    if json_validators and step in ("mentor", "redteam", "arbitration"):
                        try:
                            parsed = json.loads(full_text)
                            if step == "mentor":
                                verdict = parsed.get("verdict", "?")
                                corrections = len(parsed.get("corrections", []))
                                print(f"    → Verdict: {verdict}, Corrections: {corrections}")
                            elif step == "redteam":
                                verdict = parsed.get("verdict", "?")
                                risks = parsed.get("risks_found", 0)
                                corrections = len(parsed.get("corrections", []))
                                print(f"    → Verdict: {verdict}, Risks: {risks}, Corrections: {corrections}")
                            elif step == "arbitration":
                                safety = parsed.get("safety_status", "?")
                                approved = len(parsed.get("approved_corrections", []))
                                rejected = len(parsed.get("rejected_corrections", []))
                                print(f"    → Safety: {safety}, Approved: {approved}, Rejected: {rejected}")
                        except json.JSONDecodeError:
                            print(f"    → WARNING: Invalid JSON output!")

                elif event_type == "error":
                    print(f"[{step}] ERROR: {content}")

                elif event_type == "thought":
                    # Show thinking preview
                    preview = content[:100] + "..." if len(content) > 100 else content
                    print(f"[{step}] thinking: {preview}")

            except json.JSONDecodeError:
                pass

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    total_time = time.time() - start_time

    return {
        "mode": mode,
        "code": code,
        "total_time_sec": round(total_time, 2),
        "step_times": {k: round(v, 2) for k, v in step_times.items()},
        "step_output_sizes": {k: len(v) for k, v in step_outputs.items()},
    }


async def main():
    parser = argparse.ArgumentParser(description="Test JSON validators mode")
    parser.add_argument("code", help="Code to generate (e.g., E, E11, E11.9)")
    parser.add_argument("code_type", nargs="?", default="ICD-10", help="Code type (default: ICD-10)")
    parser.add_argument("--json-only", action="store_true", help="Only run JSON mode")
    parser.add_argument("--markdown-only", action="store_true", help="Only run Markdown mode")
    parser.add_argument("--no-force", action="store_true", help="Don't force regenerate if exists")

    args = parser.parse_args()

    results = []
    force = not args.no_force

    if not args.json_only:
        # Run Markdown mode first
        result_md = await run_generation(args.code, args.code_type, json_validators=False, force_regenerate=force)
        results.append(result_md)

    if not args.markdown_only:
        # Run JSON mode
        result_json = await run_generation(args.code, args.code_type, json_validators=True, force_regenerate=force)
        results.append(result_json)

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}\n")

    for r in results:
        print(f"{r['mode']} Mode:")
        print(f"  Total time: {r['total_time_sec']}s")
        print(f"  Step times: {r['step_times']}")
        print(f"  Output sizes: {r['step_output_sizes']}")
        print()

    if len(results) == 2:
        md_time = results[0]["total_time_sec"]
        json_time = results[1]["total_time_sec"]
        diff = md_time - json_time
        pct = (diff / md_time * 100) if md_time > 0 else 0
        print(f"Time difference: {diff:.2f}s ({pct:.1f}% {'faster' if diff > 0 else 'slower'} with JSON)")


if __name__ == "__main__":
    asyncio.run(main())
