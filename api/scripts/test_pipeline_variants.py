#!/usr/bin/env python3
"""
Test script for comparing Gemini vs OpenAI vs Hybrid pipeline variants.

Runs the same code generation with different pipeline configs and JSON validators.
Collects timing and quality metrics for analysis.

Usage:
    python api/scripts/test_pipeline_variants.py E11 ICD-10
    python api/scripts/test_pipeline_variants.py E11 ICD-10 --gemini-only
    python api/scripts/test_pipeline_variants.py E11 ICD-10 --openai-only
    python api/scripts/test_pipeline_variants.py E11 ICD-10 --hybrid-only
"""

import asyncio
import argparse
import json
import time
import sys
import os
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.generators.hierarchy_rule_generator import HierarchyRuleGenerator
from src.generators.rule_generator import parse_json_safely


@dataclass
class StepMetrics:
    """Metrics for a single pipeline step."""
    step: str
    duration_ms: int
    output_chars: int
    is_json: bool = False
    verdict: str = ""
    corrections_count: int = 0
    approved_count: int = 0
    rejected_count: int = 0
    model: str = ""


@dataclass
class PipelineResult:
    """Full result of a pipeline run."""
    variant: str  # "gemini" or "openai"
    code: str
    code_type: str
    total_duration_sec: float
    steps: Dict[str, StepMetrics]
    final_output_chars: int
    success: bool
    error: str = ""
    timestamp: str = ""


def extract_model_from_debug(output_lines: List[str], step: str) -> str:
    """Extract model name from debug output."""
    for line in output_lines:
        if "Strategy:" in line and step in line.lower():
            # Extract model from "--- [DEBUG] Strategy: GOOGLE (gemini-2.5-flash) ---"
            if "(" in line and ")" in line:
                start = line.index("(") + 1
                end = line.index(")")
                return line[start:end]
    return ""


async def run_pipeline(
    code: str,
    code_type: str,
    variant: str,
    force_regenerate: bool = True
) -> PipelineResult:
    """Run pipeline with specified variant and collect metrics."""

    # Set environment variable for variant
    os.environ["PIPELINE_VARIANT"] = variant

    # Reload config
    import importlib
    import src.generators.core_ai as core_ai
    importlib.reload(core_ai)

    print(f"\n{'='*70}")
    print(f"PIPELINE VARIANT: {variant.upper()}")
    print(f"Code: {code} ({code_type})")
    print(f"{'='*70}\n")

    # For hybrid, we use the step-based config, so model param doesn't matter much
    # but we set it to "gemini" as the default for draft/finalization
    if variant == "hybrid":
        model = "gemini"  # Draft uses Gemini in hybrid
    elif variant == "gemini":
        model = "gemini"
    else:
        model = "gpt-4.1"

    generator = HierarchyRuleGenerator(
        thinking_budget=10000,
        model=model,
        json_validators=True
    )

    start_time = time.time()
    steps: Dict[str, StepMetrics] = {}
    step_start_times: Dict[str, float] = {}
    current_step = None
    final_output_chars = 0
    error_msg = ""
    debug_lines: List[str] = []

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

                # Track debug output for model detection
                if "Strategy:" in str(content):
                    debug_lines.append(content)

                # Track step timing
                if event_type == "status":
                    if step != current_step:
                        # End previous step
                        if current_step and current_step in step_start_times:
                            if current_step not in steps:
                                steps[current_step] = StepMetrics(
                                    step=current_step,
                                    duration_ms=0,
                                    output_chars=0
                                )
                        current_step = step
                        step_start_times[step] = time.time()
                    print(f"[{step}] {content}")

                elif event_type == "done":
                    full_text = event.get("full_text", "")
                    duration_ms = event.get("duration_ms", 0)

                    # If duration_ms is 0, calculate from start time
                    if duration_ms == 0 and step in step_start_times:
                        duration_ms = int((time.time() - step_start_times[step]) * 1000)

                    # Parse JSON output for validators
                    is_json = False
                    verdict = ""
                    corrections_count = 0
                    approved_count = 0
                    rejected_count = 0

                    if step in ("mentor", "redteam", "arbitration"):
                        parsed = parse_json_safely(full_text)
                        if parsed:
                            is_json = True
                            verdict = parsed.get("verdict", parsed.get("safety_status", ""))
                            corrections_count = len(parsed.get("corrections", []))
                            approved_count = len(parsed.get("approved_corrections", []))
                            rejected_count = len(parsed.get("rejected_corrections", []))

                    steps[step] = StepMetrics(
                        step=step,
                        duration_ms=duration_ms,
                        output_chars=len(full_text),
                        is_json=is_json,
                        verdict=verdict,
                        corrections_count=corrections_count,
                        approved_count=approved_count,
                        rejected_count=rejected_count
                    )

                    # Print summary
                    json_str = " [JSON]" if is_json else ""
                    verdict_str = f" verdict={verdict}" if verdict else ""
                    corr_str = ""
                    if corrections_count:
                        corr_str = f" corrections={corrections_count}"
                    if approved_count:
                        corr_str = f" approved={approved_count} rejected={rejected_count}"

                    print(f"[{step}] DONE {duration_ms}ms, {len(full_text)} chars{json_str}{verdict_str}{corr_str}")

                    if step == "finalization":
                        final_output_chars = len(full_text)

                elif event_type == "error":
                    print(f"[{step}] ERROR: {content}")
                    error_msg = content

            except json.JSONDecodeError:
                pass

    except Exception as e:
        error_msg = str(e)
        print(f"EXCEPTION: {e}")
        import traceback
        traceback.print_exc()

    total_duration = time.time() - start_time

    return PipelineResult(
        variant=variant,
        code=code,
        code_type=code_type,
        total_duration_sec=round(total_duration, 2),
        steps=steps,
        final_output_chars=final_output_chars,
        success=error_msg == "",
        error=error_msg,
        timestamp=datetime.now().isoformat()
    )


def print_comparison(results: List[PipelineResult]):
    """Print comparison table of results."""

    print(f"\n{'='*70}")
    print("COMPARISON SUMMARY")
    print(f"{'='*70}\n")

    # Header
    variants = [r.variant for r in results]
    print(f"{'Metric':<25} | " + " | ".join(f"{v:>15}" for v in variants))
    print("-" * (25 + 3 + 18 * len(variants)))

    # Total time
    print(f"{'Total time (sec)':<25} | " + " | ".join(f"{r.total_duration_sec:>15.1f}" for r in results))

    # Per-step times
    all_steps = ["draft", "mentor", "redteam", "arbitration", "finalization"]
    for step in all_steps:
        times = []
        for r in results:
            if step in r.steps:
                times.append(f"{r.steps[step].duration_ms / 1000:>15.1f}")
            else:
                times.append(f"{'N/A':>15}")
        print(f"{step + ' (sec)':<25} | " + " | ".join(times))

    print()

    # Validators JSON parse
    print(f"{'Mentor JSON':<25} | " + " | ".join(
        f"{'Yes' if r.steps.get('mentor', StepMetrics('', 0, 0)).is_json else 'No':>15}" for r in results
    ))
    print(f"{'RedTeam JSON':<25} | " + " | ".join(
        f"{'Yes' if r.steps.get('redteam', StepMetrics('', 0, 0)).is_json else 'No':>15}" for r in results
    ))
    print(f"{'Arbitration JSON':<25} | " + " | ".join(
        f"{'Yes' if r.steps.get('arbitration', StepMetrics('', 0, 0)).is_json else 'No':>15}" for r in results
    ))

    print()

    # Verdicts
    print(f"{'Mentor verdict':<25} | " + " | ".join(
        f"{r.steps.get('mentor', StepMetrics('', 0, 0)).verdict:>15}" for r in results
    ))
    print(f"{'RedTeam verdict':<25} | " + " | ".join(
        f"{r.steps.get('redteam', StepMetrics('', 0, 0)).verdict:>15}" for r in results
    ))
    print(f"{'Arbitration status':<25} | " + " | ".join(
        f"{r.steps.get('arbitration', StepMetrics('', 0, 0)).verdict:>15}" for r in results
    ))

    print()

    # Corrections
    print(f"{'Mentor corrections':<25} | " + " | ".join(
        f"{r.steps.get('mentor', StepMetrics('', 0, 0)).corrections_count:>15}" for r in results
    ))
    print(f"{'RedTeam corrections':<25} | " + " | ".join(
        f"{r.steps.get('redteam', StepMetrics('', 0, 0)).corrections_count:>15}" for r in results
    ))
    print(f"{'Approved corrections':<25} | " + " | ".join(
        f"{r.steps.get('arbitration', StepMetrics('', 0, 0)).approved_count:>15}" for r in results
    ))
    print(f"{'Rejected corrections':<25} | " + " | ".join(
        f"{r.steps.get('arbitration', StepMetrics('', 0, 0)).rejected_count:>15}" for r in results
    ))

    print()

    # Output sizes
    print(f"{'Final output (chars)':<25} | " + " | ".join(
        f"{r.final_output_chars:>15}" for r in results
    ))

    # Speed comparison
    if len(results) == 2:
        t1, t2 = results[0].total_duration_sec, results[1].total_duration_sec
        diff = t1 - t2
        faster = results[1].variant if diff > 0 else results[0].variant
        pct = abs(diff) / max(t1, t2) * 100
        print(f"\n{faster.upper()} is {pct:.1f}% faster ({abs(diff):.1f}s difference)")


def save_results(results: List[PipelineResult], output_path: str):
    """Save results to JSON file."""
    data = {
        "test_date": datetime.now().isoformat(),
        "results": []
    }

    for r in results:
        result_dict = {
            "variant": r.variant,
            "code": r.code,
            "code_type": r.code_type,
            "total_duration_sec": r.total_duration_sec,
            "final_output_chars": r.final_output_chars,
            "success": r.success,
            "error": r.error,
            "timestamp": r.timestamp,
            "steps": {}
        }
        for step_name, step_metrics in r.steps.items():
            result_dict["steps"][step_name] = asdict(step_metrics)
        data["results"].append(result_dict)

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\nResults saved to: {output_path}")


async def main():
    parser = argparse.ArgumentParser(description="Test Gemini vs OpenAI vs Hybrid pipeline variants")
    parser.add_argument("code", help="Code to generate (e.g., E11, E11.9)")
    parser.add_argument("code_type", nargs="?", default="ICD-10", help="Code type (default: ICD-10)")
    parser.add_argument("--gemini-only", action="store_true", help="Only run Gemini variant")
    parser.add_argument("--openai-only", action="store_true", help="Only run OpenAI variant")
    parser.add_argument("--hybrid-only", action="store_true", help="Only run Hybrid variant")
    parser.add_argument("--no-force", action="store_true", help="Don't force regenerate if exists")
    parser.add_argument("--output", "-o", help="Output JSON file path")

    args = parser.parse_args()

    results: List[PipelineResult] = []
    force = not args.no_force

    # Determine which variants to run
    run_gemini = not (args.openai_only or args.hybrid_only)
    run_openai = not (args.gemini_only or args.hybrid_only)
    run_hybrid = args.hybrid_only or not (args.gemini_only or args.openai_only)

    # If specific variant requested, only run that one
    if args.gemini_only:
        run_gemini, run_openai, run_hybrid = True, False, False
    elif args.openai_only:
        run_gemini, run_openai, run_hybrid = False, True, False
    elif args.hybrid_only:
        run_gemini, run_openai, run_hybrid = False, False, True

    # Determine output file
    if args.output:
        output_path = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"data/test_results/pipeline_comparison_{args.code}_{timestamp}.json"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Run Gemini variant
    if run_gemini:
        result_gemini = await run_pipeline(args.code, args.code_type, "gemini", force)
        results.append(result_gemini)

    # Run OpenAI variant
    if run_openai:
        result_openai = await run_pipeline(args.code, args.code_type, "openai", force)
        results.append(result_openai)

    # Run Hybrid variant
    if run_hybrid:
        result_hybrid = await run_pipeline(args.code, args.code_type, "hybrid", force)
        results.append(result_hybrid)

    # Print comparison
    if len(results) > 1:
        print_comparison(results)
    elif len(results) == 1:
        r = results[0]
        print(f"\n{'='*70}")
        print(f"RESULTS: {r.variant.upper()}")
        print(f"{'='*70}")
        print(f"Total time: {r.total_duration_sec}s")
        print(f"Success: {r.success}")
        if r.error:
            print(f"Error: {r.error}")
        print(f"\nSteps:")
        for step_name, step in r.steps.items():
            print(f"  {step_name}: {step.duration_ms}ms, {step.output_chars} chars")

    # Save results
    save_results(results, output_path)


if __name__ == "__main__":
    asyncio.run(main())
