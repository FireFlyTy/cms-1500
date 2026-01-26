#!/usr/bin/env python3
"""
Rule Quality Checker

Validates generated rules against ICD-10-CM Guidelines checklist.
Calculates quality score and identifies missing coverage.

Usage:
    python api/scripts/check_rule_quality.py data/processed/rules/E11/v12/rule.md
    python api/scripts/check_rule_quality.py E11 --all-versions
"""

import argparse
import re
import json
import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional


@dataclass
class CoverageRule:
    """Definition of a coverage check rule."""
    name: str
    description: str
    pattern: str
    source: str
    weight: int = 10  # Points for this rule
    critical: bool = False  # If True, missing = automatic fail


# E11 (Type 2 Diabetes) Coverage Rules from ICD-10-CM Guidelines
E11_COVERAGE_RULES = [
    CoverageRule(
        name="default_e11",
        description="E11 is default when diabetes type not documented",
        pattern=r"default.*E11|not documented.*E11|type.*not.*documented.*E11",
        source="ICD-10-CM Guidelines p.40",
        weight=15,
        critical=True
    ),
    CoverageRule(
        name="multiple_codes",
        description="Use multiple codes for complications",
        pattern=r"as many codes|multiple codes.*complications|codes.*needed.*complications",
        source="ICD-10-CM Guidelines p.39",
        weight=10
    ),
    CoverageRule(
        name="z79_long_term",
        description="Z79 codes for long-term medication use",
        pattern=r"Z79\.\d|Z79\.4|Z79\.84|Z79\.85|long-term.*use.*insulin",
        source="ICD-10-CM Guidelines p.40",
        weight=15,
        critical=True
    ),
    CoverageRule(
        name="z79_temporary_exclusion",
        description="Z79.4 NOT for temporary insulin",
        pattern=r"temporarily.*not.*assign|not.*temporary.*insulin|Z79\.4.*should not.*temporary",
        source="ICD-10-CM Guidelines p.40",
        weight=15,
        critical=True
    ),
    CoverageRule(
        name="o24_pregnancy_first",
        description="O24 sequenced first for pregnancy + diabetes",
        pattern=r"O24.*first|pregnancy.*O24|pregnant.*O24|O24.*primary",
        source="ICD-10-CM Guidelines p.67",
        weight=10
    ),
    CoverageRule(
        name="o24_excludes_z79",
        description="O24.4 gestational excludes Z79 codes",
        pattern=r"O24\.4.*should not.*Z79|O24\.4.*exclude.*Z79|gestational.*not.*Z79",
        source="ICD-10-CM Guidelines p.67",
        weight=10
    ),
    CoverageRule(
        name="insulin_pump",
        description="Insulin pump malfunction coding (T85.6 + T38.3X6)",
        pattern=r"insulin pump|T85\.6|T38\.3X6|pump.*malfunction",
        source="ICD-10-CM Guidelines p.41",
        weight=5
    ),
    CoverageRule(
        name="secondary_diabetes",
        description="Secondary diabetes scope (E08/E09/E13)",
        pattern=r"secondary diabetes|E08|E09|E13|underlying condition",
        source="ICD-10-CM Guidelines p.41-42",
        weight=5
    ),
    CoverageRule(
        name="ckd_esrd_rule",
        description="CKD + ESRD = only N18.6",
        pattern=r"N18\.6|ESRD.*CKD|CKD.*ESRD|end.stage.*renal",
        source="ICD-10-CM Guidelines p.62",
        weight=5
    ),
    CoverageRule(
        name="remission_codes",
        description="E11.A remission codes",
        pattern=r"remission|E11\.A|without complication.*remission",
        source="ICD-10-CM Guidelines p.40",
        weight=5
    ),
    CoverageRule(
        name="sequencing_encounter",
        description="Sequence codes by encounter reason",
        pattern=r"sequenced.*encounter|encounter.*sequence|reason.*encounter.*sequence",
        source="ICD-10-CM Guidelines p.39",
        weight=10
    ),
]


@dataclass
class ErrorCheck:
    """Definition of an error check."""
    name: str
    description: str
    check_func: str  # Name of check function
    severity: str = "error"  # error, warning
    penalty: int = 20  # Points deducted


ERROR_CHECKS = [
    ErrorCheck(
        name="code_type_mismatch",
        description="Wrong code type (e.g., E10 in E11 rule)",
        check_func="check_code_type_mismatch",
        severity="error",
        penalty=30
    ),
    ErrorCheck(
        name="stitched_citations",
        description="Citations with ellipsis (non-continuous)",
        check_func="check_stitched_citations",
        severity="warning",
        penalty=5
    ),
    ErrorCheck(
        name="missing_sections",
        description="Missing required sections",
        check_func="check_missing_sections",
        severity="error",
        penalty=15
    ),
]


@dataclass
class QualityResult:
    """Result of quality check."""
    file_path: str
    code: str
    version: str

    # Coverage
    coverage_checks: Dict[str, bool] = field(default_factory=dict)
    coverage_score: int = 0
    coverage_max: int = 0

    # Errors
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    error_penalty: int = 0

    # Structure
    structure_checks: Dict[str, bool] = field(default_factory=dict)
    structure_score: int = 0

    # Citations
    total_citations: int = 0
    unique_docs: int = 0
    unique_pages: int = 0
    citation_variance: str = ""
    citation_score: int = 0

    # Final
    total_score: int = 0
    max_score: int = 100
    grade: str = ""

    def to_dict(self) -> dict:
        return {
            "file_path": self.file_path,
            "code": self.code,
            "version": self.version,
            "coverage": {
                "checks": self.coverage_checks,
                "score": self.coverage_score,
                "max": self.coverage_max
            },
            "errors": self.errors,
            "warnings": self.warnings,
            "error_penalty": self.error_penalty,
            "structure": {
                "checks": self.structure_checks,
                "score": self.structure_score
            },
            "citations": {
                "total": self.total_citations,
                "unique_docs": self.unique_docs,
                "unique_pages": self.unique_pages,
                "variance": self.citation_variance,
                "score": self.citation_score
            },
            "total_score": self.total_score,
            "max_score": self.max_score,
            "grade": self.grade
        }


def check_code_type_mismatch(rule_text: str, code: str) -> List[str]:
    """Check for wrong code types in rule."""
    errors = []

    if code.startswith("E11"):
        # E11 (Type 2) should not mention E10 (Type 1) codes
        e10_matches = re.findall(r'E10\.[A-Z0-9-]+', rule_text)
        if e10_matches:
            errors.append(f"E10 codes found in E11 rule: {', '.join(set(e10_matches))}")

    elif code.startswith("E10"):
        # E10 (Type 1) should not mention E11 (Type 2) as instructions
        # (mentioning for differentiation is OK)
        if re.search(r'assign.*E11|use.*E11.*code', rule_text, re.I):
            errors.append("E11 assignment instructions found in E10 rule")

    return errors


def check_stitched_citations(rule_text: str, code: str) -> List[str]:
    """Check for stitched citations with ellipsis."""
    warnings = []

    # Find citations with ... in the anchor
    stitched = re.findall(r'\[\[[^\]]+\|[^\]]*\.\.\.[^\]]*\]\]', rule_text)
    if stitched:
        warnings.append(f"Found {len(stitched)} stitched citations with ellipsis")

    return warnings


def check_missing_sections(rule_text: str, code: str) -> List[str]:
    """Check for missing required sections."""
    errors = []

    required_sections = [
        ("SUMMARY", r"##\s*\d*\.?\s*SUMMARY"),
        ("CRITERIA", r"##\s*\d*\.?\s*CRITERIA|INCLUSION|EXCLUSION"),
        ("INSTRUCTIONS", r"##\s*\d*\.?\s*INSTRUCTIONS|\*\*IF\*\*"),
        ("REFERENCE", r"##\s*\d*\.?\s*REFERENCE"),
    ]

    for section_name, pattern in required_sections:
        if not re.search(pattern, rule_text, re.I):
            errors.append(f"Missing required section: {section_name}")

    return errors


def analyze_citations(rule_text: str) -> Tuple[int, int, int, str]:
    """Analyze citation quality."""
    # Find all citations: [[doc_id:page | "text"]]
    citations = re.findall(r'\[\[([a-f0-9]+):(\d+)', rule_text)

    total = len(citations)
    unique_docs = len(set(c[0] for c in citations))
    unique_pages = len(set(f"{c[0]}:{c[1]}" for c in citations))

    # Determine variance level
    if unique_pages >= 15 and unique_docs >= 4:
        variance = "HIGH"
    elif unique_pages >= 8 and unique_docs >= 3:
        variance = "MEDIUM"
    else:
        variance = "LOW"

    return total, unique_docs, unique_pages, variance


def analyze_structure(rule_text: str) -> Dict[str, bool]:
    """Analyze document structure."""
    checks = {
        "has_summary": bool(re.search(r"##\s*\d*\.?\s*SUMMARY", rule_text, re.I)),
        "has_inclusion": "INCLUSION" in rule_text.upper(),
        "has_exclusion": "EXCLUSION" in rule_text.upper(),
        "has_instructions": rule_text.count("**IF**") >= 3 or rule_text.count("IF ") >= 5,
        "has_reference": bool(re.search(r"##\s*\d*\.?\s*REFERENCE", rule_text, re.I)),
        "has_source_log": "SOURCE" in rule_text.upper() and "LOG" in rule_text.upper(),
        "has_self_check": "SELF-CHECK" in rule_text.upper() or "SELF CHECK" in rule_text.upper(),
    }
    return checks


def calculate_grade(score: int) -> str:
    """Calculate letter grade from score."""
    if score >= 95:
        return "A+"
    elif score >= 90:
        return "A"
    elif score >= 85:
        return "A-"
    elif score >= 80:
        return "B+"
    elif score >= 75:
        return "B"
    elif score >= 70:
        return "B-"
    elif score >= 65:
        return "C+"
    elif score >= 60:
        return "C"
    elif score >= 55:
        return "C-"
    elif score >= 50:
        return "D"
    else:
        return "F"


def check_rule_quality(
    rule_text: str,
    code: str,
    file_path: str = "",
    version: str = ""
) -> QualityResult:
    """
    Check rule quality against checklist.

    Args:
        rule_text: The rule markdown content
        code: The code this rule is for (e.g., "E11")
        file_path: Path to the rule file
        version: Version string

    Returns:
        QualityResult with all checks and scores
    """
    result = QualityResult(
        file_path=file_path,
        code=code,
        version=version
    )

    # 1. COVERAGE CHECKS (50 points max)
    coverage_rules = E11_COVERAGE_RULES if code.startswith("E11") else E11_COVERAGE_RULES

    for rule in coverage_rules:
        matched = bool(re.search(rule.pattern, rule_text, re.I))
        result.coverage_checks[rule.name] = matched
        result.coverage_max += rule.weight
        if matched:
            result.coverage_score += rule.weight
        elif rule.critical:
            result.errors.append(f"CRITICAL: Missing {rule.description}")

    # Normalize to 50 points
    if result.coverage_max > 0:
        result.coverage_score = int(50 * result.coverage_score / result.coverage_max)
    result.coverage_max = 50

    # 2. ERROR CHECKS (up to -30 penalty)
    errors_found = check_code_type_mismatch(rule_text, code)
    for err in errors_found:
        result.errors.append(err)
        result.error_penalty += 30

    warnings_found = check_stitched_citations(rule_text, code)
    for warn in warnings_found:
        result.warnings.append(warn)
        result.error_penalty += 5

    section_errors = check_missing_sections(rule_text, code)
    for err in section_errors:
        result.errors.append(err)
        result.error_penalty += 15

    # 3. STRUCTURE CHECKS (25 points max)
    result.structure_checks = analyze_structure(rule_text)
    structure_points = sum(5 if v else 0 for v in result.structure_checks.values())
    result.structure_score = min(25, structure_points)

    # 4. CITATION CHECKS (25 points max)
    total, unique_docs, unique_pages, variance = analyze_citations(rule_text)
    result.total_citations = total
    result.unique_docs = unique_docs
    result.unique_pages = unique_pages
    result.citation_variance = variance

    # Citation scoring
    if variance == "HIGH":
        result.citation_score = 25
    elif variance == "MEDIUM":
        result.citation_score = 18
    else:
        result.citation_score = 10

    # Bonus for high citation count
    if total >= 20:
        result.citation_score = min(25, result.citation_score + 5)

    # 5. CALCULATE TOTAL
    result.total_score = (
        result.coverage_score +
        result.structure_score +
        result.citation_score -
        result.error_penalty
    )
    result.total_score = max(0, min(100, result.total_score))
    result.grade = calculate_grade(result.total_score)

    return result


def print_result(result: QualityResult, verbose: bool = True):
    """Print quality check result."""
    print(f"\n{'='*60}")
    print(f"RULE QUALITY CHECK: {result.code} (v{result.version})")
    print(f"{'='*60}")

    # Coverage
    print(f"\nCOVERAGE ({result.coverage_score}/{result.coverage_max} points):")
    for name, passed in result.coverage_checks.items():
        status = "✅" if passed else "❌"
        print(f"  {status} {name}")

    # Errors
    if result.errors:
        print(f"\nERRORS ({len(result.errors)}):")
        for err in result.errors:
            print(f"  ❌ {err}")

    if result.warnings:
        print(f"\nWARNINGS ({len(result.warnings)}):")
        for warn in result.warnings:
            print(f"  ⚠️  {warn}")

    # Structure
    print(f"\nSTRUCTURE ({result.structure_score}/25 points):")
    for name, passed in result.structure_checks.items():
        status = "✅" if passed else "❌"
        print(f"  {status} {name}")

    # Citations
    print(f"\nCITATIONS ({result.citation_score}/25 points):")
    print(f"  Total: {result.total_citations}")
    print(f"  Unique docs: {result.unique_docs}")
    print(f"  Unique pages: {result.unique_pages}")
    print(f"  Variance: {result.citation_variance}")

    # Final score
    print(f"\n{'='*60}")
    print(f"FINAL SCORE: {result.total_score}/100 ({result.grade})")
    if result.error_penalty > 0:
        print(f"  (includes -{result.error_penalty} error penalty)")
    print(f"{'='*60}\n")


def check_all_versions(code: str, rules_dir: str = "data/processed/rules") -> List[QualityResult]:
    """Check all versions of a rule."""
    results = []
    code_dir = Path(rules_dir) / code

    if not code_dir.exists():
        print(f"No rules found for {code}")
        return results

    for version_dir in sorted(code_dir.iterdir()):
        if not version_dir.is_dir():
            continue
        if version_dir.name.startswith("."):
            continue

        rule_file = version_dir / "rule.md"
        if not rule_file.exists():
            continue

        version = version_dir.name.replace("v", "")

        with open(rule_file, "r") as f:
            rule_text = f.read()

        result = check_rule_quality(
            rule_text=rule_text,
            code=code,
            file_path=str(rule_file),
            version=version
        )
        results.append(result)

    return results


def main():
    parser = argparse.ArgumentParser(description="Check rule quality against ICD-10-CM checklist")
    parser.add_argument("path", help="Path to rule.md file OR code (e.g., E11)")
    parser.add_argument("--all-versions", "-a", action="store_true", help="Check all versions of a code")
    parser.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    parser.add_argument("--output", "-o", help="Save results to file")

    args = parser.parse_args()

    if args.all_versions or not args.path.endswith(".md"):
        # Check all versions of a code
        code = args.path.upper()
        results = check_all_versions(code)

        if not results:
            print(f"No rules found for {code}")
            return

        if args.json:
            output = {"code": code, "results": [r.to_dict() for r in results]}
            print(json.dumps(output, indent=2))
        else:
            print(f"\n{'='*60}")
            print(f"ALL VERSIONS: {code}")
            print(f"{'='*60}")
            print(f"\n{'Version':<10} {'Score':<10} {'Grade':<8} {'Errors':<8} {'Coverage':<10}")
            print("-" * 50)
            for r in results:
                print(f"v{r.version:<9} {r.total_score:<10} {r.grade:<8} {len(r.errors):<8} {r.coverage_score}/50")

            # Best version
            best = max(results, key=lambda x: x.total_score)
            print(f"\nBest version: v{best.version} ({best.total_score}/100, {best.grade})")

            # Detailed output for best
            print_result(best)
    else:
        # Check single file
        with open(args.path, "r") as f:
            rule_text = f.read()

        # Extract code and version from path
        path = Path(args.path)
        version = path.parent.name.replace("v", "") if path.parent.name.startswith("v") else "?"
        code = path.parent.parent.name if path.parent.parent else "?"

        result = check_rule_quality(
            rule_text=rule_text,
            code=code,
            file_path=args.path,
            version=version
        )

        if args.json:
            print(json.dumps(result.to_dict(), indent=2))
        else:
            print_result(result)

        if args.output:
            with open(args.output, "w") as f:
                json.dump(result.to_dict(), f, indent=2)
            print(f"Results saved to: {args.output}")


if __name__ == "__main__":
    main()
