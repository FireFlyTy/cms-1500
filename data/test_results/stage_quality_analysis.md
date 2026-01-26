# Pipeline Stage Quality Analysis

**Date:** 2026-01-26
**Code:** E11 (Type 2 Diabetes Mellitus)
**Models:** Gemini 2.5 Flash vs OpenAI GPT-5.2

---

## Executive Summary

| Stage | Gemini Quality | OpenAI Quality | Best For |
|-------|---------------|----------------|----------|
| **Draft** | ⭐⭐⭐⭐ | ⭐⭐⭐ | Gemini (more complete) |
| **Mentor** | ⭐⭐⭐ | ⭐⭐⭐⭐ | OpenAI (citation fixes) |
| **RedTeam** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | OpenAI (better risk detection) |
| **Arbitration** | ⭐⭐⭐ | ⭐⭐⭐⭐ | OpenAI (no bad approvals) |
| **Finalization** | ⭐⭐⭐ | ⭐⭐⭐⭐ | OpenAI (cleaner structure) |

---

## Stage 1: DRAFT

### Metrics

| Metric | Gemini | OpenAI |
|--------|--------|--------|
| Length | 13,810 chars | 11,280 chars |
| Thinking | 21,118 chars | N/A |
| Time | 43.7s | 82.0s |
| Citation refs | 29 (19 unique) | 28 (2 unique) |
| Sections | 5 H2, 66 bullets | 5 H2, 55 bullets |

### Content Coverage

| Topic | Gemini | OpenAI |
|-------|--------|--------|
| E11 default | ✓ | ✓ |
| Z79 codes | ✓ | ✓ |
| Gestational (O24) | ✓ | ✓ |
| Insulin pump | ✗ | ✓ |
| Secondary diabetes | ✓ | ✓ |
| Sequencing rules | ✓ | ✓ |
| Complications | ✓ | ✓ |

### Quality Assessment

**Gemini Draft:**
- ✅ More detailed and comprehensive
- ✅ References clinical sources (GLP-1, depression, foot problems)
- ✅ Better citation diversity (19 unique refs)
- ❌ Mixes clinical info with coding rules
- ❌ Misses insulin pump sequencing

**OpenAI Draft:**
- ✅ Cleaner structure
- ✅ Correctly separates coding from clinical sources
- ✅ Covers insulin pump malfunction
- ❌ Shorter, less detailed
- ❌ Poor citation diversity (only 2 unique refs!)

**Winner: Gemini** (despite missing insulin pump)

---

## Stage 2: MENTOR

### Metrics

| Metric | Gemini | OpenAI |
|--------|--------|--------|
| Verdict | NEED_CLARIFICATION | NEED_CLARIFICATION |
| Time | 21.9s | 34.7s |
| Corrections | 4 | 6 |

### Correction Types

| Type | Gemini | OpenAI |
|------|--------|--------|
| CLARIFY | 1 | 3 |
| CHANGE | 1 | 1 |
| ADD_SOURCE | 2 | 0 |
| FIX_PAGE | 0 | 1 |
| FIX_OVERFLOW | 0 | 1 |

### Corrections Detail

**Gemini Mentor:**
1. ✅ CLARIFY - Rephrase INCLUSION section
2. ⚠️ CHANGE - Remove CPT debridement (questionable)
3. ✅ ADD_SOURCE - E11.A remission codes
4. ❌ ADD_SOURCE - E10.A- presymptomatic (**WRONG** - Type 1 in Type 2 rule!)

**OpenAI Mentor:**
1. ✅ FIX_PAGE - SMBG citation page correction
2. ✅ FIX_OVERFLOW - Fix stitched/ellipsis citations
3. ✅ CLARIFY - Scope (ICD-10-CM vs CPT/HCPCS)
4. ✅ CLARIFY - Insulin pump documentation gate
5. ✅ CHANGE - De-emphasize ODG clinical info
6. ✅ CLARIFY - Unspecified E11 codes guidance

### Quality Assessment

**Gemini Mentor:**
- ✅ Finds missing content (E11.A remission)
- ❌ Proposes incorrect E10.A- addition (code-type error!)
- ❌ Doesn't catch citation issues

**OpenAI Mentor:**
- ✅ Catches citation errors (page, stitching)
- ✅ Better scope clarification
- ❌ Misses E11.A remission codes
- ❌ Misses CKD/ESRD rule

**Winner: OpenAI** (catches citation issues, no code-type errors)

---

## Stage 3: REDTEAM

### Metrics

| Metric | Gemini | OpenAI |
|--------|--------|--------|
| Verdict | SAFETY_RISK | SAFETY_RISK |
| Risks found | 5 | 4 |
| Time | 46.1s | 42.3s |
| Corrections | 5 | 5 |

### Correction Types

| Type | Gemini | OpenAI |
|------|--------|--------|
| BLOCK_RISK | 4 | 3 |
| ADD_STEP | 1 | 0 |
| FIX_PAGE | 0 | 1 |
| FIX_OVERFLOW | 0 | 1 |

### Risk Analysis

**Gemini RedTeam Risks:**
1. ✅ CPT 97602 additional exclusion
2. ✅ GLP-1 contraindications (important safety!)
3. ✅ CKD + ESRD = N18.6 only
4. ✅ Gestational diabetes treatment rules
5. ✅ Provider documentation rule

**OpenAI RedTeam Risks:**
1. ✅ Gestational diabetes Z79 exclusions
2. ✅ Insulin pump should NOT hardcode E11 (excellent catch!)
3. ✅ Secondary diabetes exclusion
4. ✅ Citation page fix
5. ✅ Citation stitching fix

### Quality Assessment

**Gemini RedTeam:**
- ✅ Finds GLP-1 contraindications (safety critical)
- ✅ Finds CKD/ESRD rule
- ❌ Doesn't catch insulin pump hardcoding issue

**OpenAI RedTeam:**
- ✅ Catches insulin pump hardcoding (don't default to E11!)
- ✅ Catches secondary diabetes scope
- ✅ Validates citations
- ❌ Misses GLP-1 contraindications
- ❌ Misses CKD/ESRD rule

**Winner: OpenAI** (insulin pump catch is critical; GLP-1 is clinical not coding)

---

## Stage 4: ARBITRATION

### Metrics

| Metric | Gemini | OpenAI |
|--------|--------|--------|
| Safety status | FAILED | FAILED |
| Usability | HIGH | NEEDS_IMPROVEMENT |
| Time | 31.0s | 22.8s |
| Approved | 8 | 9 |
| Rejected | 1 | 0 |

### Approved Corrections Types

| Type | Gemini | OpenAI |
|------|--------|--------|
| BLOCK_RISK | 4 | 3 |
| CLARIFY | 1 | 3 |
| ADD_SOURCE | 2 | 0 |
| ADD_STEP | 1 | 0 |
| CHANGE | 0 | 1 |
| FIX_PAGE | 0 | 1 |
| FIX_OVERFLOW | 0 | 1 |

### Critical Errors

**Gemini Arbitration:**
- ❌ Approved E10.A- (Type 1) instruction for E11 (Type 2) rule
- ✅ Correctly rejected CPT debridement removal

**OpenAI Arbitration:**
- ✅ No incorrect approvals
- ✅ All corrections are valid for E11

### Quality Assessment

**Gemini Arbitration:**
- ✅ Good rejection reasoning (safety > clarity)
- ❌ Failed to catch code-type mismatch (E10 ≠ E11)

**OpenAI Arbitration:**
- ✅ All approvals are correct
- ✅ Cleaner decision process

**Winner: OpenAI** (no code-type errors)

---

## Stage 5: FINALIZATION

### Metrics

| Metric | Gemini | OpenAI |
|--------|--------|--------|
| Length | 36,698 chars | 24,792 chars |
| Thinking | 11,649 chars | N/A |
| Time | 51.4s | 103.5s |

### Structure

| Element | Gemini | OpenAI |
|---------|--------|--------|
| SUMMARY | Long, detailed | Concise |
| CRITERIA | 3 sections | 3 sections |
| INSTRUCTIONS | 16 IF-THEN | 12 IF-THEN |
| CLINICAL CONTEXT | Mixed in | Separate section |
| TRACEABILITY | 52 entries | 13 entries |
| SELF-CHECK | 10 items | 10 items |

### Quality Assessment

**Gemini Finalization:**
- ✅ Very detailed traceability (52 entries)
- ✅ More IF-THEN instructions
- ❌ Contains E10.A- error from arbitration
- ❌ Redundant content

**OpenAI Finalization:**
- ✅ Clean separation of clinical context
- ✅ Concise and readable
- ✅ No code-type errors
- ❌ Less detailed traceability
- ❌ Missing some rules (E11.A, CKD/ESRD)

**Winner: OpenAI** (no errors, better structure despite less detail)

---

## Ensemble Recommendations

### Option 1: Best of Each Stage

| Stage | Model | Reason |
|-------|-------|--------|
| Draft | Gemini Flash-Lite | More complete coverage |
| Mentor | GPT-5.2 | Catches citation issues |
| RedTeam | GPT-5.2 | Better risk detection |
| Arbitration | GPT-5.2 | No code-type errors |
| Finalization | Gemini Flash-Lite | Keep detail, post-process for errors |

### Option 2: Single Provider (Simplicity)

**Gemini-only:**
- ✅ Faster (172s vs 251s)
- ✅ More content
- ❌ Code-type errors pass through
- **Needs:** Post-processing validation

**OpenAI-only:**
- ✅ Cleaner output
- ✅ No code-type errors
- ❌ Missing some rules
- **Needs:** Completeness check

### Option 3: Hybrid with Validation

```
Draft: Gemini (fast, complete)
   ↓
Mentor: GPT-5.2 (citation validation)
   ↓
RedTeam: Gemini (safety risks) + GPT-5.2 (scope validation)
   ↓
Arbitration: GPT-5.2 (cleaner decisions)
   ↓
Finalization: Gemini (detail)
   ↓
Post-Validation: Code-type check, deduplication
```

---

## Key Findings

1. **Gemini excels at:** Content generation, speed, detail
2. **OpenAI excels at:** Validation, scope checking, structure
3. **Critical gap:** Neither catches code-type mismatches automatically
4. **Citation quality:** OpenAI better at validating citations

## Required Improvements

1. **Add code-type validation** in Arbitration prompt
2. **Add completeness check** against ICD-10 Guidelines
3. **Deduplication** in Finalization
4. **Separate clinical content** in Gemini output

---

## HYBRID V2 TEST RESULTS (2026-01-26)

### Configuration

| Stage | Provider | Model |
|-------|----------|-------|
| Draft | Gemini | gemini-2.5-flash-lite |
| Mentor | OpenAI | gpt-5.2 |
| RedTeam | OpenAI | gpt-5.2 |
| Arbitration | OpenAI | gpt-5.2 |
| Finalization | Gemini | gemini-2.5-flash-lite |

### Performance

| Metric | Hybrid v2 | Gemini-only | OpenAI-only |
|--------|-----------|-------------|-------------|
| **Total time** | **166s** ✓ | 231s | 170s |
| Draft | 40s | 57s | 31s |
| Mentor | 35s | 43s | 35s |
| RedTeam | 40s | 55s | 36s |
| Arbitration | 31s | 34s | 31s |
| Finalization | 51s | 85s | 71s |

### Validator Quality

| Metric | Hybrid v2 | Gemini-only | OpenAI-only |
|--------|-----------|-------------|-------------|
| Mentor JSON | ✅ Yes | ✅ Yes | ❌ No |
| Mentor corrections | 6 | 7 | 0 (parse fail) |
| RedTeam corrections | 4 | 3 | 5 |
| Arbitration | **PASSED** ✓ | FAILED | PASSED |
| Approved | 8 | 10 | 8 |
| Rejected | 0 | 2 | 0 |

### Content Quality (v12 rule.md)

**Coverage Checklist:**

| Rule | Hybrid v2 | Notes |
|------|-----------|-------|
| E11 default for unspecified type | ✅ | "default is E11.-" |
| Multiple codes for complications | ✅ | "assign as many codes as necessary" |
| Z79 for long-term meds | ✅ | "both E code(s) and Z79 code(s)" |
| Z79.4 NOT for temporary insulin | ✅ | "Do not assign Z79.4 if insulin is given only temporarily" |
| O24 first for pregnancy | ✅ | "assign an O24.- code first" |
| O24.4 exclusions (no Z79) | ✅ | "do NOT assign Z79.4, Z79.84, or Z79.85 with O24.4" |
| Insulin pump malfunction | ❌ | Missing T85.6 + T38.3X6 rules |
| Secondary diabetes scope | ⚠️ | Brief mention in REFERENCE |
| CKD + ESRD = N18.6 | ❌ | Missing |
| E11.A Remission codes | ❌ | Missing |
| E10.A- (Type 1) error | ✅ | **NOT included** (correct!) |
| GLP-1 clinical info | ✅ | Separate from coding rules |
| Sequencing by encounter | ✅ | "be sequenced based on the reason" |

**Structure Quality:**

| Element | Status | Details |
|---------|--------|---------|
| SUMMARY | ✅ | Detailed, well-cited |
| CRITERIA | ✅ | 3 sections (INCLUSION/EXCLUSION/CLARIFICATIONS) |
| INSTRUCTIONS | ✅ | 8 IF-THEN rules |
| REFERENCE | ✅ | 14 citations |
| SOURCE LOG | ✅ | All 6 docs cited |
| SELF-CHECK | ✅ | 10/10 passed |
| Output size | ✅ | 18,860 chars (balanced) |

**Citation Quality:**

| Metric | Value |
|--------|-------|
| Total citations | 24 |
| Source documents | 6/6 used |
| Pages cited | 16, 39, 40, 41, 42, 67, 249, 1, 2 |
| Citation variance | HIGH |
| Repeated quotes | NO |

### Critical Error Check

| Error Type | Hybrid v2 | Gemini v4 | OpenAI v5 |
|------------|-----------|-----------|-----------|
| E10.A- in E11 rule | ✅ **NO** | ❌ YES | ✅ NO |
| Citation page errors | ✅ Fixed | ⚠️ Some | ✅ Fixed |
| Code-type mismatch | ✅ **NONE** | ❌ Present | ✅ None |

### Conclusion

**Hybrid v2 is the WINNER:**

1. ✅ **Fastest** (166s vs 170s OpenAI, 231s Gemini)
2. ✅ **No critical errors** (no E10.A- code-type mismatch)
3. ✅ **All validators parse JSON** successfully
4. ✅ **Arbitration PASSED** (vs FAILED for Gemini)
5. ✅ **Clean structure** with proper separation of clinical context
6. ✅ **Good citation quality** (24 citations, high variance)

**Still missing (non-critical):**
- Insulin pump malfunction rules (T85.6 + T38.3X6)
- E11.A remission codes
- CKD/ESRD = N18.6 rule

These gaps are acceptable for a code-category rule (E11) and can be addressed in subcategory rules (E11.6x, etc.).

### Recommended Configuration

```python
PIPELINE_MODELS_HYBRID = {
    "draft":        {"provider": "gemini", "model": "gemini-2.5-flash-lite"},
    "mentor":       {"provider": "openai", "model": "gpt-5.2", "reasoning_effort": "low"},
    "redteam":      {"provider": "openai", "model": "gpt-5.2", "reasoning_effort": "low"},
    "arbitration":  {"provider": "openai", "model": "gpt-5.2", "reasoning_effort": "low"},
    "finalization": {"provider": "gemini", "model": "gemini-2.5-flash-lite"},
}
```

**Why this works:**
- Gemini for generation = fast content creation
- GPT-5.2 for validation = catches errors, no false positives
- Combined = best of both worlds

---

## QUALITY SCORES (All Versions)

| Version | Config | Score | Grade | Errors | Coverage |
|---------|--------|-------|-------|--------|----------|
| v1 | Initial | 32 | F | 3 | 7/50 |
| v2 | - | 56 | C- | 1 | 16/50 |
| v3 | - | 75 | B | 1 | 42/50 |
| v4 | Gemini-only | 50 | D | 2 | 47/50 |
| v5 | OpenAI-only | 88 | A- | 0 | 45/50 |
| v6 | OpenAI gpt-5-mini | 88 | A- | 0 | 45/50 |
| v7 | OpenAI gpt-5.1 | 80 | B+ | 0 | 45/50 |
| v8 | - | 88 | A- | 0 | 40/50 |
| v9 | Hybrid v1 (4.1/5.2/5.1) | 93 | A | 0 | 45/50 |
| v10 | - | 75 | B | 1 | 42/50 |
| v11 | - | 75 | B | 1 | 45/50 |
| **v12** | **Hybrid v2 (5.2 all)** | **93** | **A** | **0** | **45/50** |

**Лучший результат: v12 (Hybrid v2) - 93/100, Grade A**

---

## COST ANALYSIS

### Model Pricing

| Model | Input ($/1M tokens) | Output ($/1M tokens) |
|-------|--------------------:|---------------------:|
| gemini-2.5-flash-lite | $0.10 | $0.40 |
| gpt-5.2 | $1.75 | $14.00 |

### Token Estimates (E11 Rule)

| Stage | Input chars | Output chars | Input tokens | Output tokens |
|-------|-------------|--------------|--------------|---------------|
| Draft | ~50,000 | ~12,000 | ~12,500 | ~3,000 |
| Mentor | ~62,000 | ~4,500 | ~15,500 | ~1,125 |
| RedTeam | ~62,000 | ~3,000 | ~15,500 | ~750 |
| Arbitration | ~70,000 | ~5,000 | ~17,500 | ~1,250 |
| Finalization | ~75,000 | ~19,000 | ~18,750 | ~4,750 |
| **Total** | | | **~80,000** | **~11,000** |

### Cost Per Rule Generation

| Config | Draft | Validators | Finalization | **Total** |
|--------|-------|------------|--------------|-----------|
| **Gemini-only** | $0.002 | $0.006 | $0.003 | **$0.011** |
| **OpenAI-only** | $0.06 | $0.14 | $0.09 | **$0.29** |
| **Hybrid v2** | $0.002 | $0.14 | $0.003 | **$0.145** |

### Cost Breakdown: Hybrid v2

| Stage | Model | Input Cost | Output Cost | Total |
|-------|-------|------------|-------------|-------|
| Draft | Gemini | $0.00125 | $0.00120 | $0.00245 |
| Mentor | GPT-5.2 | $0.02713 | $0.01575 | $0.04288 |
| RedTeam | GPT-5.2 | $0.02713 | $0.01050 | $0.03763 |
| Arbitration | GPT-5.2 | $0.03063 | $0.01750 | $0.04813 |
| Finalization | Gemini | $0.00188 | $0.00190 | $0.00378 |
| **Total** | | | | **$0.135** |

### Cost-Quality Matrix

| Config | Cost/Rule | Score | Grade | Cost per Point |
|--------|-----------|-------|-------|----------------|
| Gemini-only | $0.011 | 50 | D | $0.00022 |
| OpenAI-only | $0.29 | 88 | A- | $0.0033 |
| **Hybrid v2** | **$0.135** | **93** | **A** | **$0.00145** |

**Hybrid v2 - лучшее соотношение цена/качество:**
- В 2x дешевле OpenAI-only
- В 12x дороже Gemini-only
- Но качество 93 vs 50 (Gemini) vs 88 (OpenAI)

### Monthly Cost Projection

| Scenario | Rules/month | Gemini | OpenAI | Hybrid v2 |
|----------|-------------|--------|--------|-----------|
| Low | 100 | $1.10 | $29.00 | **$13.50** |
| Medium | 1,000 | $11.00 | $290.00 | **$135.00** |
| High | 10,000 | $110.00 | $2,900.00 | **$1,350.00** |

---

## QUALITY CHECKLIST (E11)

### Coverage Rules (50 points max)

| # | Rule | Pattern | Source | Weight | Critical |
|---|------|---------|--------|--------|----------|
| 1 | E11 default when type not documented | `default.*E11\|not documented` | p.40 | 15 | YES |
| 2 | Multiple codes for complications | `as many codes\|multiple codes` | p.39 | 10 | |
| 3 | Z79 for long-term meds | `Z79\.\d\|long-term.*use` | p.40 | 15 | YES |
| 4 | Z79.4 NOT for temporary insulin | `temporarily.*not.*assign` | p.40 | 15 | YES |
| 5 | O24 first for pregnancy | `O24.*first\|pregnancy.*O24` | p.67 | 10 | |
| 6 | O24.4 excludes Z79 | `O24\.4.*should not.*Z79` | p.67 | 10 | |
| 7 | Insulin pump (T85.6) | `insulin pump\|T85\.6` | p.41 | 5 | |
| 8 | Secondary diabetes scope | `secondary\|E08\|E09\|E13` | p.41-42 | 5 | |
| 9 | CKD + ESRD = N18.6 | `N18\.6\|ESRD.*CKD` | p.62 | 5 | |
| 10 | E11.A remission codes | `remission\|E11\.A` | p.40 | 5 | |
| 11 | Sequencing by encounter | `sequenced.*encounter` | p.39 | 10 | |

### Error Checks (Penalties)

| Error | Description | Penalty |
|-------|-------------|---------|
| Code-type mismatch | E10 in E11 rule | -30 |
| Stitched citations | Ellipsis in anchors | -5 |
| Missing sections | No SUMMARY/CRITERIA/etc | -15 |

### Structure Checks (25 points max)

| Check | Points |
|-------|--------|
| has_summary | 5 |
| has_inclusion | 5 |
| has_exclusion | 5 |
| has_instructions (5+ IF-THEN) | 5 |
| has_reference | 5 |
| has_source_log | 5 |
| has_self_check | 5 |

### Citation Checks (25 points max)

| Variance | Criteria | Points |
|----------|----------|--------|
| HIGH | 15+ pages, 4+ docs | 25 |
| MEDIUM | 8+ pages, 3+ docs | 18 |
| LOW | <8 pages, <3 docs | 10 |
| Bonus | 20+ total citations | +5 |

### Grade Scale

| Score | Grade |
|-------|-------|
| 95-100 | A+ |
| 90-94 | A |
| 85-89 | A- |
| 80-84 | B+ |
| 75-79 | B |
| 70-74 | B- |
| 65-69 | C+ |
| 60-64 | C |
| 55-59 | C- |
| 50-54 | D |
| <50 | F |

---

## VERIFICATION SCRIPT

```bash
# Check single rule
python api/scripts/check_rule_quality.py data/processed/rules/E11/v12/rule.md

# Check all versions of a code
python api/scripts/check_rule_quality.py E11 --all-versions

# Output as JSON
python api/scripts/check_rule_quality.py E11 -a --json > scores.json
```

Script location: `api/scripts/check_rule_quality.py`
