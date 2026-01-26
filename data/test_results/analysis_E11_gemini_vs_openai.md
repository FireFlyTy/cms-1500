# Analysis: E11 Pipeline Comparison (Gemini vs OpenAI)

**Date:** 2026-01-26
**Code:** E11 (Type 2 Diabetes Mellitus)
**Mode:** JSON Validators

---

## Performance Summary

| Metric | Gemini | OpenAI | Winner |
|--------|--------|--------|--------|
| **Total time** | 172s | 251s | Gemini (+31%) |
| Draft | 43.7s (13.8k chars) | 82.0s (11.3k chars) | Gemini |
| Mentor | 21.9s | 34.7s | Gemini |
| RedTeam | 46.1s | 42.3s | OpenAI |
| Arbitration | 31.0s | 22.8s | OpenAI |
| Finalization | 51.4s (36.7k chars) | 103.5s (24.8k chars) | Gemini |
| **JSON Parse** | ✅ All | ✅ All | Tie |

---

## Validator Results

| Validator | Gemini | OpenAI |
|-----------|--------|--------|
| Mentor verdict | NEED_CLARIFICATION | NEED_CLARIFICATION |
| Mentor corrections | 4 | 6 |
| RedTeam verdict | SAFETY_RISK | SAFETY_RISK |
| RedTeam corrections | 5 | 5 |
| Arbitration status | FAILED | FAILED |
| Approved corrections | 8 | 9 |
| Rejected corrections | 1 | 0 |

---

## E11 Code Book Reference (86 codes)

| Category | Description | Count |
|----------|-------------|-------|
| E110x | Hyperosmolarity (with/without coma) | 2 |
| E111x | Ketoacidosis (with/without coma) | 2 |
| E112x | Nephropathy, CKD, kidney complications | 3 |
| E113x | Retinopathy, cataract, ophthalmic | 44 |
| E114x | Neuropathy (mono-, poly-, autonomic) | 6 |
| E115x | Peripheral angiopathy, circulatory | 3 |
| E116x | Skin, foot, oral, arthropathy, hypoglycemia | 14 |
| E118 | Unspecified complications | 1 |
| E119 | Without complications | 1 |

---

## Content Quality Comparison

### Output Size
- **Gemini (v4):** 36,845 bytes (rule.md)
- **OpenAI (v5):** 24,883 bytes (rule.md)

### Structure

| Section | Gemini | OpenAI |
|---------|--------|--------|
| SUMMARY | ✅ Long, detailed | ✅ Concise |
| CRITERIA | ✅ 3 sections (INCLUSION/EXCLUSION/SEQUENCING) | ✅ 3 sections |
| INSTRUCTIONS | ✅ 16 IF-THEN rules | ✅ 12 IF-THEN rules |
| CLINICAL CONTEXT | ❌ Mixed with coding | ✅ Separate section |
| TRACEABILITY | ✅ 52 entries | ✅ 13 entries |
| SELF-CHECK | ✅ 10 checks | ✅ 10 checks |

### Coverage Analysis

| Topic | Gemini | OpenAI | Source |
|-------|--------|--------|--------|
| Default E11 for unspecified type | ✅ | ✅ | ICD-10 Guidelines p.40 |
| Multiple codes for complications | ✅ | ✅ | ICD-10 Guidelines p.39 |
| Z79 for long-term meds | ✅ | ✅ | ICD-10 Guidelines p.40 |
| Z79.4 NOT for temporary insulin | ✅ | ✅ | ICD-10 Guidelines p.40 |
| O24 first for pregnancy | ✅ | ✅ | ICD-10 Guidelines p.67 |
| O24.4 exclusions (no Z79) | ✅ | ✅ | ICD-10 Guidelines p.67 |
| Insulin pump (T85.6 + T38.3X6) | ❌ Missing | ✅ Covered | ICD-10 Guidelines p.41 |
| Secondary diabetes (E08/E09/E13) | ⚠️ Brief | ✅ Detailed | ICD-10 Guidelines p.41-42 |
| CKD + ESRD = N18.6 only | ✅ Covered | ❌ Missing | ICD-10 Guidelines p.62 |
| E11.A Remission codes | ✅ Covered | ❌ Missing | ICD-10 Guidelines p.40 |
| E10.A- Presymptomatic T1DM | ✅ Covered | ❌ Missing | ICD-10 Guidelines p.39 |
| GLP-1 contraindications | ✅ In rules | ⚠️ Clinical only | ODG Guidelines |
| CPT debridement exclusions | ✅ Covered | ✅ Covered | NCCI Manual p.248 |
| Sequencing by encounter reason | ✅ | ✅ | ICD-10 Guidelines p.39 |

---

## Identified Issues

### Gemini (v4) Issues
1. **Redundancy** - Same rules repeated in SUMMARY, CRITERIA, and INSTRUCTIONS
2. **Over-citation** - 52 traceability entries with many duplicates
3. **Mixed content** - Clinical info (GLP-1 contraindications) mixed with coding rules
4. **E10.A mentioned** - Type 1 presymptomatic code mentioned in E11 (Type 2) rule

### OpenAI (v5) Issues
1. **Missing rules** - No CKD/ESRD rule, no remission codes
2. **Under-citation** - Only 13 traceability entries
3. **5a56ab38 misuse** - Medicare Policy Manual cited but not integrated into coding rules
4. **Less specific** - Fewer actionable IF-THEN instructions

### Common Issues (Both)
1. **Arbitration FAILED** - Both flagged safety issues but corrections applied
2. **SAFETY_RISK from RedTeam** - Indicates potential harmful recommendations found
3. **Missing complications detail** - Neither covers specific E11.xxx subcodes (retinopathy laterality, etc.)

---

## Files

- Gemini rule: `data/processed/rules/E11/v4/rule.md`
- Gemini log: `data/processed/rules/E11/v4/generation_log.json`
- OpenAI rule: `data/processed/rules/E11/v5/rule.md`
- OpenAI log: `data/processed/rules/E11/v5/generation_log.json`
- Test results: `data/test_results/pipeline_comparison_E11_*.json`

---

## Detailed Error Analysis

### Gemini Pipeline Errors

**MENTOR (4 corrections):**
1. ✅ CLARIFY - Rephrase INCLUSION section (valid)
2. ⚠️ CHANGE - Remove CPT debridement (rejected by arbitrator - good!)
3. ✅ ADD_SOURCE - E11.A remission instruction (valid)
4. ❌ ADD_SOURCE - E10.A- presymptomatic (WRONG - Type 1, not Type 2!)

**REDTEAM (5 risks):**
1. ✅ BLOCK_RISK - CPT 97602 additional exclusion
2. ✅ BLOCK_RISK - GLP-1 contraindications (important safety!)
3. ✅ BLOCK_RISK - CKD + ESRD = N18.6 only
4. ✅ BLOCK_RISK - Gestational diabetes treatment rules
5. ✅ ADD_STEP - Provider documentation rule

**ARBITRATION:**
- Safety: FAILED (due to risks found)
- Approved: 8 (including the WRONG E10.A- instruction!)
- Rejected: 1 (CPT removal - correctly rejected)

**Critical Error:** Arbitrator approved E10.A- (Type 1 presymptomatic) in E11 (Type 2) rule. This is a code-type mismatch that should have been caught.

---

### OpenAI Pipeline Errors

**MENTOR (6 corrections):**
1. ✅ FIX_PAGE - SMBG citation page correction
2. ✅ FIX_OVERFLOW - Stitched citation needs exact quote
3. ✅ CLARIFY - Scope (ICD-10-CM vs CPT/HCPCS)
4. ✅ CLARIFY - Insulin pump documentation gate
5. ⚠️ CHANGE - De-emphasize ODG clinical info
6. ✅ CLARIFY - Unspecified E11 codes guidance

**REDTEAM (5 corrections, 4 risks):**
1. ✅ BLOCK_RISK - Gestational diabetes Z79 exclusions
2. ✅ BLOCK_RISK - Insulin pump should NOT hardcode E11
3. ✅ BLOCK_RISK - Secondary diabetes exclusion
4. ✅ FIX_PAGE - SMBG citation correction
5. ✅ FIX_OVERFLOW - Citation stitching fix

**ARBITRATION:**
- Safety: FAILED
- Approved: 9
- Rejected: 0

**Positive:** OpenAI correctly identified that insulin pump malfunction should use documented diabetes type, not default to E11.

**Missing:** No mention of E11.A remission codes or CKD/ESRD rule (N18.6).

---

### Error Pattern Summary

| Error Type | Gemini | OpenAI |
|------------|--------|--------|
| Code-type mismatch (E10 in E11 rule) | ❌ Yes | ✅ No |
| Citation page errors | ⚠️ Not detected | ✅ Found |
| Citation stitching | ⚠️ Not detected | ✅ Found |
| Secondary diabetes handling | ⚠️ Brief | ✅ Detailed |
| Insulin pump hardcoding | ❌ Missed | ✅ Fixed |
| Remission codes (E11.A) | ✅ Included | ❌ Missing |
| CKD/ESRD rule | ✅ Included | ❌ Missing |
| GLP-1 contraindications | ✅ In rules | ⚠️ Clinical only |

---

## Recommendations

1. **Post-processing** - Add deduplication step to remove redundant rules
2. **Validation** - Add check for code-specific coverage (E11 rule shouldn't mention E10.A)
3. **Completeness check** - Verify all ICD-10 Guidelines sections for the code are covered
4. **Clinical separation** - Enforce separation of coding rules from clinical context
5. **Citation balance** - Target 20-30 traceability entries with no duplicates
6. **Code-type validation** - Arbitrator should reject corrections referencing wrong code types
7. **Citation verification** - Add step to verify page numbers and exact quotes

---

## HYBRID V2: FINAL WINNER

### Configuration

```
Draft:        Gemini flash-lite  (fast content generation)
Mentor:       GPT-5.2            (validation with reasoning)
RedTeam:      GPT-5.2            (risk detection)
Arbitration:  GPT-5.2            (clean decisions)
Finalization: Gemini flash-lite  (fast, detailed output)
```

### Performance Comparison (All Variants)

| Metric | Gemini | OpenAI | Hybrid v2 | Winner |
|--------|--------|--------|-----------|--------|
| **Total time** | 231s | 170s | **166s** | Hybrid |
| Draft | 57s | 31s | 40s | OpenAI |
| Mentor | 43s | 35s | 35s | Tie |
| RedTeam | 55s | 36s | 40s | OpenAI |
| Arbitration | 34s | 31s | 31s | Tie |
| Finalization | 85s | 71s | 51s | **Hybrid** |

### Quality Comparison (All Variants)

| Metric | Gemini | OpenAI | Hybrid v2 | Winner |
|--------|--------|--------|-----------|--------|
| Mentor JSON | ✅ | ❌ | ✅ | Hybrid |
| Mentor corrections | 7 | 0 | 6 | Gemini |
| RedTeam corrections | 3 | 5 | 4 | OpenAI |
| Arbitration | FAILED | PASSED | **PASSED** | Tie |
| E10.A- error | ❌ Yes | ✅ No | ✅ **No** | Hybrid/OpenAI |
| Output size | 43k | 17k | 19k | Hybrid (balanced) |

### Hybrid v2 Output Quality (v12)

| Coverage Item | Status |
|---------------|--------|
| E11 default for unspecified | ✅ |
| Multiple codes for complications | ✅ |
| Z79 for long-term meds | ✅ |
| Z79.4 NOT for temporary insulin | ✅ |
| O24 first for pregnancy | ✅ |
| O24.4 exclusions (no Z79) | ✅ |
| Sequencing by encounter | ✅ |
| No E10.A- error | ✅ |
| Insulin pump rules | ❌ |
| E11.A remission | ❌ |
| CKD/ESRD rule | ❌ |

### Why Hybrid Wins

1. **Fastest**: 166s (vs 170s OpenAI, 231s Gemini)
2. **No critical errors**: No E10.A- code-type mismatch
3. **All JSON parses**: 100% validator parsing success
4. **Arbitration PASSED**: Clean validation process
5. **Balanced output**: 19k chars (not too verbose, not too sparse)
6. **Good citations**: 24 unique, high variance

### Files

- Hybrid v2 rule: `data/processed/rules/E11/v12/rule.md`
- Hybrid v2 log: `data/processed/rules/E11/v12/generation_log.json`
- Test results: `data/test_results/pipeline_comparison_E11_20260126_142157.json`
