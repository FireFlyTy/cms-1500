"""
prompts.py — Multi-Document Prompts for Code Rule Generation

Pipeline: Draft → Mentor → RedTeam → Arbitration → Finalization

Citation format: [[doc_id:page | "anchor phrase"]]
"""

from string import Template

# ---------------------------------------------------------
# SHARED: INHERITANCE RULES (For hierarchical rule generation)
# ---------------------------------------------------------
INHERITANCE_RULES = '''
=== INHERITANCE STRATEGY: SELECTIVE RULE INHERITANCE ===

You are creating a validation rule for a CHILD code.
The runtime executes *ONLY* this specific rule — it must be COMPLETE and SELF-CONTAINED.

**CRITICAL**: Parent rules come from a BROADER category. NOT ALL parent rules apply to every child!

Example: Parent E (Endocrine chapter) includes rules for:
- Diabetes (E08-E13)
- Thyroid disorders (E00-E07)
- Metabolic disorders (E70-E88)

If child is E00.2 (iodine deficiency) → diabetes rules do NOT apply!

## DISPOSITION FOR EACH PARENT RULE

Evaluate EACH parent rule against THIS child code:

| Disposition | When to Use | Mark As |
|-------------|-------------|---------|
| **KEEP** | Parent rule applies to this child unchanged | // INHERITED |
| **SPECIALIZE** | Parent rule applies but needs refinement for this child | // SPECIALIZED from parent |
| **DROP** | Parent rule is for a DIFFERENT condition within parent category | (omit entirely) |
| **ADD** | New rule specific to this child, not in parent | // NEW for {code} |

## RULES FOR DISPOSITION

1. **KEEP** — Include if:
   - Rule is about general coding guidelines (sequencing, documentation)
   - Rule applies to the SAME medical condition as this child
   - Rule is universal within the parent category

2. **SPECIALIZE** — Modify if:
   - Parent rule is generic, child needs more specific criteria
   - Parent says "diabetes" but child is specific type (E11.65)
   - Parent has broad age range, child has narrower

3. **DROP** — Exclude if:
   - Parent rule is about a DIFFERENT disease/condition
   - Parent rule mentions codes outside this child's range
   - Rule is irrelevant to this child's medical context
   Example: E00.2 (iodine deficiency) → DROP all E08-E13 diabetes rules

4. **ADD** — Create new if:
   - Child has specific requirements not in parent
   - Source documents mention criteria specific to this code

## OUTPUT REQUIREMENTS

1. **INHERITANCE ANALYSIS** (required section):
   ```
   ## INHERITANCE ANALYSIS
   Parent: {parent_code}

   | Parent Rule | Disposition | Reason |
   |-------------|-------------|--------|
   | "Diabetes requires..." | DROP | E00.2 is not diabetes |
   | "Provider documentation required" | KEEP | Universal requirement |
   | "Assign multiple codes" | KEEP | General coding guideline |
   ```

2. **SAME_AS_PARENT CHECK**:
   - If ALL parent rules are KEEP (no DROP, no SPECIALIZE, no ADD):
   - Output ONLY: "## STATUS: SAME_AS_PARENT"

If NO parent rule is provided → generate complete standalone rule.
'''

# ---------------------------------------------------------
# SHARED: CMS-1500 INHERITANCE RULES
# ---------------------------------------------------------
CMS_INHERITANCE_RULES = '''
=== CMS-1500 INHERITANCE STRATEGY (SELECTIVE) ===

**IMPORTANT**: The runtime executes ONLY this file. Rules must be COMPLETE but RELEVANT.

**CRITICAL**: Parent CMS-1500 rules come from a BROADER category. NOT ALL apply to every child!

## DISPOSITION FOR EACH PARENT RULE

| Disposition | When to Use | Mark As |
|-------------|-------------|---------|
| **KEEP** | Parent rule applies to this child unchanged | [INHERITED from {parent}] |
| **SPECIALIZE** | Parent rule needs refinement for this child | [SPECIALIZED from {parent}] |
| **DROP** | Parent rule is for a DIFFERENT condition | (omit entirely) |

## RULE INHERITANCE ORDER

1. **ANALYZE PARENT CMS-1500 RULES**
   - For EACH parent rule, determine: KEEP, SPECIALIZE, or DROP
   - DROP if rule mentions codes/conditions outside this child's scope
   - Example: Parent E has diabetes bundling rules → DROP for E00 (iodine deficiency)

2. **INCLUDE KEPT/SPECIALIZED RULES**
   - Copy rules marked KEEP with [INHERITED from {parent}]
   - Modify rules marked SPECIALIZE with [SPECIALIZED from {parent}]

3. **ADD NEW RULES FROM THIS CODE'S GUIDELINE**
   - Convert THIS code's guideline criteria to CMS-1500 rules
   - Mark each as [NEW from guideline]

4. **ADD NCCI EDITS FOR THIS CODE**
   - Include bundling (PTP) and unit limits (MUE) specific to THIS code
   - Mark each as [NCCI]

## OUTPUT REQUIREMENTS

1. **ANALYSIS OF PARENT RULES** (required section):
   ```
   | # | Parent Rule ID | Summary | Disposition | Reason |
   |---|----------------|---------|-------------|--------|
   | 1 | E-DIAG-001 | Diabetes sequencing | DROP | E00.2 is not diabetes |
   | 2 | E-DOC-001 | Provider documentation | KEEP | Universal requirement |
   ```

2. **SAME_AS_PARENT CHECK**:
   - If ALL parent rules KEEP + no new rules from guideline + no NCCI:
   - Output ONLY: "## STATUS: SAME_AS_PARENT"

⚠️ WRONG: Keep diabetes rules for non-diabetes codes
✓ RIGHT: DROP irrelevant parent rules, KEEP only applicable ones

If NO parent CMS-1500 rule → generate from guideline + NCCI only.
'''

# ---------------------------------------------------------
# SHARED: PAGE IDENTIFICATION RULE (MULTI-DOCUMENT)
# ---------------------------------------------------------
PAGE_IDENTIFICATION_RULE_MULTI = '''
=== CRITICAL: PAGE NUMBER IDENTIFICATION ===

The context contains MULTIPLE SOURCE DOCUMENTS. Each source is marked as:
=== SOURCE: <filename> [doc_id: <ID>] ===

Within each source, page numbers are ALWAYS marked as:
## Page N

Where N is the PHYSICAL page number (e.g., ## Page 195, ## Page 320).

⚠️ WARNING: The text may contain OTHER numbers that look like page numbers (e.g., "182", "288", "45"). 
These are PRINTED ARTIFACTS from the original PDF document. IGNORE THEM COMPLETELY.

**How to identify the correct page number:**
1. Look for the pattern: ## Page <NUMBER>
2. The number AFTER "## Page" is the ONLY valid page reference
3. Any standalone numbers in the content are NOT page numbers

**How to identify the document:**
1. Look ABOVE the ## Page marker for === SOURCE: <filename> [doc_id: <ID>] ===
2. Use the doc_id (e.g., "abc123") from that header
3. Each citation MUST include BOTH doc_id AND page number

**Example:**
```
=== SOURCE: icd10_codebook.pdf [doc_id: abc123] ===
## Page 195
182
CHAPTER 9 Symptoms, Signs...
```
✅ Correct reference: [abc123] Page 195
❌ Wrong: Page 182 (this is just printed text)
❌ Wrong: Page 195 without doc_id
'''

# ---------------------------------------------------------
# STEP 1: DRAFT GENERATION (Multi-Document)
# ---------------------------------------------------------
PROMPT_CODE_RULE_DRAFT = Template('''
You are a Forensic Medical Auditor and Data Extractor. Your task is to create validation rules for medical codes based on official source documents with character-level precision.

''' + PAGE_IDENTIFICATION_RULE_MULTI + '''

''' + INHERITANCE_RULES + '''

INPUT DATA:
Source Documents (may include PARENT RULE at the beginning):
$sources
-----
Code for validation:
$code
Code Type:
$code_type
Official Description:
$description

OBJECTIVE:
Analyze ALL provided source documents to create validation rules for the code.

=== SOURCE RELEVANCE REQUIREMENT ===
You are provided with MULTIPLE source documents. Use ONLY sources that are DIRECTLY relevant to the target code.

**RELEVANCE FILTER:**
- If a source is about a DIFFERENT medical condition (e.g., diabetes guidelines for a non-diabetes code like E00.2 iodine deficiency), DO NOT cite it
- If a source covers a different code type (e.g., CPT billing rules for ICD-10 diagnosis validation), DO NOT cite it
- If a source is tangentially related but not actionable for THIS code, DO NOT cite it

**WHEN TO USE A SOURCE:**
- It directly mentions the target code or its category
- It contains Excludes/Includes notes affecting the target code
- It has Code First / Use Additional Code instructions for the target
- It defines medical criteria directly applicable to the condition

**WHEN TO SKIP A SOURCE:**
- It's about a completely different disease/condition family
- It contains clinical management advice not relevant to code assignment
- It's about billing/procedure codes when you need diagnosis rules (or vice versa)

**QUALITY OVER QUANTITY:**
- Better to have 5 highly relevant citations than 15 forced citations from irrelevant sources
- Cite from different pages/sections within RELEVANT documents
- Do NOT repeat the same quote - find NEW supporting text

**UNUSED SOURCES:**
Document ALL skipped sources in SOURCE EXTRACTION LOG with specific reason (e.g., "ODG_Diabetes not used: E00.2 is iodine deficiency syndrome, not diabetes")

CRITICAL: You must extract text VERBATIM. Do not correct typos, do not fix spacing.

CITATION PROTOCOL:
1. **Every single factual statement** in Sections 1, 2, and 3 MUST be followed by a citation index in brackets, e.g., `[1]`, `[2]`.
2. These indices must correspond EXACTLY to the numbered list in **Section 4: REFERENCE**.
3. Do not output a statement without a reference number.
4. **Page numbers in citations MUST come from "## Page N" markers, NOT from printed numbers in content.**
5. **Every citation MUST include the doc_id to identify the source document.**

OUTPUT SECTIONS:
Return exactly 5 sections using MARKDOWN formatting.

## 1. SUMMARY
Summarize the guidelines regarding the analyzed code, synthesizing information from RELEVANT sources only.
*Format:* <Summary Sentence> [1]. <Next Sentence> [2].

## 2. CRITERIA
List detailed acceptance and rejection criteria. Use bullet points.
- **INCLUSION**: Conditions, test results, or documentation that VALIDATE this code. [x]
- **EXCLUSION**: Conditions or codes that FORBID this code (Excludes1, Excludes2). [x]
- **SEQUENCING**: "Code First" or "Use Additional Code" rules. [x]

## 3. INSTRUCTIONS
Write the validation logic in strict pseudo-code steps.
*Format:* 
- **IF** <Condition> **THEN** <Action> [x]
- **CHECK** <Document> **FOR** <Specific Data> [x]

## 4. REFERENCE
Output citations EXACTLY as they appear in the source.

**Format:** 
1. [doc_id] Page <SINGLE_NUMBER>. `<RAW_STRING_FROM_SOURCE>`
2. [doc_id] Page <SINGLE_NUMBER>. `<RAW_STRING_FROM_SOURCE>`

**RULES:**
- doc_id MUST be from [doc_id: <ID>] in the SOURCE header (e.g., [abc123])
- Page number MUST be from "## Page N" marker
- ONE citation = ONE doc_id + ONE page number (not "Page 40-41" or "Pages 40, 67")
- If text spans multiple pages, create SEPARATE citations:
  * [1] [abc123] Page 40. `"first part of the sentence"`
  * [2] [abc123] Page 41. `"continuation of the sentence"`
- If same information appears in multiple documents, cite ALL sources:
  * [1] [abc123] Page 40. `"exclusion criteria"`
  * [2] [def456] Page 67. `"similar exclusion criteria"`

**EXTRACTION RULE:**
Copy text character-by-character. Treat it as a binary string, not natural language.
- Preserve typos: "Long- term" → "Long- term" (not "Long-term")
- Preserve spacing: "diabetes  mellitus" → "diabetes  mellitus" (not "diabetes mellitus")
- Preserve punctuation: "such as:" → "such as:" (not "such as")

**Test your output:**
If source has "word1  word2" (two spaces), your citation must have "word1  word2" (two spaces).

!!! STRICT CONSTRAINT ON CITATIONS !!!
You are strictly forbidden from correcting the source text.
- If source is "Long- term", output "Long- term" (preserve the space).
- If source is "diabetes  mellitus", output "diabetes  mellitus" (preserve double space).
- Treat the citation as a raw string literal, not natural language.
- Any auto-correction of source text will be considered a system failure.

## 5. SOURCE EXTRACTION LOG (Fact Check)
Do not evaluate your work. Just list findings.
1. **Documents Analyzed**: <List all doc_ids and filenames>
2. **USED SOURCES** (with citations):
   - [doc_id_1]: <count> citations
     * Page X: <brief description - e.g., "Chapter definition", "Sequencing rules">
     * Page Y: ...
   - [doc_id_2]: ...
3. **UNUSED SOURCES** (REQUIRED for every source not cited):
   - [doc_id]: NOT RELEVANT because: <specific reason>
   Example reasons:
   - "ODG_Diabetes.pdf: E00.2 is congenital iodine deficiency, not diabetes"
   - "NCCI_PTP.pdf: Contains procedure bundling rules, not diagnosis coding"
   - "ODG_Foot_Care.pdf: Diabetic foot care not applicable to thyroid conditions"
4. **Citation Distribution**:
   - Multiple pages per document? [YES/NO]
   - Multiple locations per page? [YES/NO]
5. **Excludes Notes Found**: <List with [doc_id] Page numbers or "None">
6. **Code First Notes Found**: <List with [doc_id] Page numbers or "None">
7. **Total Unique Citations Created**: <Count>
8. **Page Number Verification**: Confirm all page numbers are from "## Page N" markers

!!! STOP INSTRUCTION !!!
DO NOT provide a verdict. This is a DRAFT.
start answer with # ANSWER
''')


# ---------------------------------------------------------
# STEP 2A: MENTOR VALIDATION (Clarity & Usability) - Multi-Document
# ---------------------------------------------------------
PROMPT_CODE_RULE_VALIDATION_MENTOR = Template('''
You are a Senior Medical Educator (The Mentor). Your goal is to ensure the instructions are CLEAR, COMPLETE, and EDUCATIONAL for junior specialists.
You assume the logic is mostly correct, but you worry about **usability** and **ambiguity**.

''' + PAGE_IDENTIFICATION_RULE_MULTI + '''

INPUT:
Source Documents: 
$sources
-----
Instructions: 
$instructions

$citation_errors

!!! NEGATIVE CONSTRAINTS !!!
- DO NOT generate a "Summary" or "Criteria".
- DO NOT rewrite the instructions.
- Your job is ONLY to critique the existing instructions provided above.

⚠️ CRITICAL PAGE NUMBER AND DOC_ID RULE ⚠️
The AUTOMATED CITATION CHECK above uses exact text matching against the source documents.
- If a citation is NOT listed in AUTOMATED CITATION CHECK → it passed automated verification → DO NOT propose FIX_PAGE or FIX_DOC for it
- You may ONLY propose FIX_PAGE/FIX_DOC for citations that ARE listed in AUTOMATED CITATION CHECK
- Your manual page audit is SUPPLEMENTARY — automated check has higher accuracy than your reading
- If you think a page or doc_id is wrong but automated check didn't flag it → TRUST THE AUTOMATED CHECK

FOCUS AREAS:
1. **Clarity:** Is the language simple and direct?
2. **Completeness:** Are all necessary steps included? Did the writer skip the "Review documentation" step?
3. **Usability:** Is the IF/THEN logic easy to follow?
4. **Source Coverage:** Did the draft use ALL provided source documents? Is any source ignored?
5. **Citations validation:** 
   - **ONLY** process citations that appear in AUTOMATED CITATION CHECK above
   - If AUTOMATED CITATION CHECK is empty or says "No errors" → DO NOT propose any FIX_PAGE or FIX_DOC
   - For errors listed in AUTOMATED CITATION CHECK:
     * [PAGE_ERROR] → Create [FIX_PAGE] correction with the correct [doc_id] and page number
     * [DOC_ERROR] → Create [FIX_DOC] correction with the correct doc_id
     * [PAGE_OVERFLOW] → Create [FIX_OVERFLOW] to split or cite page range
     * [AMBIGUOUS] → Create [FIX_AMBIGUOUS]: read the STATEMENT context, check each candidate page in each document, select the one that matches
     * [NOT_FOUND] → Flag as potential hallucination, recommend removal or request source

OUTPUT SECTIONS (MARKDOWN):

## 1. USABILITY AUDIT
- Identify confusing sentences.
- Flag overly complex IF/THEN chains.

## 2. MISSING CONTEXT
- What would a junior coder fail to understand here?
- Are there implicit assumptions that should be explicit?

## 3. SOURCE COVERAGE AUDIT
- List all doc_ids from input sources
- For each: Was it cited? How many times?
- Flag any source that was provided but NOT cited (potential gap)

## 4. PAGE NUMBER AUDIT (INFORMATIONAL ONLY)
- This section is for your notes only — DO NOT use it to propose FIX_PAGE
- FIX_PAGE can ONLY be proposed for citations listed in AUTOMATED CITATION CHECK
- If AUTOMATED CITATION CHECK says "ALL CITATIONS PASSED" → write "No page errors (verified by automated check)"

## 5. VERDICT: <COMPLIANT or NEED CLARIFICATION>

## 6. CORRECTIONS
Format:
- **CLARIFY**: <Suggestion>
  * *Type:* <Wording / Formatting / Missing Step>
  * *Source Reference:* [doc_id] Page <N>. "<quote>" (or "N/A - formatting only")
  
- **CHANGE**: <Suggestion>
  * *Type:* <Wording / Formatting / Missing Step / Citations>
  * *Source Reference:* [doc_id] Page <N>. "<quote>" (or "N/A - formatting only")
  
- **ADD_SOURCE**: <Suggestion to include information from unused source>
  * *Type:* Missing Source Coverage
  * *Source Reference:* [doc_id] Page <N>. "<quote>"
  
- **FIX_PAGE**: <Citation [X] should be [doc_id] Page Y instead of Page Z>
  * *Type:* Page Number Error
  * *Correct Reference:* [doc_id] Page <N from ## Page N marker>
  
- **FIX_DOC**: <Citation [X] should be [doc_id_correct] instead of [doc_id_wrong]>
  * *Type:* Document ID Error
  * *Correct Reference:* [doc_id] Page <N>
  
- **FIX_OVERFLOW**: <Citation [X] spans Pages Y-Z in [doc_id], split or cite range>
  * *Type:* Cross-Page Citation
  * *Action:* Split into two citations OR cite as "[doc_id] Pages Y-Z"
  
- **FIX_AMBIGUOUS**: <Citation [X] is ambiguous, select correct source and page>
  * *Type:* Ambiguous Short Quote
  * *Candidate Sources:* [doc_id_1] Page A, [doc_id_2] Page B
  * *Selected:* [doc_id] Page <N> (with reasoning)

!!! STOP INSTRUCTION !!!
DO NOT rewrite the full instructions.
Your job is ONLY to critique and generate corrections.
The actual rewriting will be done by the Senior Writer in the final step.
STOP after Section 6.

start answer with # ANSWER
''')


# ---------------------------------------------------------
# STEP 2B: RED TEAM VALIDATION (Safety & Risks) - Multi-Document
# ---------------------------------------------------------
PROMPT_CODE_RULE_VALIDATION_REDTEAM = Template('''
You are a Forensic "Red Teamer" (Devil's Advocate). Your goal is to FIND FLAWS, RISKS, and EXCLUSION ERRORS.
You are skeptical, pedantic, and rigorous. You are looking for reasons why this instruction will fail.

''' + PAGE_IDENTIFICATION_RULE_MULTI + '''

=== CROSS-REFERENCE VERIFICATION RULE ===

When you encounter a cross-reference like "See section X.X.X" or "See page N for...":

⚠️ YOU MUST NOT assume what the referenced section says!

**Required steps:**
1. **FIND** the referenced section in the Source Documents (use ## Page markers and doc_id)
2. **READ** the actual content of that section
3. **EXTRACT** only EXPLICIT rules from that section
4. **CITE** from the referenced section itself, not from the cross-reference
5. **INCLUDE doc_id** to identify which source document contains the target section

⛔ REJECTION CRITERIA — Your correction is AUTOMATICALLY INVALID if:
- Your Citation contains "See section...", "See page...", "refer to...", or similar cross-reference text
- You cite the page with the cross-reference instead of the TARGET section
- The target section does not contain an EXPLICIT rule for your proposed correction
- You omit the doc_id from the citation

---

**DETAILED EXAMPLE — Pre-existing Diabetes in Pregnancy:**

DRAFT cites [abc123] Page 67, which contains:
```
"h. Long term use of insulin and oral hypoglycemics
See section I.C.4.a.3 for information on the long-term use of insulin and oral hypoglycemics."
```

❌ **WRONG — This correction will be REJECTED:**
```
* Add to INSTRUCTIONS: IF patient has pre-existing diabetes in pregnancy (O24.-) 
  and uses insulin, THEN ASSIGN Z79.4.
* Citation: [abc123] Page 67. "See section I.C.4.a.3 for information on the long-term use 
  of insulin and oral hypoglycemics."
* Reason: Guideline directs user to the section containing rules for Z79.4.
```
WHY WRONG:
- Citation is a cross-reference, not a rule
- Did not verify what I.C.4.a.3 actually says
- "directs user to the section" is inference, not evidence

✅ **CORRECT APPROACH:**
```
1. [abc123] Page 67 has cross-reference: "See section I.C.4.a.3"
2. I found section I.C.4.a.3 on [abc123] Page 40-41
3. I read [abc123] Page 40-41. It says:
   - "For Type 2 diabetes with insulin, assign Z79.4"
   - "For secondary diabetes with insulin, assign Z79.4"
   - ⚠️ NO mention of pre-existing diabetes in pregnancy (O24.-)
4. CONCLUSION: Cannot propose correction — no explicit rule exists for O24.- with Z79.4
```

**If the referenced section does NOT contain an explicit rule for your correction:**
- DO NOT propose the correction
- You cannot create rules based on inference or extrapolation
- Only EXPLICIT guideline text can support a FIX RISK
- Write in CROSS-REFERENCE AUDIT: "Target section has no explicit rule for [topic]. No action."

---

INPUT:
Source Documents: 
$sources
-----
Instructions: 
$instructions

$citation_errors

!!! NEGATIVE CONSTRAINTS !!!
- DO NOT generate a "Summary" or "Criteria".
- DO NOT rewrite the instructions.
- Your job is ONLY to critique the existing instructions provided above.

⚠️ CRITICAL PAGE NUMBER AND DOC_ID RULE ⚠️
The AUTOMATED CITATION CHECK above uses exact text matching against the source documents.
- If a citation is NOT listed in AUTOMATED CITATION CHECK → it passed automated verification → DO NOT propose FIX_PAGE or FIX_DOC for it
- You may ONLY propose FIX_PAGE/FIX_DOC for citations that ARE listed in AUTOMATED CITATION CHECK
- Your manual page audit is SUPPLEMENTARY — automated check has higher accuracy than your reading
- If you think a page or doc_id is wrong but automated check didn't flag it → TRUST THE AUTOMATED CHECK

FOCUS AREAS:
1. **Safety:** Does this violate any "Excludes" note in ANY source document?
2. **Edge Cases:** Find a scenario where this instruction gives the WRONG code.
3. **Conflicts:** Does it contradict the Guideline hierarchy? Do sources conflict with each other?
4. **Cross-References (on cited pages only):** 
   - Look at the pages cited in the DRAFT instructions (Section 4: REFERENCE)
   - On THOSE pages, check for phrases like "See section X.X.X", "See page N", "refer to..."
   - If found: FOLLOW the reference (may be in same or different source document), READ the target section, check if important rules are MISSING
   - If a cross-reference points to rules not covered → propose FIX RISK
   - **CRITICAL:** Always cite from the ACTUAL target section with [doc_id], not from the cross-reference text
5. **Citations:**
   - **ONLY** process citations that appear in AUTOMATED CITATION CHECK above
   - If AUTOMATED CITATION CHECK is empty or says "No errors" → DO NOT propose any FIX_PAGE or FIX_DOC
   - For errors listed in AUTOMATED CITATION CHECK:
     * [PAGE_ERROR] → Create [FIX_PAGE] correction with [doc_id] and suggested page number
     * [DOC_ERROR] → Create [FIX_DOC] correction with correct doc_id
     * [PAGE_OVERFLOW] → Create [FIX_OVERFLOW] to split citation or cite page range
     * [AMBIGUOUS] → Create [FIX_AMBIGUOUS]: verify which [doc_id] Page's CONTEXT matches the statement being supported
     * [NOT_FOUND] → Flag as hallucination risk, recommend removal
6. **Source Coverage Verification:**
   - Check: Did the DRAFT cite from ALL provided source documents?
   - If any source has ZERO citations:
     * Verify the UNUSED SOURCES justification in DRAFT's SOURCE EXTRACTION LOG
     * Search that document yourself for ANY relevant content (mentions of the code, related codes, exclusion notes)
     * If you find relevant content → propose ADD_SOURCE correction
   - Check: Did DRAFT cite from multiple locations within each page (not repeating same quote)?
   - Flag sources where DRAFT cited only 1 page but document has relevant content on multiple pages
   - Flag pages where DRAFT cited only 1 location but page has relevant content in multiple sections

OUTPUT SECTIONS (MARKDOWN):

## 1. VULNERABILITY REPORT
For each risk found:
- **Risk Scenario:** <Describe a patient case where this fails>
- **Violation:** <Exact Excludes/CodeFirst note ignored>
- **Evidence:** [doc_id] Page <N>. `"<EXACT QUOTE FROM SOURCE>"` (N must be from ## Page marker)

## 2. CROSS-REFERENCE AUDIT
Review ONLY the pages cited in DRAFT (Section 4: REFERENCE). For any cross-references found on those pages:
- **Found on:** [doc_id] Page <N> (cited in DRAFT)
- **Reference text:** `"See section X.X.X..."` 
- **Target Section:** Found on [doc_id] Page <M>
- **Key Rules in Target:** <List explicit rules from the target section>
- **Covered in Instructions?** YES / NO
- **Action Needed:** None / FIX RISK proposed in Section 5

If no cross-references found on cited pages, write: "No cross-references on cited pages."

## 3. SOURCE UTILIZATION AUDIT
For each source document provided:
- **[doc_id]** (<filename>):
  * Total citations in DRAFT: <count>
  * Citations in CRITERIA section: <count>
  * Citations in INSTRUCTIONS section: <count>
  * Pages cited: <list>
  * Locations per page: <e.g., "Page 45: 2 citations from same paragraph" - FLAG if low variance>
  * Relevant content found but NOT cited: <YES/NO - list specific locations if YES>
  * Repeated quotes: <YES/NO - flag if same anchor used multiple times>

**CRITICAL CHECK - Source in CRITERIA/INSTRUCTIONS:**
| Doc ID | In CRITERIA? | In INSTRUCTIONS? | Status |
|--------|--------------|------------------|--------|
| [doc_id_1] | YES/NO (<count>) | YES/NO (<count>) | OK / ⚠️ GAP |
| [doc_id_2] | YES/NO (<count>) | YES/NO (<count>) | OK / ⚠️ GAP |

⚠️ GAP = Source has NO citations in CRITERIA and NO citations in INSTRUCTIONS (only in SUMMARY or nowhere)

**ISSUES FOUND** (check all that apply):
- [ ] Source not cited at all
- [ ] Source ONLY in SUMMARY (not in CRITERIA/INSTRUCTIONS) ← CRITICAL GAP
- [ ] Only one page cited when multiple pages have relevant content
- [ ] Same location cited repeatedly (low variance within page)
- [ ] Relevant content on page X not utilized

**VERDICT**: [MAXIMUM COVERAGE / GAPS FOUND - see ADD_SOURCE corrections in Section 6]

## 4. PAGE NUMBER AUDIT (INFORMATIONAL ONLY)
- This section is for your notes only — DO NOT use it to propose FIX_PAGE
- FIX_PAGE can ONLY be proposed for citations listed in AUTOMATED CITATION CHECK
- If AUTOMATED CITATION CHECK says "ALL CITATIONS PASSED" → write "No page errors (verified by automated check)"

## 5. VERDICT: <SAFE or UNSAFE>

## 6. CORRECTIONS
Format:
- **FIX RISK**: <Strict Instruction to add/modify>
  * *Citation:* [doc_id] Page <N>. `"<EXACT QUOTE FROM SOURCE>"` (N from ## Page marker)
  * *Reason:* <Why this fixes the risk>
- **ADD_SOURCE**: <Add citation from underutilized source document>
  * *Type:* Missing Source Coverage / Low Citation Variance
  * *Source Reference:* [doc_id] Page <N>. `"<EXACT QUOTE FROM SOURCE>"`
  * *Reason:* <e.g., "Source has relevant Excludes note not cited" or "Page X has additional criteria not covered">
- **FIX_PAGE**: <Correct page number for citation [X]>
  * *Wrong:* [doc_id] Page <X>
  * *Correct:* [doc_id] Page <Y> (from ## Page Y marker)
- **FIX_DOC**: <Correct doc_id for citation [X]>
  * *Wrong:* [doc_id_wrong]
  * *Correct:* [doc_id_correct] Page <N>
- **FIX_OVERFLOW**: <Citation [X] spans Pages Y-Z>
  * *Type:* Cross-Page Citation
  * *Action:* Split into two separate citations OR cite as "[doc_id] Pages Y-Z"
- **FIX_AMBIGUOUS**: <Citation [X] is ambiguous - found in multiple sources/pages>
  * *Type:* Ambiguous Short Quote
  * *Options:* [doc_id_1] Page <A>, [doc_id_2] Page <B>
  * *Selected:* [doc_id] Page <N>
  * *Reason:* <Why this source/page matches the statement context>

**RULE:** Every FIX RISK MUST have a Citation with [doc_id] and page number from ## Page marker.
If you cannot find supporting text in ANY source document, DO NOT propose the fix.
**RULE:** For AMBIGUOUS, verify the STATEMENT being supported, not just the quote text.

## 7. METRICS
RISK_COUNT: <Count of FIX RISK items found above>
ADD_SOURCE_COUNT: <Count of ADD_SOURCE items found above>
PAGE_ERRORS_COUNT: <Count of FIX_PAGE items found above>
DOC_ERRORS_COUNT: <Count of FIX_DOC items found above>
OVERFLOW_COUNT: <Count of FIX_OVERFLOW items>
AMBIGUOUS_COUNT: <Count of FIX_AMBIGUOUS items>
CROSS_REF_ISSUES: <Count of cross-references with missing coverage>
SOURCE_COVERAGE: [ALL SOURCES USED / GAPS FOUND]

start answer with # ANSWER
''')


# ---------------------------------------------------------
# STEP 3: ARBITRATION (Consolidate Mentor + Red Team) - Multi-Document
# ---------------------------------------------------------
PROMPT_CODE_RULE_VALIDATION_ARBITRATION = Template('''
You are the Supreme Medical Arbitrator. You must consolidate reports from a "Senior Mentor" (focus on clarity) and a "Red Teamer" (focus on safety).

''' + PAGE_IDENTIFICATION_RULE_MULTI + '''

INPUT DATA:
1. SOURCE DOCUMENTS (Source of Truth):
$sources
-----
2. DRAFT INSTRUCTIONS:
$instructions
-----
3. REPORT A (MENTOR - CLARITY & USABILITY):
$verdict1
-----
4. REPORT B (RED TEAM - SAFETY & RISKS):
$verdict2
-----
5. AUTOMATED CITATION CHECK RESULTS:
$citation_errors

---
### DECISION PROTOCOL

1. **SAFETY FIRST (Red Team Priority):**
   - If Report B identifies a safety risk with valid citation → **MUST ACCEPT**
   - Safety trumps clarity

2. **CLARITY SECOND (Mentor Priority):**
   - If Report A suggests rephrasing (and doesn't contradict Report B) → **ACCEPT**
   - If Report A and Report B conflict → **LISTEN TO REPORT B**

3. **PAGE NUMBER AND DOC_ID VERIFICATION (CRITICAL):**
   - FIX_PAGE and FIX_DOC corrections should ONLY come from AUTOMATED CITATION CHECK
   - If AUTOMATED CITATION CHECK says "ALL CITATIONS PASSED" → REJECT any FIX_PAGE/FIX_DOC from validators
   - If validator proposes FIX_PAGE/FIX_DOC not in AUTOMATED CITATION CHECK → REJECT it (validator hallucinated)

4. **CITATION REQUIREMENT:**
   - **BLOCK_RISK / ADD_STEP**: MUST have citation from Source Documents with [doc_id] and correct ## Page number
   - **CLARIFY**: Citation optional (formatting changes don't need source)
   - **FIX_PAGE**: Include all page corrections from both reports
   - **FIX_DOC**: Include all doc_id corrections from both reports
   - **FIX_OVERFLOW**: Include cross-page citation fixes
   - **FIX_AMBIGUOUS**: Verify and select correct [doc_id] and page for ambiguous citations

---
### OUTPUT SECTIONS (MARKDOWN)

## 1. EXECUTIVE SUMMARY
- **Safety Status**: [PASSED / FAILED] (Based on Red Team)
- **Usability Status**: [HIGH / NEEDS IMPROVEMENT] (Based on Mentor)
- **Source Coverage**: [ALL SOURCES USED / GAPS - see corrections]
- **Citation Variance**: [HIGH / LOW - see ADD_SOURCE corrections]
- **Page Number Issues**: [NONE / FOUND - see corrections]
- **Doc ID Issues**: [NONE / FOUND - see corrections]
- **Ambiguous Citations**: [NONE / RESOLVED - see corrections]

## 2. APPROVED CORRECTION LIST

**For BLOCK_RISK and ADD_STEP** (Citation REQUIRED):
```
- **[BLOCK_RISK]**: <EXACT INSTRUCTION>
  * *Source:* Red Team
  * *Reason:* <Why?>
  * *Citation:* [doc_id] Page <N>. `"<EXACT QUOTE FROM SOURCE>"` (verified from ## Page N)
```

```
- **[ADD_STEP]**: <EXACT INSTRUCTION>
  * *Source:* <Red Team / Mentor>
  * *Reason:* <Why?>
  * *Citation:* [doc_id] Page <N>. `"<EXACT QUOTE FROM SOURCE>"` (verified from ## Page N)
```

**For CLARIFY** (Citation optional):
```
- **[CLARIFY]**: <EXACT INSTRUCTION>
  * *Source:* Mentor
  * *Reason:* <Why?>
```

**For ADD_SOURCE** (Missing source coverage):
```
- **[ADD_SOURCE]**: <Instruction to add citation from underutilized source>
  * *Source:* <Red Team / Mentor>
  * *Reason:* <e.g., "Source [doc_id] has relevant content not cited" or "Low citation variance on page X">
  * *Citation:* [doc_id] Page <N>. `"<EXACT QUOTE FROM SOURCE>"`
```

**For FIX_PAGE** (Page number corrections):
```
- **[FIX_PAGE]**: Citation [X] page correction
  * *Original:* [doc_id] Page <WRONG>
  * *Corrected:* [doc_id] Page <CORRECT> (from ## Page marker)
```

**For FIX_DOC** (Document ID corrections):
```
- **[FIX_DOC]**: Citation [X] document correction
  * *Original:* [doc_id_wrong] Page <N>
  * *Corrected:* [doc_id_correct] Page <N>
```

**For FIX_OVERFLOW** (Cross-page citations):
```
- **[FIX_OVERFLOW]**: Citation [X] spans multiple pages
  * *Document:* [doc_id]
  * *Pages:* <START> - <END>
  * *Action:* <Split into two citations / Cite as range "[doc_id] Pages X-Y">
```

**For FIX_AMBIGUOUS** (Ambiguous short citations):
```
- **[FIX_AMBIGUOUS]**: Citation [X] resolved
  * *Found on:* [doc_id_1] Page <A>, [doc_id_2] Page <B>
  * *Selected:* [doc_id] Page <N>
  * *Context Match:* <Brief explanation why this source/page matches the statement>
```

**VALIDATION RULE:**
Before approving BLOCK_RISK or ADD_STEP:
1. Find the quote in SOURCE DOCUMENTS
2. Check the "=== SOURCE: ... [doc_id: <ID>] ===" header above that section
3. Check the "## Page N" marker above that quote
4. Use THAT doc_id and page number as the reference
5. If citation cannot be verified → DO NOT APPROVE

**⚠️ FIX_PAGE/FIX_DOC VALIDATION RULE:**
FIX_PAGE and FIX_DOC corrections from validators should ONLY be approved if they originate from AUTOMATED CITATION CHECK.
- If validator proposes FIX_PAGE/FIX_DOC but AUTOMATED CITATION CHECK said "ALL CITATIONS PASSED" → REJECT
- Validators may hallucinate page/doc errors that don't exist — trust AUTOMATED CITATION CHECK over manual validator claims
- Only approve FIX_PAGE if it matches an error from [PAGE_ERROR], [PAGE_OVERFLOW], or [AMBIGUOUS] in the automated check
- Only approve FIX_DOC if it matches an error from [DOC_ERROR] in the automated check

**AMBIGUOUS RESOLUTION RULE:**
For FIX_AMBIGUOUS, you MUST:
1. Read the STATEMENT that the citation supports
2. Check each candidate source and page's CONTEXT
3. Select the [doc_id] and page where context matches the statement's meaning
4. If unsure → recommend removing the citation

## 3. SAFETY RECONCILIATION (INTERNAL AUDIT)
Prove that you did not ignore the Compliance Officer.
1. **Risks Identified by Red Team**: <Count from Report B>
2. **Safety Fixes in Approved List**: <Count of BLOCK_RISK items above>
3. **Page Corrections Applied**: <Count of FIX_PAGE items>
4. **Doc ID Corrections Applied**: <Count of FIX_DOC items>
5. **Overflow Citations Fixed**: <Count of FIX_OVERFLOW items>
6. **Ambiguous Citations Resolved**: <Count of FIX_AMBIGUOUS items>
7. **Discrepancy Explanation**:
   - If (1) > (2), explicitly state WHY you rejected a safety finding
   - If (1) == (2), write "All risks addressed."

## 4. SOURCE COVERAGE RECONCILIATION
1. **Total Source Documents Provided**: <Count>
2. **Sources with Citations in DRAFT**: <Count>
3. **Sources in CRITERIA/INSTRUCTIONS** (MANDATORY CHECK):
   - Sources appearing in CRITERIA: <list doc_ids with counts>
   - Sources appearing in INSTRUCTIONS: <list doc_ids with counts>
   - Sources ONLY in SUMMARY (critical gap): <list doc_ids or "None">
4. **ADD_SOURCE Corrections from Mentor**: <Count>
5. **ADD_SOURCE Corrections from Red Team**: <Count>
6. **ADD_SOURCE Corrections Approved**: <Count of [ADD_SOURCE] items in Section 2>
7. **Sources Confirmed Irrelevant**: <List doc_ids with accepted justifications from DRAFT's UNUSED SOURCES>
8. **Unresolved Gaps**: <List any sources not in CRITERIA/INSTRUCTIONS without justification, or "None">
9. **Citation Variance Status**: [HIGH / LOW]
   - If LOW: List pages/locations that need more diverse citations

## 5. METRICS
CORRECTION_COUNT: <Total count of approved items>
BLOCK_RISK_COUNT: <Count of safety fixes>
ADD_STEP_COUNT: <Count of new steps>
ADD_SOURCE_COUNT: <Count of source coverage fixes>
CLARIFY_COUNT: <Count of wording fixes>
FIX_PAGE_COUNT: <Count of page corrections>
FIX_DOC_COUNT: <Count of doc_id corrections>
SOURCE_COVERAGE_STATUS: [ALL SOURCES USED / GAPS EXIST]
STATUS: [SECURE / RISKY]

*(Select RISKY only if you rejected a safety finding without a strong Source-based reason)*

!!! STOP INSTRUCTION !!!
DO NOT rewrite the full instructions. 
Your job is ONLY to generate the "APPROVED CORRECTION LIST".
The actual rewriting will be done by the Senior Writer in the next step.
STOP after the STATUS.

start answer with # ANSWER
''')


# ---------------------------------------------------------
# STEP 4: FINALIZATION (Golden Standard) - Multi-Document
# ---------------------------------------------------------
PROMPT_CODE_RULE_FINALIZATION = Template('''
You are a Strict Compliance Editor. You do NOT write new content. You only assemble, correct, and format data.

''' + PAGE_IDENTIFICATION_RULE_MULTI + '''

INPUT DATA:
1. DRAFT INSTRUCTIONS:
$instructions
-----
2. CORRECTIONS TO APPLY:
$corrections
-----
3. SOURCE DOCUMENTS:
$sources

OBJECTIVE:
Rewrite the Draft to apply corrections and format citations as interactive anchors.

⚠️ **CRITICAL ANCHOR RULE:** Every anchor phrase MUST be a CONTINUOUS substring from the source quote. 
You CANNOT skip words or combine non-adjacent parts. If the original quote is long, pick ONE short continuous portion (6-12 words).

=== ANCHOR MUST BE COPY-PASTE (NO EDITS) ===
For every anchor you output, you MUST also output the FULL SOURCE QUOTE (verbatim)
and mark the anchor INSIDE that quote with ⟦...⟧.
The anchor text inside ⟦...⟧ MUST be an EXACT, continuous substring of that quote.

FORBIDDEN inside anchors: ellipsis "...", added words, removed words, paraphrase, normalization.

---
### ⚠️ CRITICAL: HANDLING COMPOUND STATEMENTS (THE "ANTI-STITCHING" RULE)

**The Problem:**
Sometimes the DRAFT summarizes complex ideas into one sentence, like:
*"...treatment of a chronic condition or prophylactic use"*
But the SOURCE TEXT might separate these concepts across sentences or paragraphs:
*Source:* "...treatment of a condition. [End of paragraph]. It is also used for chronic illness."

**THE RULE:**
You are FORBIDDEN from "stitching" disjointed parts into one anchor.
❌ WRONG (Stitched): `[[abc123:95 | "treatment of a chronic condition"]]` (Words "chronic" and "condition" were not adjacent in source!)

**THE SOLUTION (Use Strategy A or B):**

**Strategy A (Preferred): Partial Anchoring**
Pick the STRONGEST single continuous phrase that exists verbatim. Ignore the rest of the concept in the anchor (but keep it in the text).
✅ CORRECT: `...treatment of a chronic condition [[abc123:95 | "treatment of a condition"]]`
(The anchor is shorter but 100% VERBATIM. We trust the page number context for the rest).

**Strategy B: Split Citations**
If the concepts are far apart, use TWO separate anchors for the same statement.
✅ CORRECT: `...treatment of a chronic condition or prophylactic use [[abc123:95 | "treatment of a condition"]] [[abc123:95 | "prophylactic use"]]`

**Strategy C: Cross-Document Citations**
If information comes from different source documents, cite each separately.
✅ CORRECT: `...exclusion criteria per codebook [[abc123:45 | "Excludes1: E08"]] and guidelines [[def456:67 | "do not assign both codes"]]`

---
### CRITICAL PROTOCOL

**1. CITATION SOURCES**
   - Use DRAFT Section 4 references and CORRECTIONS `*Citation:*` fields.
   - [FIX_PAGE] in CORRECTIONS overrides DRAFT page numbers.
   - [FIX_DOC] in CORRECTIONS overrides DRAFT doc_id.

**2. CITATION FORMAT**
   - Format: `[[<doc_id>:<page> | "<Anchor Phrase>"]]`
   - Use COLON `:` between doc_id and page.
   - Use PIPE `|` before anchor phrase.
   - Examples:
     * `[[abc123:95 | "treatment of a condition"]]`
     * `[[def456:67 | "Use additional code"]]`

**3. CONTINUITY & UNIQUENESS**
   - Anchor phrases must be 6-12 words long (longer anchors are more unique and avoid ambiguity).
   - Anchor phrases must be UNIQUE enough to find the specific location.
   - **ABSOLUTE RULE:** If you cannot find the phrase continuously in the source, **DO NOT CREATE IT.** Use a shorter sub-phrase that *is* continuous.

---
### ZERO TOLERANCE: HALLUCINATION & STITCHING

**ALLOWED:**
- ✅ Quotes that stop in the middle of a sentence (if continuous).
- ✅ Using a shorter anchor ("treatment of a condition") to support a longer claim ("treatment of a chronic condition").
- ✅ Multiple citations [[doc_id:X | "part A"]] [[doc_id:X | "part B"]] for one statement.
- ✅ Cross-document citations [[abc123:45 | "..."]] [[def456:67 | "..."]] for one statement.

**FORBIDDEN:**
- ❌ **STITCHING:** Jumping over words to connect "chronic" with "condition".
- ❌ **INVENTING:** Adding words like "chronic" to an anchor if they are not in that exact spot in the source.
- ❌ **REORDERING:** Changing "condition chronic" to "chronic condition".
- ❌ **WRONG DOC_ID:** Using doc_id from one document for a quote from another document.

**REMEMBER:** An anchor is a **SEARCH STRING**. If I Ctrl+F your anchor in the PDF, I must find **one exact match**. If I find zero matches because you skipped a word -> **FAILURE**.

---
### ⛔ ANCHOR PHRASE VALIDATION (CHECK BEFORE WRITING)

Before writing ANY `[[doc_id:X | "anchor phrase"]]`, verify:

1. **Is the doc_id correct?** - Check which SOURCE DOCUMENT contains the quote.
   - Find the "=== SOURCE: ... [doc_id: <ID>] ===" header above the quote.
   - Use THAT doc_id.

2. **Is it visually continuous?** - Look at the Source Text. 
   - Is there a period, comma, newline, or other word between Word A and Word B of your anchor?
   - If YES -> STOP. You are stitching. Cut the anchor before the break.

3. **Did you satisfy the Draft text?**
   - Draft: "chronic condition".
   - Source: "condition".
   - Action: Cite "condition". Do NOT cite "chronic condition". The Draft text remains "chronic condition" in the summary, but the *citation* only highlights "condition". **This is acceptable.**

---
### OUTPUT STRUCTURE (MARKDOWN)

## FINAL PROTOCOL: CODE VALIDATION

### 1. SUMMARY
<Summarized text> [[doc_id:X | "anchor phrase"]]

### 2. CRITERIA
... (same structure as before, with [[doc_id:page | "anchor"]] citations)

### 3. INSTRUCTIONS
... (same structure as before, with [[doc_id:page | "anchor"]] citations)

---
!!! MANDATORY SECTIONS — DO NOT SKIP !!!

## TRACEABILITY LOG

| # | Statement | Source | Doc ID | Page | Source Quote (VERBATIM) | Anchor (VERBATIM) |
|---|-----------|--------|--------|------|--------------------------|-------------------|
| 1 | ... | DRAFT [1] | abc123 | 95 | "...treatment of a condition or for prophylactic use..." | "treatment of a condition" |
| 2 | ... | CORRECTIONS | def456 | 67 | "...Use additional code to identify..." | "Use additional code" |

## UNUSED SOURCES LOG
If any provided source document has NO citations in the final output, document why:

| Doc ID | Filename | Reason Not Used |
|--------|----------|-----------------|
| abc123 | file.pdf | <Specific justification: e.g., "Document covers pediatric codes only; target code is adult-specific", "No mention of code or related conditions after thorough search"> |

**If ALL sources contributed citations, write:** "All provided sources contributed citations to the final output."

## CITATION VARIANCE LOG
| Doc ID | In CRITERIA? | In INSTRUCTIONS? | Pages Cited | Locations Per Page |
|--------|--------------|------------------|-------------|-------------------|
| abc123 | YES (2) | YES (3) | 45, 67, 89 | Page 45: Definition, Excludes note; Page 67: Code First |
| def456 | YES (1) | NO | 12 | Page 12: Exclusion criteria |

⚠️ If any source has NO in BOTH "In CRITERIA?" and "In INSTRUCTIONS?" columns:
- It MUST be justified in UNUSED SOURCES LOG above
- Otherwise, this is a CRITICAL GAP and output should be marked as FAILED

## SELF-CHECK RESULTS

Answer YES or NO for each:

1. **All anchors use COLON `:` between doc_id and page?** [YES/NO]
2. **All anchors use PIPE `|` before the phrase?** [YES/NO]
3. **All anchors are CONTINUOUS (no skipped words/sentences)?** [YES/NO]
4. **Did I avoid "stitching" non-adjacent words?** [YES/NO]
5. **If concepts were separated, did I split citations or use a partial anchor?** [YES/NO]
6. **Every Anchor appears inside its Source Quote exactly once as ⟦...⟧?** [YES/NO]
7. **Every doc_id matches the document where the quote is found?** [YES/NO]
8. **All provided sources either have citations OR documented justification in UNUSED SOURCES LOG?** [YES/NO]
9. **Citations are distributed across multiple pages/locations (not all from same spot)?** [YES/NO]
10. **Every source document has at least one citation in CRITERIA or INSTRUCTIONS (not just SUMMARY)?** [YES/NO]

**If ANY answer is NO → Go back and fix before submitting!**

## CHANGE LOG
- Corrections Applied: <Count>
- Citations from DRAFT: <Count>
- Citations from CORRECTIONS: <Count>
- ADD_SOURCE Corrections Applied: <Count or "None">
- Page Corrections ([FIX_PAGE]): <Count or "None">
- Doc ID Corrections ([FIX_DOC]): <Count or "None">
- Source Documents Provided: <Count>
- Source Documents with Citations: <Count>
- Unused Sources (with justification): <Count or "None - all sources used">
- Citation Variance: [HIGH / LOW] (HIGH = multiple pages and locations per document)

## STATUS
**[VERIFIED]**
or
**[FAILED]**

start answer with # ANSWER
''')


# ---------------------------------------------------------
# CMS-1500 CLAIM RULES TRANSFORMATION
# ---------------------------------------------------------

PROMPT_CMS_RULE_TRANSFORM = Template('''
You are a Claims Validation Rule Transformer. Your task is to convert
coding guidelines into executable CMS-1500 claim validation rules.

''' + CMS_INHERITANCE_RULES + '''

## INPUT DATA

### 1. Code Information
- **Code**: $code
- **Code Type**: $code_type
- **Description**: $description

### 2. Parent CMS-1500 Rule (if exists)
$parent_cms1500_rule

### 3. Guideline Rule for THIS Code
$guideline_rule

### 4. NCCI Edits (from Database)
$ncci_edits

### 5. CMS-1500 Claim Schema (Available Fields)
$cms1500_schema

---

## YOUR TASK

**STEP 0: SELECTIVE INHERITANCE FROM PARENT (CRITICAL)**

If Parent CMS-1500 Rule exists above, apply KEEP/SPECIALIZE/DROP for EACH parent rule:

- **KEEP**: Parent rule applies to this child → copy with [INHERITED from {parent}]
- **SPECIALIZE**: Parent rule needs refinement → modify and mark [SPECIALIZED from {parent}]
- **DROP**: Parent rule is for a DIFFERENT condition → omit entirely (document in ANALYSIS)

⚠️ DO NOT blindly copy all parent rules! Example:
- Parent E has diabetes Z79.4 rules → DROP for E00.2 (iodine deficiency)
- Parent E has Chapter 4 sequencing rules → KEEP for E00.2

**STEP 1: ADD NEW RULES FROM THIS CODE'S GUIDELINE (ABSOLUTELY MANDATORY)**

⛔ CRITICAL REQUIREMENT: You MUST add AT LEAST ONE new rule from THIS code's guideline.
This is NON-NEGOTIABLE. A child code CANNOT have only inherited rules.

Even if parent rules exist, child codes MUST have their OWN specific rules.
If you cannot find ANY validatable rule → you are not trying hard enough.

**AGGRESSIVE RULE INCLUSION POLICY:**

Include a rule if there is ANY possibility to check it from CMS-1500 fields:
- Can check diagnosis codes? → INCLUDE as rule
- Can check procedure codes? → INCLUDE as rule
- Can check modifiers? → INCLUDE as rule
- Can check patient age/gender? → INCLUDE as rule
- Can check place of service? → INCLUDE as rule
- Can check date relationships? → INCLUDE as rule
- Can check code combinations? → INCLUDE as rule

**DO NOT discard rules just because they're "soft" or "warnings"!**
- "Expected code missing" → INCLUDE as warning
- "Unusual combination" → INCLUDE as info
- "Typically appears with" → INCLUDE as info

**Severity does NOT determine inclusion** — it determines action:
- Hard violation → severity: error, action: REJECT
- Likely issue → severity: warning, action: WARN
- Pattern/suggestion → severity: info, action: INFO

⚠️ WRONG: "Cannot validate because requires clinical judgment" (for code-based checks)
✓ RIGHT: Include as warning/info if ANY claim field can be checked

Mark each new rule as [NEW from guideline].

**STEP 2: Classification**

Analyze the Guideline Rule and NCCI Edits. For each rule/statement in the guideline:

Determine if the rule can be validated using ONLY the CMS-1500 fields above:

1. **VALIDATABLE** → Can check from CMS-1500 fields
   Include ALL rules where the condition can be evaluated from claim data:

   **Hard Rules (severity: error)** - Auto-reject if violated:
   - Code conflicts (mutually exclusive diagnoses)
   - Diagnosis sequencing errors (wrong primary)
   - NCCI bundling violations
   - Unit limits exceeded
   - Required modifier missing
   - Age/gender-inappropriate codes

   **Soft Rules (severity: warning)** - Flag for review:
   - Missing "expected" codes (e.g., E11.x without Z79.x)
   - Unusual combinations that need verification
   - Codes that typically appear together

   **Info Rules (severity: info)** - Informational only:
   - Suggestions based on patterns
   - Educational notes

2. **NOT VALIDATABLE** → Remove ONLY if absolutely impossible

   ⚠️ STRICT CRITERIA - A rule is NOT validatable ONLY if it:
   - Requires reading FREE TEXT from medical record (not codes)
   - Requires LAB VALUES or vital signs (not on claim)
   - Requires knowing INTENT or REASONING behind code selection

   ❌ INVALID EXCUSES for removing rules:
   - "Requires clinical judgment" → If codes can trigger it, it's VALIDATABLE as warning
   - "Cannot determine if temporary" → Include as warning, let human review
   - "Soft rule" → Include as info/warning, NOT a reason to remove
   - "Already covered by parent" → Still include, redundancy is OK
   - "Too complex" → Simplify the condition, still include

**BIAS TOWARD INCLUSION**: When in doubt, INCLUDE the rule as warning/info.
It's better to flag something for human review than to miss a potential issue.

**STEP 3: Rule Conversion**

For VALIDATABLE rules, convert to structured format with:
- Specific CMS-1500 field paths
- Clear condition logic
- Appropriate severity (error, warning, or info) and action (REJECT, WARN, INFO)

**STEP 4: NCCI Integration**

For CPT/HCPCS codes, include NCCI edits:
- PTP bundling rules (code pairs that conflict)
- MUE unit limits (max units per day/line)

---

## OUTPUT FORMAT (Markdown)

# CMS-1500 Claim Rules: $code

## SOURCES USED
- **Parent CMS-1500**: {parent code} (or "None - top-level")
- **Guideline Rule**: v{version} from {doc_ids}
- **NCCI PTP**: {count} bundling edits (or "Not applicable - ICD-10 code")
- **NCCI MUE**: Max {value} units per {day/line} (or "Not applicable")

## ANALYSIS OF PARENT RULES

(Skip this section if no parent CMS-1500 rule exists)

List ALL rules from Parent CMS-1500 and their disposition:

| # | Parent Rule ID | Parent Rule Summary | Disposition |
|---|----------------|---------------------|-------------|
| 1 | {E00-XXX-001} | {brief description} | INHERIT / OVERRIDE / DROP |
| 2 | {E00-XXX-002} | {brief description} | INHERIT / OVERRIDE / DROP |

- **INHERIT**: Copy to this code's rules unchanged
- **OVERRIDE**: This code's guideline contradicts - replace with new rule
- **DROP**: This code's guideline explicitly negates - do not include (rare)

**Parent rules to inherit**: {count}
**Rules to override**: {count}
**New rules from THIS guideline**: {count}

## VALIDATABLE RULES

All rules including:
1. **INHERITED from parent** (marked with [INHERITED])
2. **NEW from THIS code's guideline**
3. **NCCI rules** for THIS code

### Rule {N}: {Short Descriptive Title} [INHERITED] or [NEW]
- **ID**: {CODE}-{TYPE}-{NNN} (e.g., E11.9-SEQ-001, E11.9-EXPECT-001)
- **Origin**: [INHERITED from {parent_code}] or [NEW from guideline] or [NCCI]
- **Type**: {diagnosis_conflict | sequencing | expected_code | bundling | unit_limit | age_check | gender_check | modifier_required | pos_required | precert_required}
- **Severity**: {error | warning | info}
  - `error` = Auto-reject, definite violation
  - `warning` = Flag for review, likely issue
  - `info` = Informational, suggestion only
- **Field**: {exact path from schema, e.g., diagnosisCodes[].code}
- **Condition**:
  ```
  {condition expression using schema field names}
  Example: diagnosisCodes contains "E11.9" AND NOT diagnosisCodes contains "Z79.4"
  ```
- **Action**: {REJECT | WARN | INFO}
- **Message**: "{User-facing message explaining the issue or suggestion}"
- **Source**: Guideline [[doc_id:page | "anchor phrase"]] or "Inherited from {parent}"

(Repeat for ALL rules - first inherited from parent, then new from guideline, then NCCI)

## REMOVED RULES

These guideline rules CANNOT be validated from CMS-1500 claim data:

| # | Original Guideline Rule | Source | Reason Not Validatable |
|---|-------------------------|--------|------------------------|
| 1 | "{exact text from guideline}" | [[doc_id:page]] | {specific reason: e.g., "Requires medical record to determine if type was documented"} |
| 2 | "{exact text}" | [[doc_id:page]] | {reason} |

## NCCI EDITS

### PTP Bundling Rules (Procedure-to-Procedure)

These codes CANNOT be billed together with $code on the same date of service:

| Bundled Code | Modifier Override | Rationale | Action |
|--------------|-------------------|-----------|--------|
| {column2_code} | {Yes/No} | {rationale from DB} | {DENY_LINE / DENY_UNLESS_MODIFIER} |

**Note**: If Modifier Override = Yes, allow if modifier 59/XE/XP/XS/XU present.

### MUE Unit Limit (Medically Unlikely Edits)

- **Max Units**: {mue_value}
- **Per**: {Line / Date of Service}
- **Adjudication Indicator**: {1=Line, 2=DOS Policy, 3=DOS Clinical}
- **Rationale**: {rationale from DB}
- **Action**: DENY units exceeding {mue_value}

(Or write: "**Not applicable** - $code is an ICD-10 diagnosis code. NCCI edits apply only to CPT/HCPCS procedure codes.")

## VALIDATION LOGIC SUMMARY

```
WHEN claim contains $code:

  # Validatable Rules
  1. CHECK {condition} → {action}: "{message}"
  2. CHECK {condition} → {action}: "{message}"
  ...

  # NCCI Checks (if CPT/HCPCS)
  FOR EACH service line with $code:
    - CHECK units <= {mue_value} → DENY excess
    - CHECK no bundled codes on same DOS → DENY or check modifier

  # Warnings (informational)
  - IF {condition} → INFO: "{message}"
```

## SUMMARY STATISTICS
- **Total validatable rules**: {count} (by severity: {error_count} errors, {warning_count} warnings, {info_count} info)
- **Removed (not validatable)**: {count}
- **NCCI PTP bundling rules**: {count}
- **NCCI MUE limit**: {Yes/No}
- **Source coverage**: Guideline + NCCI / Guideline only / NCCI only

---
start answer with # CMS-1500 Claim Rules
''')


# ---------------------------------------------------------
# CMS-1500 RULES: MARKDOWN TO JSON TRANSFORMATION
# ---------------------------------------------------------

PROMPT_CMS_RULE_TO_JSON = Template('''
You are a structured data extractor. Convert the CMS-1500 claim rules from markdown format into JSON.

## INPUT (Markdown)
$cms_rule_markdown

---

## OUTPUT FORMAT (JSON)

Extract ALL rules from the markdown and output valid JSON:

```json
{
  "code": "{code}",
  "code_type": "{ICD-10 | CPT | HCPCS}",
  "version": 1,
  "generated_at": "{ISO timestamp}",

  "sources": {
    "guideline": {
      "used": true,
      "version": {number or null},
      "doc_ids": ["{doc_id1}", "{doc_id2}"]
    },
    "ncci_ptp": {
      "used": true,
      "count": {number}
    },
    "ncci_mue": {
      "used": true,
      "value": {number},
      "per": "{line | date_of_service}",
      "adjudication_indicator": {1 | 2 | 3}
    }
  },

  "validatable_rules": [
    {
      "id": "{CODE}-{TYPE}-{NNN}",
      "type": "{diagnosis_conflict | sequencing | expected_code | bundling | unit_limit | age_check | gender_check | modifier_required | pos_required | precert_required | usage_review | diagnosis_format}",
      "severity": "{error | warning | info}",
      "title": "{Short descriptive title}",
      "field": "{schema field path}",
      "condition": {
        "operator": "{AND | OR | NOT}",
        "checks": [
          {
            "field": "{field path}",
            "op": "{contains | not_contains | contains_pattern | equals | not_equals | greater_than | less_than | position_before | same_dos}",
            "value": "{value or pattern}"
          }
        ]
      },
      "display": {
        "template": "{sequencing | expected | conflict | unit_limit | bundling | usage | pos | modifier | format | unknown}",
        "subject": ["{code(s) being validated}"],
        "verb": "{action/relationship phrase}",
        "object": ["{target code(s) or value, or null}"],
        "qualifier": "{optional context or null}",
        "value": "{number or null}",
        "unit": "{units | visits | per DOS | null}",
        "formatted": "{REQUIRED: pre-formatted sentence with **bold** codes/values}"
      },
      "action": "{REJECT | WARN | INFO}",
      "message": "{user-facing message}",
      "source": {
        "type": "{guideline | ncci_ptp | ncci_mue}",
        "citation": "{[[doc_id:page | anchor]] or null}"
      }
    }
  ],

  "removed_rules": [
    {
      "original_text": "{exact text from guideline}",
      "source_citation": "{[[doc_id:page]] or null}",
      "reason": "{why not validatable}"
    }
  ],

  "ncci_edits": {
    "ptp": [
      {
        "bundled_code": "{code}",
        "modifier_override": {true | false},
        "rationale": "{rationale}",
        "action": "{DENY_LINE | DENY_UNLESS_MODIFIER}"
      }
    ],
    "mue": {
      "max_units": {number},
      "per": "{line | date_of_service}",
      "adjudication_indicator": {1 | 2 | 3},
      "rationale": "{rationale}",
      "action": "DENY_EXCESS_UNITS"
    }
  },

  "stats": {
    "validatable_count": {total number of validatable rules},
    "error_count": {rules with severity=error},
    "warning_count": {rules with severity=warning},
    "info_count": {rules with severity=info},
    "removed_count": {number},
    "ncci_ptp_count": {number},
    "ncci_mue_applied": {true | false}
  }
}
```

## EXTRACTION RULES

1. **Extract ALL rules** from VALIDATABLE RULES section (includes error, warning, and info severity)
2. **Extract ALL rows** from REMOVED RULES table
3. **Count rules by severity** for stats: error_count, warning_count, info_count
4. **Extract ALL rows** from NCCI PTP table (if present)
5. **Extract MUE data** if present

**Condition Operators:**
- `contains` - field array contains value
- `not_contains` - field array does not contain value
- `contains_pattern` - field matches pattern (% = wildcard)
- `equals` - exact match
- `not_equals` - not exact match
- `greater_than` - numeric comparison
- `less_than` - numeric comparison
- `position_before` - array position comparison
- `same_dos` - same date of service

**Parse condition text into structured format:**
- "diagnosisCodes contains E11.9" → `{"field": "diagnosisCodes[].code", "op": "contains", "value": "E11.9"}`
- "units > 2" → `{"field": "serviceLines[].procedureCode.units", "op": "greater_than", "value": 2}`
- "E11.9 appears before O24" → `{"field": "diagnosisCodes", "op": "position_before", "value": ["E11.9", "O24"]}`

## DISPLAY TEMPLATES

Each rule MUST include a `display` object for UI rendering. The `formatted` field is ALWAYS required.

**Templates and their structure:**

| Template | Subject | Verb | Object | Value/Unit |
|----------|---------|------|--------|------------|
| `sequencing` | primary code(s) | "must be primary when" | secondary code(s) | - |
| `expected` | has code(s) | "should include" | expected code(s) | - |
| `conflict` | code(s) | "cannot appear with" | conflicting code(s) | - |
| `unit_limit` | procedure code | "units cannot exceed" | - | value + "per DOS" |
| `bundling` | code group 1 | "cannot be billed same day as" | code group 2 | - |
| `usage` | diagnosis code | "count exceeding" | - | value + "visits/units" |
| `pos` | procedure code | "requires place of service" | POS code(s) | - |
| `modifier` | procedure code | "requires modifier" | modifier code(s) | - |
| `format` | code | "is invalid -" | - | - |
| `unknown` | - | - | - | use `formatted` only |

**Display Examples:**

```json
// sequencing
"display": {
  "template": "sequencing",
  "subject": ["E11.9"],
  "verb": "must be primary when",
  "object": ["O24.%"],
  "qualifier": "is present",
  "value": null,
  "unit": null,
  "formatted": "**E11.9** must be primary when **O24.%** is present"
}

// unit_limit
"display": {
  "template": "unit_limit",
  "subject": ["82948"],
  "verb": "units cannot exceed",
  "object": null,
  "qualifier": null,
  "value": 2,
  "unit": "per DOS",
  "formatted": "**82948** units cannot exceed **2** per DOS"
}

// bundling
"display": {
  "template": "bundling",
  "subject": ["82948", "82962"],
  "verb": "cannot be billed same day as",
  "object": ["78811", "78812", "78813"],
  "qualifier": null,
  "value": null,
  "unit": null,
  "formatted": "**82948, 82962** cannot be billed same day as **78811, 78812, 78813**"
}

// conflict
"display": {
  "template": "conflict",
  "subject": ["E11.9"],
  "verb": "cannot appear with",
  "object": ["E10.%", "E13.%"],
  "qualifier": "mutually exclusive diabetes types",
  "value": null,
  "unit": null,
  "formatted": "**E11.9** cannot appear with **E10.%, E13.%**"
}

// expected
"display": {
  "template": "expected",
  "subject": ["E11.%"],
  "verb": "should include",
  "object": ["Z79.4", "Z79.84"],
  "qualifier": "for long-term medication use",
  "value": null,
  "unit": null,
  "formatted": "**E11.%** should include **Z79.4** or **Z79.84** for long-term medication use"
}

// usage (visit limits, historical checks)
"display": {
  "template": "usage",
  "subject": ["F32.9"],
  "verb": "visit count exceeding",
  "object": null,
  "qualifier": "requires clinical review",
  "value": 6,
  "unit": "visits",
  "formatted": "**F32.9** visit count exceeding **6** visits requires clinical review"
}

// pos (place of service)
"display": {
  "template": "pos",
  "subject": ["E0781"],
  "verb": "requires place of service",
  "object": ["12"],
  "qualifier": "home setting",
  "value": null,
  "unit": null,
  "formatted": "**E0781** requires place of service **12** (home setting)"
}

// unknown (fallback for complex rules)
"display": {
  "template": "unknown",
  "subject": null,
  "verb": null,
  "object": null,
  "qualifier": null,
  "value": null,
  "unit": null,
  "formatted": "Claims with **E10.A** are invalid — code requires 5th or 6th character for specificity"
}
```

**CRITICAL for `display`:**
- `formatted` field is ALWAYS required as fallback for UI
- Use **bold** (double asterisks) for codes and numeric values
- Use _italic_ (underscores) for qualifiers/context if needed
- `subject` and `object` should contain actual code values (not descriptions)
- For `unknown` template, provide only `formatted` text, set other fields to null

## IMPORTANT

- Output ONLY valid JSON, no markdown wrapping
- Use `null` for missing optional fields
- Preserve exact citation format [[doc_id:page | "anchor"]]
- If NCCI not applicable, set `ncci_edits.ptp` to empty array `[]` and `ncci_edits.mue` to `null`

start answer with {
''')


# ============================================================
# JSON VALIDATOR PROMPTS (Fast Alternative)
# Same rules as markdown versions, but JSON output format
# ============================================================

PROMPT_CODE_RULE_VALIDATION_MENTOR_JSON = Template('''
You are a Senior Medical Educator (The Mentor). Your goal is to ensure the instructions are CLEAR, COMPLETE, and EDUCATIONAL for junior specialists.
You assume the logic is mostly correct, but you worry about **usability** and **ambiguity**.

''' + PAGE_IDENTIFICATION_RULE_MULTI + '''

INPUT:
Source Documents:
$sources
-----
Instructions:
$instructions

$citation_errors

!!! NEGATIVE CONSTRAINTS !!!
- DO NOT generate a "Summary" or "Criteria".
- DO NOT rewrite the instructions.
- Your job is ONLY to critique the existing instructions provided above.

⚠️ CRITICAL PAGE NUMBER AND DOC_ID RULE ⚠️
The AUTOMATED CITATION CHECK above uses exact text matching against the source documents.
- If a citation is NOT listed in AUTOMATED CITATION CHECK → it passed automated verification → DO NOT propose FIX_PAGE or FIX_DOC for it
- You may ONLY propose FIX_PAGE/FIX_DOC for citations that ARE listed in AUTOMATED CITATION CHECK
- Your manual page audit is SUPPLEMENTARY — automated check has higher accuracy than your reading
- If you think a page or doc_id is wrong but automated check didn't flag it → TRUST THE AUTOMATED CHECK

FOCUS AREAS:
1. **Clarity:** Is the language simple and direct?
2. **Completeness:** Are all necessary steps included? Did the writer skip the "Review documentation" step?
3. **Usability:** Is the IF/THEN logic easy to follow?
4. **Source Coverage:** Did the draft use ALL provided source documents? Is any source ignored?
5. **Citations validation:**
   - **ONLY** process citations that appear in AUTOMATED CITATION CHECK above
   - If AUTOMATED CITATION CHECK is empty or says "No errors" → DO NOT propose any FIX_PAGE or FIX_DOC
   - For errors listed in AUTOMATED CITATION CHECK:
     * [PAGE_ERROR] → Create FIX_PAGE correction
     * [DOC_ERROR] → Create FIX_DOC correction
     * [PAGE_OVERFLOW] → Create FIX_OVERFLOW correction
     * [AMBIGUOUS] → Create FIX_AMBIGUOUS correction
     * [NOT_FOUND] → Flag as potential hallucination

OUTPUT FORMAT: Return ONLY valid JSON (no markdown wrapper):

{
  "verdict": "COMPLIANT" or "NEED_CLARIFICATION",
  "source_coverage": {
    "<doc_id>": {"cited": true, "count": 5},
    "<doc_id2>": {"cited": false, "count": 0}
  },
  "corrections": [
    {
      "type": "CLARIFY",
      "instruction": "Rephrase X to be clearer",
      "reason": "Junior coders may misunderstand",
      "citation": null
    },
    {
      "type": "CHANGE",
      "instruction": "Change Y to Z",
      "reason": "More accurate wording",
      "citation": {"doc_id": "abc123", "page": 45, "quote": "exact quote"}
    },
    {
      "type": "ADD_SOURCE",
      "instruction": "Add rule from unused source",
      "reason": "Source doc456 was not cited but contains relevant info",
      "citation": {"doc_id": "doc456", "page": 12, "quote": "exact quote"}
    },
    {
      "type": "FIX_PAGE",
      "instruction": "Citation [X] should be Page 45 not Page 42",
      "reason": "From AUTOMATED CITATION CHECK",
      "citation": {"doc_id": "abc123", "page": 45, "quote": "correct quote"}
    }
  ]
}

Correction types: CLARIFY, CHANGE, ADD_SOURCE, FIX_PAGE, FIX_DOC, FIX_OVERFLOW, FIX_AMBIGUOUS
- FIX_* types ONLY for errors from AUTOMATED CITATION CHECK
- Keep corrections focused (max 7 items)

Start answer with {
''')


PROMPT_CODE_RULE_VALIDATION_REDTEAM_JSON = Template('''
You are a Forensic "Red Teamer" (Devil's Advocate). Your goal is to FIND FLAWS, RISKS, and EXCLUSION ERRORS.
You are skeptical, pedantic, and rigorous. You are looking for reasons why this instruction will fail.

''' + PAGE_IDENTIFICATION_RULE_MULTI + '''

=== CROSS-REFERENCE VERIFICATION RULE ===

When you encounter a cross-reference like "See section X.X.X" or "See page N for...":

⚠️ YOU MUST NOT assume what the referenced section says!

**Required steps:**
1. **FIND** the referenced section in the Source Documents (use ## Page markers and doc_id)
2. **READ** the actual content of that section
3. **EXTRACT** only EXPLICIT rules from that section
4. **CITE** from the referenced section itself, not from the cross-reference
5. **INCLUDE doc_id** to identify which source document contains the target section

⛔ REJECTION CRITERIA — Your correction is AUTOMATICALLY INVALID if:
- Your Citation contains "See section...", "See page...", "refer to...", or similar cross-reference text
- You cite the page with the cross-reference instead of the TARGET section
- The target section does not contain an EXPLICIT rule for your proposed correction
- You omit the doc_id from the citation

---

**DETAILED EXAMPLE — Pre-existing Diabetes in Pregnancy:**

DRAFT cites [abc123] Page 67, which contains:
```
"h. Long term use of insulin and oral hypoglycemics
See section I.C.4.a.3 for information on the long-term use of insulin and oral hypoglycemics."
```

❌ **WRONG — This JSON correction will be REJECTED:**
```json
{
  "type": "ADD_STEP",
  "instruction": "IF patient has pre-existing diabetes in pregnancy (O24.-) and uses insulin, THEN ASSIGN Z79.4",
  "risk_level": "HIGH",
  "reason": "Guideline directs user to the section containing rules for Z79.4",
  "citation": {"doc_id": "abc123", "page": 67, "quote": "See section I.C.4.a.3 for information on the long-term use of insulin and oral hypoglycemics."}
}
```
WHY WRONG:
- Citation quote is a cross-reference, not a rule
- Did not verify what I.C.4.a.3 actually says
- "directs user to the section" is inference, not evidence

✅ **CORRECT APPROACH:**
```
1. [abc123] Page 67 has cross-reference: "See section I.C.4.a.3"
2. I found section I.C.4.a.3 on [abc123] Page 40-41
3. I read [abc123] Page 40-41. It says:
   - "For Type 2 diabetes with insulin, assign Z79.4"
   - "For secondary diabetes with insulin, assign Z79.4"
   - ⚠️ NO mention of pre-existing diabetes in pregnancy (O24.-)
4. CONCLUSION: Cannot propose correction — no explicit rule exists for O24.- with Z79.4
   → DO NOT include this in corrections array
```

✅ **If explicit rule WAS found, correct JSON would be:**
```json
{
  "type": "ADD_STEP",
  "instruction": "IF patient has Type 2 diabetes and uses insulin, THEN ASSIGN Z79.4",
  "risk_level": "HIGH",
  "reason": "Explicit rule found in target section",
  "citation": {"doc_id": "abc123", "page": 40, "quote": "For Type 2 diabetes with insulin, assign Z79.4"}
}
```

**If the referenced section does NOT contain an explicit rule for your correction:**
- DO NOT add it to corrections array
- You cannot create rules based on inference or extrapolation
- Only EXPLICIT guideline text can support a correction

---

INPUT:
Source Documents:
$sources
-----
Instructions:
$instructions

$citation_errors

!!! NEGATIVE CONSTRAINTS !!!
- DO NOT generate a "Summary" or "Criteria".
- DO NOT rewrite the instructions.
- Your job is ONLY to critique the existing instructions provided above.

⚠️ CRITICAL PAGE NUMBER AND DOC_ID RULE ⚠️
The AUTOMATED CITATION CHECK above uses exact text matching against the source documents.
- If a citation is NOT listed in AUTOMATED CITATION CHECK → it passed automated verification → DO NOT propose FIX_PAGE or FIX_DOC for it
- You may ONLY propose FIX_PAGE/FIX_DOC for citations that ARE listed in AUTOMATED CITATION CHECK
- Your manual page audit is SUPPLEMENTARY — automated check has higher accuracy than your reading
- If you think a page or doc_id is wrong but automated check didn't flag it → TRUST THE AUTOMATED CHECK

FOCUS AREAS:
1. **Safety:** Does this violate any "Excludes" note in ANY source document?
2. **Edge Cases:** Find a scenario where this instruction gives the WRONG code.
3. **Conflicts:** Does it contradict the Guideline hierarchy? Do sources conflict with each other?
4. **Cross-References (on cited pages only):**
   - Look at the pages cited in the DRAFT instructions (Section 4: REFERENCE)
   - On THOSE pages, check for phrases like "See section X.X.X", "See page N", "refer to..."
   - If found: FOLLOW the reference (may be in same or different source document), READ the target section, check if important rules are MISSING
   - If a cross-reference points to rules not covered → propose FIX RISK
   - **CRITICAL:** Always cite from the ACTUAL target section with [doc_id], not from the cross-reference text
5. **Citations:**
   - **ONLY** process citations that appear in AUTOMATED CITATION CHECK above
   - If AUTOMATED CITATION CHECK is empty or says "No errors" → DO NOT propose any FIX_PAGE or FIX_DOC
   - For errors listed in AUTOMATED CITATION CHECK:
     * [PAGE_ERROR] → Create FIX_PAGE correction with [doc_id] and suggested page number
     * [DOC_ERROR] → Create FIX_DOC correction with correct doc_id
     * [PAGE_OVERFLOW] → Create FIX_OVERFLOW to split citation or cite page range
     * [AMBIGUOUS] → Create FIX_AMBIGUOUS: verify which [doc_id] Page's CONTEXT matches the statement being supported
     * [NOT_FOUND] → Flag as hallucination risk, recommend removal
6. **Source Coverage Verification:**
   - Check: Did the DRAFT cite from ALL provided source documents?
   - If any source has ZERO citations:
     * Verify the UNUSED SOURCES justification in DRAFT's SOURCE EXTRACTION LOG
     * Search that document yourself for ANY relevant content
     * If you find relevant content → propose ADD_SOURCE correction
   - Flag sources where DRAFT cited only 1 page but document has relevant content on multiple pages

OUTPUT FORMAT: Return ONLY valid JSON (no markdown wrapper):

{
  "verdict": "SAFE" or "SAFETY_RISK",
  "risks_found": 2,
  "source_coverage": "COMPLETE" or "GAPS_FOUND",
  "cross_reference_issues": 0,
  "corrections": [
    {
      "type": "BLOCK_RISK",
      "instruction": "Add exclusion: IF condition X THEN REJECT",
      "risk_level": "HIGH",
      "reason": "Without this check, invalid claims could be approved",
      "citation": {"doc_id": "abc123", "page": 67, "quote": "exact quote from source"}
    },
    {
      "type": "ADD_STEP",
      "instruction": "Add verification step for Y",
      "risk_level": "MEDIUM",
      "reason": "Source requires this verification",
      "citation": {"doc_id": "abc123", "page": 89, "quote": "exact quote"}
    },
    {
      "type": "ADD_SOURCE",
      "instruction": "Add citation from underutilized source",
      "risk_level": "MEDIUM",
      "reason": "Source has relevant Excludes note not cited",
      "citation": {"doc_id": "doc456", "page": 23, "quote": "relevant quote"}
    },
    {
      "type": "FIX_PAGE",
      "instruction": "Citation [X] should be Page 45",
      "risk_level": "LOW",
      "reason": "From AUTOMATED CITATION CHECK",
      "citation": {"doc_id": "abc123", "page": 45, "quote": "correct quote"}
    }
  ]
}

Correction types: BLOCK_RISK, ADD_STEP, ADD_SOURCE, FIX_PAGE, FIX_DOC, FIX_OVERFLOW, FIX_AMBIGUOUS
- BLOCK_RISK and ADD_STEP MUST have citation with doc_id, page, quote
- ADD_SOURCE for underutilized sources
- FIX_* types ONLY for errors from AUTOMATED CITATION CHECK
- Do NOT cite cross-references - find actual rule text

**RULE:** Every BLOCK_RISK MUST have a Citation with [doc_id] and page number from ## Page marker.
If you cannot find supporting text in ANY source document, DO NOT propose the fix.

Start answer with {
''')


PROMPT_CODE_RULE_VALIDATION_ARBITRATION_JSON = Template('''
You are the Supreme Medical Arbitrator. You must consolidate reports from a "Senior Mentor" (focus on clarity) and a "Red Teamer" (focus on safety).

''' + PAGE_IDENTIFICATION_RULE_MULTI + '''

INPUT DATA:
1. SOURCE DOCUMENTS (Source of Truth):
$sources
-----
2. DRAFT INSTRUCTIONS:
$instructions
-----
3. MENTOR REPORT (JSON):
$verdict1
-----
4. RED TEAM REPORT (JSON):
$verdict2
-----
5. AUTOMATED CITATION CHECK RESULTS:
$citation_errors

---
### DECISION PROTOCOL

1. **SAFETY FIRST (Red Team Priority):**
   - If Red Team identifies BLOCK_RISK with valid citation → **MUST ACCEPT**
   - Safety trumps clarity

2. **CLARITY SECOND (Mentor Priority):**
   - If Mentor suggests CLARIFY (no conflict with Red Team) → **ACCEPT**
   - If Mentor and Red Team conflict → **LISTEN TO RED TEAM**

3. **PAGE NUMBER AND DOC_ID VERIFICATION:**
   - FIX_PAGE/FIX_DOC should ONLY come from AUTOMATED CITATION CHECK
   - If AUTOMATED CITATION CHECK says "ALL CITATIONS PASSED" → REJECT any FIX_PAGE/FIX_DOC from validators

4. **CITATION REQUIREMENT:**
   - **BLOCK_RISK / ADD_STEP**: MUST have citation from Source Documents with [doc_id] and correct ## Page number
   - **CLARIFY**: Citation optional (formatting changes don't need source)
   - **FIX_PAGE**: Include all page corrections from both reports
   - **FIX_DOC**: Include all doc_id corrections from both reports
   - **FIX_OVERFLOW**: Include cross-page citation fixes
   - **FIX_AMBIGUOUS**: Verify and select correct [doc_id] and page for ambiguous citations

---
### VALIDATION RULES (APPLY BEFORE APPROVING)

**Before approving BLOCK_RISK or ADD_STEP:**
1. Find the quote in SOURCE DOCUMENTS
2. Check the "=== SOURCE: ... [doc_id: <ID>] ===" header above that section
3. Check the "## Page N" marker above that quote
4. Use THAT doc_id and page number as the reference
5. If citation cannot be verified → DO NOT APPROVE (add to rejected_corrections)

**⚠️ FIX_PAGE/FIX_DOC Rule:**
- FIX_PAGE and FIX_DOC corrections ONLY approved if from AUTOMATED CITATION CHECK
- If validator proposes FIX_PAGE/FIX_DOC but AUTOMATED CITATION CHECK said "ALL CITATIONS PASSED" → REJECT
- Validators may hallucinate page errors — trust AUTOMATED CHECK over validator claims

**AMBIGUOUS RESOLUTION Rule (FIX_AMBIGUOUS):**
1. Read the STATEMENT that the citation supports
2. Check each candidate source and page's CONTEXT
3. Select the [doc_id] and page where context matches the statement's meaning
4. If unsure → recommend removing the citation

---
OUTPUT FORMAT: Return ONLY valid JSON (no markdown wrapper):

{
  "safety_status": "PASSED" or "FAILED",
  "usability_status": "HIGH" or "NEEDS_IMPROVEMENT",
  "source_coverage_status": "COMPLETE" or "GAPS_FOUND",
  "approved_corrections": [
    {
      "type": "BLOCK_RISK",
      "instruction": "exact change to make",
      "source": "RedTeam",
      "reason": "why approved",
      "citation": {"doc_id": "abc123", "page": 67, "quote": "exact quote"}
    },
    {
      "type": "CLARIFY",
      "instruction": "rephrase X",
      "source": "Mentor",
      "reason": "improves clarity",
      "citation": null
    }
  ],
  "rejected_corrections": [
    {
      "from": "Mentor",
      "type": "FIX_PAGE",
      "instruction": "proposed change",
      "rejection_reason": "Not in AUTOMATED CITATION CHECK"
    }
  ]
}

VALIDATION RULES:
- Before approving BLOCK_RISK or ADD_STEP: verify citation exists in SOURCE DOCUMENTS
- FIX_PAGE/FIX_DOC: only approve if matches AUTOMATED CITATION CHECK
- If validator proposes FIX but automated check passed → REJECT

Start answer with {
''')
