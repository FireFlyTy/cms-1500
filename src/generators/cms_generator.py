"""
CMS-1500 Claim Rules Generator

Two-step pipeline:
    1. Transform: Guideline + NCCI → CMS Rules (Markdown)
    2. Parse: Markdown → Structured JSON

Storage:
    data/rules/{code}/cms/v{N}/
        cms_rule.md      - Full markdown output
        cms_rule.json    - Structured JSON
        generation_log.json - Pipeline log
"""

import os
import json
import time
import asyncio
from typing import List, Dict, Optional, AsyncGenerator, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

from .core_ai import stream_gemini_generator, stream_openai_model
from .prompts import (
    PROMPT_CMS_RULE_TRANSFORM,
    PROMPT_CMS_RULE_TO_JSON,
)
from src.db.connection import get_db_connection


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).parent.parent.parent
RULES_DIR = BASE_DIR / "data" / "processed" / "rules"
CMS1500_SCHEMA_PATH = BASE_DIR / "data" / "raw" / "cms_1500_mongo" / "schema-for-prompt.md"

RULES_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class NCCIEdits:
    """NCCI edits for a CPT/HCPCS code."""
    ptp_edits: List[Dict] = field(default_factory=list)  # Bundling rules
    mue_value: Optional[int] = None
    mue_per: Optional[str] = None  # "line" or "date_of_service"
    mue_adjudication: Optional[int] = None  # 1, 2, or 3
    mue_rationale: Optional[str] = None


@dataclass
class CMSStepResult:
    """Result of one pipeline step."""
    step: str
    output: str
    thinking: str = ""
    duration_ms: int = 0


@dataclass
class CMSPipelineResult:
    """Full pipeline result."""
    code: str
    code_type: str
    version: int
    created_at: str
    guideline_source: Optional[Dict] = None
    ncci_edits: Optional[Dict] = None
    steps: Dict[str, CMSStepResult] = field(default_factory=dict)
    total_duration_ms: int = 0
    final_markdown: str = ""
    final_json: Optional[Dict] = None
    status: str = "completed"


@dataclass
class SSEEvent:
    """SSE event for streaming."""
    step: str
    type: str  # status | thought | content | done | error
    content: str = ""
    thinking: str = ""
    full_text: str = ""
    duration_ms: int = 0
    preview: str = ""
    think_preview: str = ""

    def to_json(self) -> str:
        data = {k: v for k, v in asdict(self).items() if v or k in ('step', 'type')}
        return json.dumps(data, ensure_ascii=False)


# ============================================================
# CMS RULE GENERATOR
# ============================================================

class CMSRuleGenerator:
    """
    Generator for CMS-1500 claim validation rules.

    Usage:
        generator = CMSRuleGenerator()
        async for event in generator.stream_pipeline("E11.9"):
            yield f"data: {event}\\n\\n"
    """

    # Default model - can be overridden via env var or constructor
    DEFAULT_MODEL = os.getenv("RULE_GENERATOR_MODEL", "gemini")

    def __init__(self, thinking_budget: int = 8000, model: str = None):
        self.thinking_budget = thinking_budget
        self.model = model or self.DEFAULT_MODEL
        self._results: Dict[str, CMSStepResult] = {}

    async def _stream_model(self, prompt: str, budget: int = None) -> AsyncGenerator[str, None]:
        """Stream from configured model (Gemini or OpenAI)."""
        if self.model.startswith("gpt"):
            async for chunk in stream_openai_model(prompt, model=self.model):
                yield chunk
        else:
            async for chunk in stream_gemini_generator(prompt, budget or self.thinking_budget):
                yield chunk

    async def stream_pipeline(
        self,
        code: str,
        code_type: Optional[str] = None,
        parent_cms1500_rule: Optional[str] = None,
    ) -> AsyncGenerator[str, None]:
        """
        Stream full CMS rules generation pipeline.

        Args:
            code: ICD-10, CPT, or HCPCS code
            code_type: Optional - will auto-detect if not provided
            parent_cms1500_rule: Optional - parent's CMS-1500 rule for inheritance

        Yields:
            JSON SSE events
        """
        pipeline_start = time.time()

        try:
            # Auto-detect code type if not provided
            if not code_type:
                code_type = self._detect_code_type(code)

            yield self._event("pipeline", "status",
                f"Starting CMS rule generation for {code} ({code_type})"
            ).to_json()

            # 1. Load guideline rule (if exists)
            t1 = time.time()
            guideline_rule, guideline_meta = self._load_guideline_rule(code)
            guideline_load_ms = (time.time() - t1) * 1000

            if guideline_rule:
                yield self._event("pipeline", "status",
                    f"Loaded guideline v{guideline_meta.get('version', '?')} ({guideline_load_ms:.0f}ms)"
                ).to_json()
            else:
                yield self._event("pipeline", "status",
                    f"No guideline rule found ({guideline_load_ms:.0f}ms)"
                ).to_json()

            # 2. Fetch NCCI edits (for CPT/HCPCS only)
            ncci_edits = None
            ncci_text = "**Not applicable** - This is an ICD-10 diagnosis code. NCCI edits apply only to CPT/HCPCS procedure codes."

            if code_type in ("CPT", "HCPCS"):
                t2 = time.time()
                ncci_edits = self._fetch_ncci_edits(code)
                ncci_text = self._format_ncci_for_prompt(ncci_edits, code)
                ncci_load_ms = (time.time() - t2) * 1000

                ptp_count = len(ncci_edits.ptp_edits) if ncci_edits else 0
                mue_info = f"MUE={ncci_edits.mue_value}" if ncci_edits and ncci_edits.mue_value else "No MUE"
                yield self._event("pipeline", "status",
                    f"Found {ptp_count} PTP edits, {mue_info} ({ncci_load_ms:.0f}ms)"
                ).to_json()

            # 3. Load CMS-1500 schema
            t3 = time.time()
            cms_schema = self._load_cms_schema()
            schema_load_ms = (time.time() - t3) * 1000

            if not cms_schema:
                yield self._event("pipeline", "error", "Failed to load CMS-1500 schema").to_json()
                return

            yield self._event("pipeline", "status",
                f"Loaded CMS-1500 schema ({schema_load_ms:.0f}ms)"
            ).to_json()

            # 4. Get code description
            t4 = time.time()
            description = self._get_code_description(code, code_type)
            desc_load_ms = (time.time() - t4) * 1000

            yield self._event("pipeline", "timing",
                f"Setup complete: guideline={guideline_load_ms:.0f}ms, schema={schema_load_ms:.0f}ms, desc={desc_load_ms:.0f}ms"
            ).to_json()

            # Check if we have anything to process
            if not guideline_rule and (not ncci_edits or (not ncci_edits.ptp_edits and ncci_edits.mue_value is None)):
                yield self._event("pipeline", "error",
                    f"No sources found for {code}: no guideline rule and no NCCI edits"
                ).to_json()
                return

            # 5. STEP 1: Transform to CMS rules (Markdown)
            async for event in self._stream_transform(
                code, code_type, description,
                guideline_rule or "No guideline rule available.",
                ncci_text,
                cms_schema,
                parent_cms1500_rule or "**No parent CMS-1500 rule** - This is a top-level code."
            ):
                yield event

            if "transform" not in self._results:
                yield self._event("pipeline", "error", "Transform step failed").to_json()
                return

            # 6. STEP 2: Parse to JSON
            async for event in self._stream_parse():
                yield event

            if "parse" not in self._results:
                yield self._event("pipeline", "error", "Parse step failed").to_json()
                return

            # 7. Save results
            total_ms = int((time.time() - pipeline_start) * 1000)

            pipeline_result = self._build_pipeline_result(
                code, code_type, total_ms,
                guideline_meta, ncci_edits
            )
            save_path = self._save_results(code, pipeline_result)

            yield self._event("pipeline", "done",
                content=f"Pipeline completed. Saved to {save_path}",
                duration_ms=total_ms
            ).to_json()

        except Exception as e:
            import traceback
            traceback.print_exc()
            yield self._event("pipeline", "error", str(e)).to_json()

    # --------------------------------------------------------
    # STEP 1: TRANSFORM (Guideline + NCCI → CMS Rules MD)
    # --------------------------------------------------------

    async def _stream_transform(
        self,
        code: str,
        code_type: str,
        description: str,
        guideline_rule: str,
        ncci_text: str,
        cms_schema: str,
        parent_cms1500_rule: str = ""
    ) -> AsyncGenerator[str, None]:
        """Transform guideline + NCCI into CMS rules markdown."""
        step = "transform"
        yield self._event(step, "status", "Generating CMS rules...").to_json()

        prompt = PROMPT_CMS_RULE_TRANSFORM.substitute(
            code=code,
            code_type=code_type,
            description=description,
            parent_cms1500_rule=parent_cms1500_rule,
            guideline_rule=guideline_rule,
            ncci_edits=ncci_text,
            cms1500_schema=cms_schema
        )

        full_text, thinking, duration_ms = "", "", 0
        async for event_json, final in self._stream_step_with_events(step, prompt):
            if final is not None:
                full_text, thinking, duration_ms = final
            elif event_json:
                yield event_json

        self._results[step] = CMSStepResult(
            step=step,
            output=full_text,
            thinking=thinking,
            duration_ms=duration_ms
        )

        yield self._event(step, "done",
            full_text=full_text,
            thinking=thinking,
            duration_ms=duration_ms
        ).to_json()

    # --------------------------------------------------------
    # STEP 2: PARSE (Markdown → JSON)
    # --------------------------------------------------------

    async def _stream_parse(self) -> AsyncGenerator[str, None]:
        """Parse CMS rules markdown into structured JSON."""
        step = "parse"
        yield self._event(step, "status", "Parsing to structured JSON...").to_json()

        transform_result = self._results["transform"]

        prompt = PROMPT_CMS_RULE_TO_JSON.substitute(
            cms_rule_markdown=transform_result.output
        )

        # No reasoning for JSON parsing - straightforward extraction
        full_text, thinking, duration_ms = "", "", 0
        async for event_json, final in self._stream_step_with_events(step, prompt, thinking_budget=None):
            if final is not None:
                full_text, thinking, duration_ms = final
            elif event_json:
                yield event_json

        # Validate JSON output
        try:
            # Clean up: LLM might add markdown code blocks
            json_text = full_text.strip()
            if json_text.startswith("```"):
                json_text = json_text.split("\n", 1)[1]
            if json_text.endswith("```"):
                json_text = json_text.rsplit("```", 1)[0]
            json_text = json_text.strip()

            parsed_json = json.loads(json_text)
            yield self._event(step, "status", "JSON parsed successfully").to_json()
        except json.JSONDecodeError as e:
            yield self._event(step, "status", f"JSON parse warning: {e}").to_json()
            parsed_json = None

        self._results[step] = CMSStepResult(
            step=step,
            output=json_text if parsed_json else full_text,
            thinking=thinking,
            duration_ms=duration_ms
        )

        yield self._event(step, "done",
            full_text=full_text,
            thinking=thinking,
            duration_ms=duration_ms
        ).to_json()

    # --------------------------------------------------------
    # HELPERS: Streaming
    # --------------------------------------------------------

    # Sentinel value to indicate "use class default thinking budget"
    _USE_DEFAULT = object()

    async def _stream_step_with_events(
        self,
        step: str,
        prompt: str,
        thinking_budget = _USE_DEFAULT
    ) -> AsyncGenerator[Tuple[str, Optional[Tuple[str, str, int]]], None]:
        """Stream one step, yield SSE events, at end yield result."""
        start_time = time.time()
        full_text = ""
        thinking = ""

        # Use class default if not specified, None means no thinking
        budget = self.thinking_budget if thinking_budget is self._USE_DEFAULT else thinking_budget
        async for chunk in self._stream_model(prompt, budget):
            data = json.loads(chunk.strip())

            if data["type"] == "thought":
                thinking += data.get("content", "")
                if len(thinking) % 500 < 50:
                    event = self._event(step, "thought",
                        content=data.get("content", ""),
                        think_preview=thinking[-200:] if len(thinking) > 200 else thinking
                    ).to_json()
                    yield (event, None)

            elif data["type"] == "content":
                full_text += data.get("content", "")
                if len(full_text) % 300 < 30:
                    event = self._event(step, "content",
                        content=data.get("content", ""),
                        preview=full_text[-300:] if len(full_text) > 300 else full_text
                    ).to_json()
                    yield (event, None)

            elif data["type"] == "done":
                full_text = data.get("full_text", full_text)
                thinking = data.get("think_text", thinking)

            elif data["type"] == "error":
                raise Exception(data.get("content", "Unknown error"))

        duration_ms = int((time.time() - start_time) * 1000)
        yield ("", (full_text, thinking, duration_ms))

    def _event(self, step: str, type: str, content: str = "", **kwargs) -> SSEEvent:
        """Create SSE event."""
        return SSEEvent(step=step, type=type, content=content, **kwargs)

    # --------------------------------------------------------
    # HELPERS: Data Loading
    # --------------------------------------------------------

    def _detect_code_type(self, code: str) -> str:
        """Auto-detect code type from format."""
        code = code.upper().strip()

        # HCPCS: starts with letter (except valid CPT ranges)
        if code[0].isalpha():
            return "HCPCS"

        # CPT: 5-digit numeric (00100-99999)
        if code.isdigit() and len(code) == 5:
            return "CPT"

        # ICD-10: letter + digits with optional decimal
        if code[0].isalpha() and any(c.isdigit() for c in code):
            return "ICD-10"

        return "ICD-10"  # Default

    def _load_guideline_rule(self, code: str) -> Tuple[Optional[str], Optional[Dict]]:
        """Load the latest guideline rule for a code."""
        code_dir = code.replace(".", "_").replace("/", "_").replace(":", "_").replace("-", "_")
        code_path = RULES_DIR / code_dir

        if not code_path.exists():
            return None, None

        # Find latest version
        versions = []
        for name in code_path.iterdir():
            if name.is_dir() and name.name.startswith('v') and name.name[1:].isdigit():
                versions.append(int(name.name[1:]))

        if not versions:
            return None, None

        latest_version = max(versions)
        rule_md_path = code_path / f"v{latest_version}" / "rule.md"
        rule_json_path = code_path / f"v{latest_version}" / "rule.json"

        if not rule_md_path.exists():
            return None, None

        with open(rule_md_path, 'r', encoding='utf-8') as f:
            rule_content = f.read()

        # Get metadata from rule.json if available
        meta = {"version": latest_version}
        if rule_json_path.exists():
            try:
                with open(rule_json_path, 'r', encoding='utf-8') as f:
                    rule_data = json.load(f)
                    meta["doc_ids"] = rule_data.get("source_doc_ids", [])
                    meta["created_at"] = rule_data.get("created_at")
            except:
                pass

        return rule_content, meta

    def _fetch_ncci_edits(self, code: str) -> NCCIEdits:
        """Fetch NCCI PTP and MUE edits from database."""
        edits = NCCIEdits()

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Fetch PTP edits where this code is column1 (comprehensive)
            cursor.execute("""
                SELECT column2, modifier_indicator, rationale
                FROM ncci_ptp
                WHERE column1 = ? AND deletion_date = '*'
                LIMIT 100
            """, (code,))

            for row in cursor.fetchall():
                edits.ptp_edits.append({
                    "bundled_code": row["column2"],
                    "modifier_indicator": row["modifier_indicator"],
                    "rationale": row["rationale"],
                    "direction": "column1"
                })

            # Also fetch where this code is column2 (bundled into others)
            cursor.execute("""
                SELECT column1, modifier_indicator, rationale
                FROM ncci_ptp
                WHERE column2 = ? AND deletion_date = '*'
                LIMIT 100
            """, (code,))

            for row in cursor.fetchall():
                edits.ptp_edits.append({
                    "bundled_code": row["column1"],
                    "modifier_indicator": row["modifier_indicator"],
                    "rationale": row["rationale"],
                    "direction": "column2"
                })

            # Fetch MUE - check both tables, prefer non-zero value
            # DME table is more relevant for HCPCS E-codes
            cursor.execute("""
                SELECT mue_value, adjudication_indicator, rationale
                FROM ncci_mue_dme
                WHERE code = ?
            """, (code,))
            dme_row = cursor.fetchone()

            cursor.execute("""
                SELECT mue_value, adjudication_indicator, rationale
                FROM ncci_mue_pra
                WHERE code = ?
            """, (code,))
            pra_row = cursor.fetchone()

            # Prefer DME for E-codes, otherwise prefer row with positive MUE value
            mue_row = None
            if dme_row and dme_row["mue_value"] and dme_row["mue_value"] > 0:
                mue_row = dme_row
            elif pra_row and pra_row["mue_value"] and pra_row["mue_value"] > 0:
                mue_row = pra_row
            elif dme_row:
                mue_row = dme_row
            elif pra_row:
                mue_row = pra_row

            if mue_row and mue_row["mue_value"] is not None:
                edits.mue_value = mue_row["mue_value"]
                edits.mue_rationale = mue_row["rationale"]

                adj = mue_row["adjudication_indicator"]
                if "Line" in str(adj):
                    edits.mue_per = "line"
                    edits.mue_adjudication = 1
                elif "Policy" in str(adj):
                    edits.mue_per = "date_of_service"
                    edits.mue_adjudication = 2
                else:
                    edits.mue_per = "date_of_service"
                    edits.mue_adjudication = 3

            conn.close()

        except Exception as e:
            print(f"Error fetching NCCI edits: {e}")

        return edits

    def _format_ncci_for_prompt(self, edits: NCCIEdits, code: str) -> str:
        """Format NCCI edits as text for the prompt."""
        lines = []

        # PTP Edits
        if edits.ptp_edits:
            lines.append(f"### PTP Bundling Edits for {code}")
            lines.append("")

            # Group by direction
            as_column1 = [e for e in edits.ptp_edits if e["direction"] == "column1"]
            as_column2 = [e for e in edits.ptp_edits if e["direction"] == "column2"]

            if as_column1:
                lines.append(f"**{code} bundles these codes** (deny column2 when billed with {code}):")
                lines.append("")
                lines.append("| Bundled Code | Modifier Override | Rationale |")
                lines.append("|--------------|-------------------|-----------|")
                for edit in as_column1[:50]:  # Limit to top 50
                    mod = "Yes" if edit["modifier_indicator"] == 1 else "No"
                    rationale = (edit["rationale"] or "N/A")[:60]
                    lines.append(f"| {edit['bundled_code']} | {mod} | {rationale} |")
                if len(as_column1) > 50:
                    lines.append(f"| ... | ... | ({len(as_column1) - 50} more edits) |")
                lines.append("")

            if as_column2:
                lines.append(f"**{code} is bundled INTO these codes** (deny {code} when billed with column1):")
                lines.append("")
                lines.append("| Comprehensive Code | Modifier Override | Rationale |")
                lines.append("|-------------------|-------------------|-----------|")
                for edit in as_column2[:50]:
                    mod = "Yes" if edit["modifier_indicator"] == 1 else "No"
                    rationale = (edit["rationale"] or "N/A")[:60]
                    lines.append(f"| {edit['bundled_code']} | {mod} | {rationale} |")
                if len(as_column2) > 50:
                    lines.append(f"| ... | ... | ({len(as_column2) - 50} more edits) |")
                lines.append("")
        else:
            lines.append("### PTP Bundling Edits")
            lines.append(f"No active PTP edits found for {code}.")
            lines.append("")

        # MUE Edit
        lines.append("### MUE (Medically Unlikely Edit)")
        if edits.mue_value is not None:
            adj_text = {
                1: "Line Edit (per claim line)",
                2: "Date of Service Edit: Policy",
                3: "Date of Service Edit: Clinical"
            }.get(edits.mue_adjudication, "Unknown")

            lines.append(f"- **Max Units**: {edits.mue_value}")
            lines.append(f"- **Per**: {edits.mue_per}")
            lines.append(f"- **Adjudication**: {adj_text}")
            lines.append(f"- **Rationale**: {edits.mue_rationale or 'N/A'}")
        else:
            lines.append(f"No MUE found for {code}.")

        return "\n".join(lines)

    def _load_cms_schema(self) -> Optional[str]:
        """Load CMS-1500 schema from file."""
        if CMS1500_SCHEMA_PATH.exists():
            with open(CMS1500_SCHEMA_PATH, 'r', encoding='utf-8') as f:
                return f.read()
        return None

    def _get_code_description(self, code: str, code_type: str) -> str:
        """Get code description from database."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            if code_type == "ICD-10":
                cursor.execute("SELECT description FROM icd10 WHERE code = ?", (code,))
            elif code_type == "CPT":
                cursor.execute("SELECT description FROM cpt WHERE code = ?", (code,))
            elif code_type == "HCPCS":
                cursor.execute("SELECT long_description FROM hcpcs WHERE code = ?", (code,))
            else:
                return f"{code_type} code"

            row = cursor.fetchone()
            conn.close()

            if row:
                return row[0]

        except Exception as e:
            print(f"Error fetching code description: {e}")

        return f"{code_type} code {code}"

    # --------------------------------------------------------
    # HELPERS: Results
    # --------------------------------------------------------

    def _get_next_cms_version(self, code: str) -> int:
        """Find next version number for CMS rules."""
        code_dir = code.replace(".", "_").replace("/", "_").replace(":", "_").replace("-", "_")
        cms_path = RULES_DIR / code_dir / "cms"

        if not cms_path.exists():
            return 1

        versions = []
        for name in cms_path.iterdir():
            if name.is_dir() and name.name.startswith('v') and name.name[1:].isdigit():
                versions.append(int(name.name[1:]))

        if not versions:
            return 1

        return max(versions) + 1

    def _build_pipeline_result(
        self,
        code: str,
        code_type: str,
        total_ms: int,
        guideline_meta: Optional[Dict],
        ncci_edits: Optional[NCCIEdits]
    ) -> CMSPipelineResult:
        """Build full pipeline result."""
        version = self._get_next_cms_version(code)

        # Parse JSON result
        final_json = None
        parse_result = self._results.get("parse")
        if parse_result:
            try:
                final_json = json.loads(parse_result.output)
            except:
                pass

        return CMSPipelineResult(
            code=code,
            code_type=code_type,
            version=version,
            created_at=datetime.utcnow().isoformat() + "Z",
            guideline_source=guideline_meta,
            ncci_edits=asdict(ncci_edits) if ncci_edits else None,
            steps=self._results,
            total_duration_ms=total_ms,
            final_markdown=self._results.get("transform", CMSStepResult("", "")).output,
            final_json=final_json,
            status="completed"
        )

    def _save_results(self, code: str, result: CMSPipelineResult) -> str:
        """
        Save results to files.

        Structure:
            data/rules/{code}/cms/v{N}/
                cms_rule.md        - markdown output
                cms_rule.json      - structured JSON
                generation_log.json - full log
        """
        code_dir = code.replace(".", "_").replace("/", "_").replace(":", "_").replace("-", "_")
        version_dir = RULES_DIR / code_dir / "cms" / f"v{result.version}"
        version_dir.mkdir(parents=True, exist_ok=True)

        # Save generation log
        log_path = version_dir / "generation_log.json"
        log_data = {
            "code": result.code,
            "code_type": result.code_type,
            "version": result.version,
            "created_at": result.created_at,
            "guideline_source": result.guideline_source,
            "ncci_edits": result.ncci_edits,
            "pipeline": {
                step: {
                    "output": r.output,
                    "thinking": r.thinking,
                    "duration_ms": r.duration_ms
                }
                for step, r in result.steps.items()
            },
            "total_duration_ms": result.total_duration_ms,
            "status": result.status
        }

        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False)

        # Save markdown
        md_path = version_dir / "cms_rule.md"
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(result.final_markdown)

        # Save JSON
        json_path = version_dir / "cms_rule.json"
        if result.final_json:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(result.final_json, f, indent=2, ensure_ascii=False)
        else:
            # Save raw parse output if JSON parsing failed
            with open(json_path, 'w', encoding='utf-8') as f:
                parse_result = result.steps.get("parse")
                f.write(parse_result.output if parse_result else "{}")

        # Save to database
        self._save_to_database(code, result, str(version_dir))

        return str(version_dir)

    def _save_to_database(self, code: str, result: CMSPipelineResult, rule_path: str):
        """Save or update rule in database."""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Get source document IDs
            source_docs = None
            if result.guideline_source and result.guideline_source.get("doc_ids"):
                source_docs = json.dumps(result.guideline_source["doc_ids"])

            # Get guideline rule path
            guideline_rule_path = None
            if result.guideline_source and result.guideline_source.get("version"):
                code_dir = code.replace(".", "_").replace("/", "_").replace(":", "_").replace("-", "_")
                version = result.guideline_source["version"]
                guideline_rule_path = str(RULES_DIR / code_dir / f"v{version}")

            # Check if rule already exists
            cursor.execute("""
                SELECT id FROM rules WHERE code = ? AND rule_level = ?
            """, (code, "cms"))

            existing = cursor.fetchone()

            if existing:
                # Update existing rule
                cursor.execute("""
                    UPDATE rules SET
                        code_type = ?,
                        status = ?,
                        rule_path = ?,
                        source_documents = ?,
                        generated_at = ?,
                        guideline_rule_path = ?
                    WHERE code = ? AND rule_level = ?
                """, (
                    result.code_type,
                    result.status,
                    rule_path,
                    source_docs,
                    result.created_at,
                    guideline_rule_path,
                    code,
                    "cms"
                ))
            else:
                # Insert new rule
                cursor.execute("""
                    INSERT INTO rules (code, code_type, rule_level, status, rule_path, source_documents, generated_at, guideline_rule_path)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    code,
                    result.code_type,
                    "cms",
                    result.status,
                    rule_path,
                    source_docs,
                    result.created_at,
                    guideline_rule_path
                ))

            conn.commit()
            conn.close()
            print(f"Saved rule to database: {code} (cms)")

        except Exception as e:
            print(f"Error saving rule to database: {e}")


# ============================================================
# CONVENIENCE FUNCTION
# ============================================================

async def generate_cms_rule_stream(
    code: str,
    code_type: Optional[str] = None,
    thinking_budget: int = 8000
) -> AsyncGenerator[str, None]:
    """
    Convenience function for streaming CMS rule generation.

    Usage:
        async for event in generate_cms_rule_stream("99213"):
            yield f"data: {event}\\n\\n"
    """
    generator = CMSRuleGenerator(thinking_budget=thinking_budget)
    async for event in generator.stream_pipeline(code, code_type):
        yield event
