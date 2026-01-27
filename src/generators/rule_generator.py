"""
Rule Generator Pipeline Orchestrator

Полный пайплайн генерации правил:
    Draft → [Mentor || RedTeam] → Arbitration → Finalization

SSE Event Format:
{
    "step": "draft" | "mentor" | "redteam" | "arbitration" | "finalization",
    "type": "status" | "thought" | "content" | "verification" | "done" | "error",
    "content": "...",
    "thinking": "...",          # для done
    "full_text": "...",         # для done
    "duration_ms": 15000,       # для done
    "citations_check": {...},   # для verification
}
"""

import os
import json
import time
import asyncio
from typing import List, Dict, Optional, AsyncGenerator, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime
from string import Template

from .context_builder import build_sources_context, SourcesContext, SourceDocument
from .core_ai import stream_gemini_generator, stream_openai_model, stream_pipeline_step
from .prompts import (
    PROMPT_CODE_RULE_DRAFT,
    PROMPT_META_CATEGORY_DRAFT,
    DESCENDANT_INSTRUCTIONS,
    LEAF_CODE_INSTRUCTIONS,
    PROMPT_CODE_RULE_VALIDATION_MENTOR,
    PROMPT_CODE_RULE_VALIDATION_REDTEAM,
    PROMPT_CODE_RULE_VALIDATION_ARBITRATION,
    PROMPT_CODE_RULE_FINALIZATION,
    # JSON variants (faster)
    PROMPT_CODE_RULE_VALIDATION_MENTOR_JSON,
    PROMPT_CODE_RULE_VALIDATION_REDTEAM_JSON,
    PROMPT_CODE_RULE_VALIDATION_ARBITRATION_JSON,
)

# Import validators
from src.validators.citations import (
    parse_sources_to_pages,
    verify_citations,
    verify_draft_citations,
    format_citation_errors_for_prompt,
    apply_repairs_to_output,
)


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RULES_DIR = os.path.join(BASE_DIR, "data", "processed", "rules")

os.makedirs(RULES_DIR, exist_ok=True)


# ============================================================
# HELPERS
# ============================================================

def strip_json_fences(text: str) -> str:
    """Strip markdown code fences from JSON output.

    Models often return JSON wrapped in ```json ... ``` markers.
    This function removes them to get clean JSON.
    """
    text = text.strip()
    # Remove ```json ... ``` or ``` ... ```
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def parse_json_safely(text: str) -> dict:
    """Parse JSON from text, handling markdown fences."""
    cleaned = strip_json_fences(text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return {}


def has_descendants(code: str, code_type: str = "ICD-10") -> bool:
    """
    Check if a code has descendants in the hierarchy.

    Uses code_hierarchy table to determine if child codes exist.

    Args:
        code: The code to check (e.g., "E11", "E11.6", "E11.65")
        code_type: Type of code (ICD-10, CPT, HCPCS)

    Returns:
        True if code has children, False if it's a leaf code
    """
    import sqlite3

    db_path = os.path.join(BASE_DIR, "data", "db", "reference.db")
    if not os.path.exists(db_path):
        # Fallback: assume codes with fewer characters have descendants
        # E11 → has descendants, E11.65 → likely leaf
        if code_type == "ICD-10":
            return len(code) < 6  # E11.65 = 6 chars
        return len(code) < 5  # CPT/HCPCS: 5 digit codes are usually specific

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if any code starts with this pattern
        cursor.execute("""
            SELECT COUNT(*) FROM code_hierarchy
            WHERE code_type = ?
            AND pattern LIKE ?
            AND pattern != ?
        """, (code_type, f"{code}%", code))

        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        # Fallback heuristic
        if code_type == "ICD-10":
            return len(code) < 6
        return len(code) < 5


def get_code_level(code: str, code_type: str = "ICD-10") -> str:
    """
    Determine the hierarchical level of a code.

    Args:
        code: The code (e.g., "E", "E11", "E11.6", "E11.65")
        code_type: Type of code

    Returns:
        "meta_category" | "category" | "subcategory" | "code"
    """
    if code_type == "ICD-10":
        # ICD-10: E=meta, E11=category, E11.6=subcategory, E11.65=code
        if len(code) == 1:
            return "meta_category"
        elif "." not in code:
            return "category"
        else:
            after_dot = code.split(".")[1]
            if len(after_dot) == 1:
                return "subcategory"
            return "code"
    elif code_type == "CPT":
        # CPT: ranges like 99201-99215 → category; 99213 → code
        if "-" in code:
            return "category"
        return "code"
    else:
        # HCPCS: J=meta_category, J1xxx=category, J1234=code
        if len(code) == 1:
            return "meta_category"
        elif len(code) < 5:
            return "category"
        return "code"


def inject_level_instructions(base_template: Template, code: str, code_type: str) -> Template:
    """
    Inject level-appropriate instructions into the base prompt template.

    Args:
        base_template: The base PROMPT_CODE_RULE_DRAFT template
        code: The code being processed
        code_type: Type of code

    Returns:
        Modified template with level-specific instructions injected
    """
    has_children = has_descendants(code, code_type)

    # Get the template source
    source = base_template.template

    # Find injection point (after === SOURCE DOCUMENTS ===)
    injection_point = "=== SOURCE DOCUMENTS ==="

    if has_children:
        instructions = DESCENDANT_INSTRUCTIONS.replace("$code", code)
    else:
        instructions = LEAF_CODE_INSTRUCTIONS.replace("$code", code)

    # Inject after the source documents header intro
    if injection_point in source:
        parts = source.split(injection_point)
        modified_source = parts[0] + injection_point + "\n\n" + instructions + parts[1]
        return Template(modified_source)

    # Fallback: prepend instructions
    return Template(instructions + "\n\n" + source)


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class StepResult:
    """Результат одного шага пайплайна."""
    step: str
    output: str
    thinking: str = ""
    duration_ms: int = 0
    citations_check: Optional[Dict] = None
    corrections_count: int = 0
    risks_count: int = 0
    verdict: str = ""


@dataclass
class PipelineResult:
    """Полный результат пайплайна."""
    code: str
    version: int
    created_at: str
    source_documents: List[Dict]
    steps: Dict[str, StepResult] = field(default_factory=dict)
    total_duration_ms: int = 0
    final_output: str = ""
    final_citations_check: Optional[Dict] = None
    status: str = "completed"  # completed | failed | partial


@dataclass 
class SSEEvent:
    """SSE событие для стриминга."""
    step: str
    type: str  # status | thought | content | verification | done | error
    content: str = ""
    thinking: str = ""
    full_text: str = ""
    duration_ms: int = 0
    citations_check: Optional[Dict] = None
    preview: str = ""
    think_preview: str = ""
    
    def to_json(self) -> str:
        data = {k: v for k, v in asdict(self).items() if v or k in ('step', 'type')}
        return json.dumps(data, ensure_ascii=False)


# ============================================================
# RULE GENERATOR
# ============================================================

class RuleGenerator:
    """
    Оркестратор пайплайна генерации правил.

    Usage:
        generator = RuleGenerator()
        async for event in generator.stream_pipeline("E11.9"):
            yield f"data: {event}\\n\\n"
    """

    # Default model - can be overridden via env var or constructor
    DEFAULT_MODEL = os.getenv("RULE_GENERATOR_MODEL", "gemini")  # "gemini" or "gpt-4.1"

    # JSON validators flag - faster but less detailed output
    DEFAULT_JSON_VALIDATORS = os.getenv("RULE_GENERATOR_JSON_VALIDATORS", "false").lower() == "true"

    def __init__(self, thinking_budget: int = 10000, model: str = None, json_validators: bool = None):
        self.thinking_budget = thinking_budget
        self.model = model or self.DEFAULT_MODEL
        self.json_validators = json_validators if json_validators is not None else self.DEFAULT_JSON_VALIDATORS
        self._sources_ctx: Optional[SourcesContext] = None
        self._doc_pages: Optional[Dict] = None
        self._results: Dict[str, StepResult] = {}
        self._inheritance_context: Optional[str] = None

    async def _stream_model(self, prompt: str, step: str = None) -> AsyncGenerator[str, None]:
        """Stream from configured model based on pipeline step.

        Args:
            prompt: The prompt to send
            step: Pipeline step (draft, mentor, redteam, arbitration, finalization)
                  If provided, uses PIPELINE_MODELS config for model selection.
        """
        if step:
            # Use pipeline config for model selection (step config controls thinking_budget)
            async for chunk in stream_pipeline_step(prompt, step=step):
                yield chunk
        elif self.model.startswith("gpt"):
            async for chunk in stream_openai_model(prompt, model=self.model):
                yield chunk
        else:
            # Default to Gemini
            async for chunk in stream_gemini_generator(prompt, self.thinking_budget):
                yield chunk

    async def stream_pipeline(
        self,
        code: str,
        document_ids: Optional[List[str]] = None,
        code_type: str = "ICD-10",
        parallel_validators: bool = False,
        inheritance_context: Optional[str] = None,
        level: Optional[str] = None,
    ) -> AsyncGenerator[str, None]:
        """
        Стримит полный пайплайн генерации правила.

        Args:
            code: ICD-10 код
            document_ids: Опционально - конкретные документы
            code_type: Тип кода (ICD-10, CPT, etc.)
            parallel_validators: True для параллельного запуска Mentor+RedTeam
            inheritance_context: Контекст наследования (родительские правила)
            level: Hierarchical level (meta_category, category, subcategory, code)
                   If not provided, auto-detected from code structure.

        Yields:
            JSON SSE events
        """
        pipeline_start = time.time()
        self._inheritance_context = inheritance_context
        # Auto-detect level if not provided
        self._level = level or get_code_level(code, code_type)

        try:
            # 1. Build sources context
            yield self._event("pipeline", "status", "Building sources context...").to_json()

            self._sources_ctx = build_sources_context(code, document_ids, code_type=code_type)

            # Allow empty sources if we have inheritance context
            if not self._sources_ctx.sources_text and not inheritance_context:
                yield self._event("pipeline", "error", f"No documents found for code {code}").to_json()
                return

            # Parse sources for citation verification
            self._doc_pages = parse_sources_to_pages(self._sources_ctx.sources_text) if self._sources_ctx.sources_text else {}

            yield self._event("pipeline", "status",
                f"Found {len(self._sources_ctx.source_documents)} documents, "
                f"{self._sources_ctx.total_pages} pages"
                + (f" + inheritance context" if inheritance_context else "")
            ).to_json()

            # 2. DRAFT
            async for event in self._stream_draft(code, code_type):
                yield event
            
            if "draft" not in self._results:
                yield self._event("pipeline", "error", "Draft generation failed").to_json()
                return
            
            # 3. VALIDATORS (Mentor + RedTeam)
            if parallel_validators:
                async for event in self._stream_validators_parallel():
                    yield event
            else:
                async for event in self._stream_validators_sequential():
                    yield event
            
            # 4. ARBITRATION
            async for event in self._stream_arbitration():
                yield event
            
            # 5. FINALIZATION
            async for event in self._stream_finalization():
                yield event
            
            # 6. Save results
            total_ms = int((time.time() - pipeline_start) * 1000)
            
            pipeline_result = self._build_pipeline_result(code, total_ms)
            save_path = self._save_results(code, pipeline_result)
            
            yield self._event("pipeline", "done", 
                content=f"Pipeline completed. Saved to {save_path}",
                duration_ms=total_ms
            ).to_json()
            
        except Exception as e:
            yield self._event("pipeline", "error", str(e)).to_json()
    
    # --------------------------------------------------------
    # STEP: DRAFT
    # --------------------------------------------------------
    
    async def _stream_draft(self, code: str, code_type: str) -> AsyncGenerator[str, None]:
        """Генерирует Draft."""
        step = "draft"

        # Select prompt based on level
        level = self._level
        if level == "meta_category":
            prompt_template = PROMPT_META_CATEGORY_DRAFT
            level_info = "meta-category (comprehensive extraction)"
        else:
            # For non-meta-categories, inject level-specific instructions
            prompt_template = inject_level_instructions(PROMPT_CODE_RULE_DRAFT, code, code_type)
            has_children = has_descendants(code, code_type)
            level_info = f"{level} ({'has descendants' if has_children else 'leaf code'})"

        yield self._event(step, "status", f"Generating draft... [level: {level_info}]").to_json()

        # Build sources with optional inheritance context
        sources_text = self._sources_ctx.sources_text or ""
        if self._inheritance_context:
            sources_text = self._inheritance_context + "\n\n" + sources_text

        prompt = prompt_template.substitute(
            sources=sources_text,
            code=code,
            code_type=code_type,
            description=self._sources_ctx.code_description or f"See source documents"
        )
        
        # Stream with events
        full_text, thinking, duration_ms = "", "", 0
        async for event_json, final in self._stream_step_with_events(step, prompt):
            if final is not None:
                full_text, thinking, duration_ms = final
            elif event_json:
                yield event_json
        
        # Verify citations (Draft uses indexed format, not inline)
        citations_check = verify_draft_citations(full_text, self._doc_pages)
        
        # Store result (no auto-repair for Draft - repairs happen in Finalization)
        self._results[step] = StepResult(
            step=step,
            output=full_text,
            thinking=thinking,
            duration_ms=duration_ms,
            citations_check=citations_check
        )
        
        yield self._event(step, "verification",
            citations_check=citations_check
        ).to_json()
        
        yield self._event(step, "done",
            full_text=full_text,
            thinking=thinking,
            duration_ms=duration_ms,
            citations_check=citations_check
        ).to_json()
    
    # --------------------------------------------------------
    # STEP: VALIDATORS (Sequential)
    # --------------------------------------------------------
    
    async def _stream_validators_sequential(self) -> AsyncGenerator[str, None]:
        """Запускает Mentor и RedTeam последовательно."""
        draft = self._results["draft"]
        
        # Format citation errors for validators
        citation_errors = format_citation_errors_for_prompt(draft.citations_check)
        
        # MENTOR
        yield self._event("mentor", "status", "Running Mentor validation...").to_json()
        
        mentor_prompt = PROMPT_CODE_RULE_VALIDATION_MENTOR.substitute(
            sources=self._sources_ctx.sources_text,
            instructions=draft.output,
            citation_errors=citation_errors
        )
        
        full_text, thinking, duration_ms = "", "", 0
        async for event_json, final in self._stream_step_with_events("mentor", mentor_prompt):
            if final is not None:
                full_text, thinking, duration_ms = final
            elif event_json:
                yield event_json
        
        self._results["mentor"] = StepResult(
            step="mentor",
            output=full_text,
            thinking=thinking,
            duration_ms=duration_ms,
            corrections_count=self._count_corrections(full_text),
            verdict=self._extract_verdict(full_text)
        )
        
        yield self._event("mentor", "done",
            full_text=full_text,
            thinking=thinking,
            duration_ms=duration_ms
        ).to_json()
        
        # REDTEAM
        yield self._event("redteam", "status", "Running RedTeam validation...").to_json()
        
        redteam_prompt = PROMPT_CODE_RULE_VALIDATION_REDTEAM.substitute(
            sources=self._sources_ctx.sources_text,
            instructions=draft.output,
            citation_errors=citation_errors
        )
        
        full_text, thinking, duration_ms = "", "", 0
        async for event_json, final in self._stream_step_with_events("redteam", redteam_prompt):
            if final is not None:
                full_text, thinking, duration_ms = final
            elif event_json:
                yield event_json
        
        self._results["redteam"] = StepResult(
            step="redteam",
            output=full_text,
            thinking=thinking,
            duration_ms=duration_ms,
            risks_count=self._count_risks(full_text),
            verdict=self._extract_verdict(full_text)
        )
        
        yield self._event("redteam", "done",
            full_text=full_text,
            thinking=thinking,
            duration_ms=duration_ms
        ).to_json()
    
    # --------------------------------------------------------
    # STEP: VALIDATORS (Parallel)
    # --------------------------------------------------------
    
    async def _stream_validators_parallel(self) -> AsyncGenerator[str, None]:
        """Запускает Mentor и RedTeam параллельно с interleaved streaming."""
        draft = self._results["draft"]
        citation_errors = format_citation_errors_for_prompt(draft.citations_check)

        mode = "JSON" if self.json_validators else "Markdown"
        yield self._event("validation", "status", f"Running Mentor + RedTeam in parallel ({mode} mode)...").to_json()

        # Select prompts based on mode
        if self.json_validators:
            mentor_prompt = PROMPT_CODE_RULE_VALIDATION_MENTOR_JSON.substitute(
                sources=self._sources_ctx.sources_text,
                instructions=draft.output,
                citation_errors=citation_errors
            )
            redteam_prompt = PROMPT_CODE_RULE_VALIDATION_REDTEAM_JSON.substitute(
                sources=self._sources_ctx.sources_text,
                instructions=draft.output,
                citation_errors=citation_errors
            )
        else:
            mentor_prompt = PROMPT_CODE_RULE_VALIDATION_MENTOR.substitute(
                sources=self._sources_ctx.sources_text,
                instructions=draft.output,
                citation_errors=citation_errors
            )
            redteam_prompt = PROMPT_CODE_RULE_VALIDATION_REDTEAM.substitute(
                sources=self._sources_ctx.sources_text,
                instructions=draft.output,
                citation_errors=citation_errors
            )
        
        # Create queues for interleaved streaming
        mentor_queue = asyncio.Queue()
        redteam_queue = asyncio.Queue()
        
        mentor_result = {"full_text": "", "thinking": "", "start": time.time()}
        redteam_result = {"full_text": "", "thinking": "", "start": time.time()}
        
        async def stream_to_queue(prompt: str, queue: asyncio.Queue, step: str, result: dict):
            try:
                async for chunk in self._stream_model(prompt, step=step):
                    data = json.loads(chunk.strip())
                    data["step"] = step
                    
                    if data["type"] == "thought":
                        result["thinking"] += data.get("content", "")
                    elif data["type"] == "content":
                        result["full_text"] += data.get("content", "")
                    elif data["type"] == "done":
                        result["full_text"] = data.get("full_text", result["full_text"])
                        result["thinking"] = data.get("think_text", result["thinking"])
                    
                    await queue.put(data)
                    
            except Exception as e:
                await queue.put({"step": step, "type": "error", "content": str(e)})
            finally:
                await queue.put(None)  # Signal completion
        
        # Start parallel tasks
        mentor_task = asyncio.create_task(
            stream_to_queue(mentor_prompt, mentor_queue, "mentor", mentor_result)
        )
        redteam_task = asyncio.create_task(
            stream_to_queue(redteam_prompt, redteam_queue, "redteam", redteam_result)
        )
        
        # Merge streams
        mentor_done = False
        redteam_done = False
        
        while not (mentor_done and redteam_done):
            # Poll mentor queue
            if not mentor_done:
                try:
                    item = await asyncio.wait_for(mentor_queue.get(), timeout=0.05)
                    if item is None:
                        mentor_done = True
                        duration_ms = int((time.time() - mentor_result["start"]) * 1000)
                        self._results["mentor"] = StepResult(
                            step="mentor",
                            output=mentor_result["full_text"],
                            thinking=mentor_result["thinking"],
                            duration_ms=duration_ms,
                            corrections_count=self._count_corrections(mentor_result["full_text"]),
                            verdict=self._extract_verdict(mentor_result["full_text"])
                        )
                        yield self._event("mentor", "done",
                            full_text=mentor_result["full_text"],
                            thinking=mentor_result["thinking"],
                            duration_ms=duration_ms
                        ).to_json()
                    else:
                        yield json.dumps(item, ensure_ascii=False)
                except asyncio.TimeoutError:
                    pass
            
            # Poll redteam queue
            if not redteam_done:
                try:
                    item = await asyncio.wait_for(redteam_queue.get(), timeout=0.05)
                    if item is None:
                        redteam_done = True
                        duration_ms = int((time.time() - redteam_result["start"]) * 1000)
                        self._results["redteam"] = StepResult(
                            step="redteam",
                            output=redteam_result["full_text"],
                            thinking=redteam_result["thinking"],
                            duration_ms=duration_ms,
                            risks_count=self._count_risks(redteam_result["full_text"]),
                            verdict=self._extract_verdict(redteam_result["full_text"])
                        )
                        yield self._event("redteam", "done",
                            full_text=redteam_result["full_text"],
                            thinking=redteam_result["thinking"],
                            duration_ms=duration_ms
                        ).to_json()
                    else:
                        yield json.dumps(item, ensure_ascii=False)
                except asyncio.TimeoutError:
                    pass
        
        # Ensure tasks are done
        await mentor_task
        await redteam_task
    
    # --------------------------------------------------------
    # STEP: ARBITRATION
    # --------------------------------------------------------
    
    async def _stream_arbitration(self) -> AsyncGenerator[str, None]:
        """Генерирует Arbitration (объединение Mentor + RedTeam)."""
        step = "arbitration"
        mode = "JSON" if self.json_validators else "Markdown"
        yield self._event(step, "status", f"Running arbitration ({mode} mode)...").to_json()

        draft = self._results["draft"]
        mentor = self._results.get("mentor")
        redteam = self._results.get("redteam")

        if not mentor or not redteam:
            yield self._event(step, "error", "Missing validator results").to_json()
            return

        citation_errors = format_citation_errors_for_prompt(draft.citations_check)

        # Select prompt based on mode
        if self.json_validators:
            prompt = PROMPT_CODE_RULE_VALIDATION_ARBITRATION_JSON.substitute(
                sources=self._sources_ctx.sources_text,
                instructions=draft.output,
                verdict1=mentor.output,
                verdict2=redteam.output,
                citation_errors=citation_errors
            )
        else:
            prompt = PROMPT_CODE_RULE_VALIDATION_ARBITRATION.substitute(
                sources=self._sources_ctx.sources_text,
                instructions=draft.output,
                verdict1=mentor.output,
                verdict2=redteam.output,
                citation_errors=citation_errors
            )
        
        full_text, thinking, duration_ms = "", "", 0
        async for event_json, final in self._stream_step_with_events(step, prompt):
            if final is not None:
                full_text, thinking, duration_ms = final
            elif event_json:
                yield event_json
        
        self._results[step] = StepResult(
            step=step,
            output=full_text,
            thinking=thinking,
            duration_ms=duration_ms,
            corrections_count=self._count_approved_corrections(full_text)
        )
        
        yield self._event(step, "done",
            full_text=full_text,
            thinking=thinking,
            duration_ms=duration_ms
        ).to_json()
    
    # --------------------------------------------------------
    # STEP: FINALIZATION
    # --------------------------------------------------------
    
    async def _stream_finalization(self) -> AsyncGenerator[str, None]:
        """Генерирует финальную версию правила."""
        step = "finalization"
        yield self._event(step, "status", "Generating final version...").to_json()
        
        draft = self._results["draft"]
        arbitration = self._results.get("arbitration")
        
        if not arbitration:
            yield self._event(step, "error", "Missing arbitration result").to_json()
            return
        
        prompt = PROMPT_CODE_RULE_FINALIZATION.substitute(
            sources=self._sources_ctx.sources_text,
            instructions=draft.output,
            corrections=arbitration.output
        )
        
        full_text, thinking, duration_ms = "", "", 0
        async for event_json, final in self._stream_step_with_events(step, prompt):
            if final is not None:
                full_text, thinking, duration_ms = final
            elif event_json:
                yield event_json
        
        # Final citation verification
        citations_check = verify_citations(full_text, self._doc_pages)
        
        # Apply auto-repairs
        if citations_check.get("repaired"):
            full_text = apply_repairs_to_output(full_text, citations_check["repaired"])
            yield self._event(step, "verification",
                content=f"Auto-repaired {len(citations_check['repaired'])} citations"
            ).to_json()
        
        self._results[step] = StepResult(
            step=step,
            output=full_text,
            thinking=thinking,
            duration_ms=duration_ms,
            citations_check=citations_check
        )
        
        yield self._event(step, "verification",
            citations_check=citations_check
        ).to_json()
        
        yield self._event(step, "done",
            full_text=full_text,
            thinking=thinking,
            duration_ms=duration_ms,
            citations_check=citations_check
        ).to_json()
    
    # --------------------------------------------------------
    # HELPERS
    # --------------------------------------------------------
    
    async def _stream_step_with_events(
        self, 
        step: str, 
        prompt: str
    ) -> AsyncGenerator[Tuple[str, Optional[Tuple[str, str, int]]], None]:
        """
        Стримит один шаг, yield'ит SSE events, в конце yield'ит результат.
        
        Yields:
            (event_json, None) - для промежуточных событий
            ("", (full_text, thinking, duration_ms)) - финальный результат
        """
        start_time = time.time()
        full_text = ""
        thinking = ""

        async for chunk in self._stream_model(prompt, step=step):
            data = json.loads(chunk.strip())
            
            if data["type"] == "thought":
                thinking += data.get("content", "")
                # Yield preview every ~500 chars
                if len(thinking) % 500 < 50:
                    event = self._event(step, "thought",
                        content=data.get("content", ""),
                        think_preview=thinking[-200:] if len(thinking) > 200 else thinking
                    ).to_json()
                    yield (event, None)
                    
            elif data["type"] == "content":
                full_text += data.get("content", "")
                # Yield preview every ~300 chars
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
    
    async def _collect_step(self, step: str, prompt: str) -> Tuple[str, str, int]:
        """
        Выполняет шаг без стриминга событий, только собирает результат.
        
        Returns:
            (full_text, thinking, duration_ms)
        """
        result = None
        async for event, final in self._stream_step_with_events(step, prompt):
            if final is not None:
                result = final
        return result
    
    def _event(self, step: str, type: str, content: str = "", **kwargs) -> SSEEvent:
        """Создаёт SSE событие."""
        return SSEEvent(step=step, type=type, content=content, **kwargs)
    
    def _count_corrections(self, text: str) -> int:
        """Считает количество corrections в output Mentor."""
        # Try JSON first (for json_validators mode)
        parsed = parse_json_safely(text)
        if parsed and "corrections" in parsed:
            return len(parsed["corrections"])
        # Fallback to markdown markers
        count = 0
        for marker in ["**CLARIFY**", "**CHANGE**", "**ADD_SOURCE**", "**FIX_PAGE**", "**FIX_DOC**"]:
            count += text.count(marker)
        return count

    def _count_risks(self, text: str) -> int:
        """Считает количество рисков в output RedTeam."""
        # Try JSON first
        parsed = parse_json_safely(text)
        if parsed and "risks_found" in parsed:
            return parsed["risks_found"]
        if parsed and "corrections" in parsed:
            return len(parsed["corrections"])
        # Fallback to markdown markers
        return text.count("**FIX RISK**") + text.count("**Risk Scenario:**")

    def _count_approved_corrections(self, text: str) -> int:
        """Считает количество approved corrections в Arbitration."""
        # Try JSON first
        parsed = parse_json_safely(text)
        if parsed and "approved_corrections" in parsed:
            return len(parsed["approved_corrections"])
        # Fallback to markdown markers
        count = 0
        for marker in ["[BLOCK_RISK]", "[ADD_STEP]", "[CLARIFY]", "[FIX_PAGE]", "[FIX_DOC]"]:
            count += text.count(marker)
        return count

    def _extract_verdict(self, text: str) -> str:
        """Извлекает verdict из output валидатора."""
        # Try JSON first
        parsed = parse_json_safely(text)
        if parsed and "verdict" in parsed:
            return parsed["verdict"]
        if parsed and "safety_status" in parsed:
            return parsed["safety_status"]
        import re
        # Look for "## N. VERDICT: XXX" pattern
        match = re.search(r'VERDICT:\s*(\w+)', text, re.IGNORECASE)
        return match.group(1) if match else ""
    
    def _get_next_version(self, code: str) -> int:
        """Finds the next version number for a code."""
        code_dir = code.replace(".", "_").replace("/", "_")
        code_path = os.path.join(RULES_DIR, code_dir)

        if not os.path.exists(code_path):
            return 1

        # Find existing versions (v1, v2, v3, ...)
        existing_versions = []
        for name in os.listdir(code_path):
            if name.startswith('v') and name[1:].isdigit():
                existing_versions.append(int(name[1:]))

        if not existing_versions:
            return 1

        return max(existing_versions) + 1

    def _build_pipeline_result(self, code: str, total_ms: int) -> PipelineResult:
        """Собирает полный результат пайплайна."""
        next_version = self._get_next_version(code)

        return PipelineResult(
            code=code,
            version=next_version,
            created_at=datetime.utcnow().isoformat() + "Z",
            source_documents=[
                {
                    "doc_id": doc.doc_id,
                    "filename": doc.filename,
                    "pages": doc.pages
                }
                for doc in self._sources_ctx.source_documents
            ],
            steps=self._results,
            total_duration_ms=total_ms,
            final_output=self._results.get("finalization", StepResult("", "")).output,
            final_citations_check=self._results.get("finalization", StepResult("", "")).citations_check,
            status="completed"
        )
    
    def _save_results(self, code: str, result: PipelineResult) -> str:
        """
        Сохраняет результаты в файлы.
        
        Structure:
            data/rules/E11_9/v1/
                generation_log.json  - полный лог пайплайна
                rule.json            - финальное правило
                rule.md              - markdown версия
        """
        # Sanitize code for path
        code_dir = code.replace(".", "_").replace("/", "_")
        version_dir = os.path.join(RULES_DIR, code_dir, f"v{result.version}")
        os.makedirs(version_dir, exist_ok=True)
        
        # Save generation log
        log_path = os.path.join(version_dir, "generation_log.json")

        # Extract parent pattern from inheritance context if present
        parent_pattern = None
        if self._inheritance_context:
            import re
            match = re.search(r"=== PARENT GUIDELINE RULE: ([A-Z0-9.]+) ===", self._inheritance_context)
            if match:
                parent_pattern = match.group(1)

        log_data = {
            "code": result.code,
            "version": result.version,
            "created_at": result.created_at,
            "source_documents": result.source_documents,
            "inheritance": {
                "context_used": bool(self._inheritance_context),
                "context_length": len(self._inheritance_context) if self._inheritance_context else 0,
                "parent_pattern": parent_pattern
            },
            "pipeline": {
                step: {
                    "output": r.output,
                    "thinking": r.thinking,
                    "duration_ms": r.duration_ms,
                    "citations_check": r.citations_check,
                    "corrections_count": r.corrections_count,
                    "risks_count": r.risks_count,
                    "verdict": r.verdict
                }
                for step, r in result.steps.items()
            },
            "total_duration_ms": result.total_duration_ms,
            "status": result.status
        }
        
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False)
        
        # Save final rule
        rule_path = os.path.join(version_dir, "rule.json")
        final_step = result.steps.get("finalization")
        
        rule_data = {
            "code": result.code,
            "version": result.version,
            "content": result.final_output,
            "citations": self._extract_citations(result.final_output),
            "validation_status": "verified" if self._is_verified(result.final_citations_check) else "has_issues",
            "citations_summary": result.final_citations_check,
            "source_doc_ids": [d["doc_id"] for d in result.source_documents],
            "created_at": result.created_at,
            "total_duration_ms": result.total_duration_ms
        }
        
        with open(rule_path, 'w', encoding='utf-8') as f:
            json.dump(rule_data, f, indent=2, ensure_ascii=False)
        
        # Save markdown
        md_path = os.path.join(version_dir, "rule.md")
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(f"# Rule: {code}\n\n")
            f.write(f"Version: {result.version}\n")
            f.write(f"Generated: {result.created_at}\n\n")
            f.write("---\n\n")
            f.write(result.final_output)
        
        return version_dir
    
    def _extract_citations(self, text: str) -> List[Dict]:
        """Извлекает все цитаты из текста."""
        import re
        citations = []
        # Pattern: [[doc_id:page | "anchor"]]
        pattern = r'\[\[([a-f0-9]+):(\d+)\s*\|\s*"([^"]+)"\]\]'
        
        for match in re.finditer(pattern, text, re.IGNORECASE):
            citations.append({
                "doc_id": match.group(1),
                "page": int(match.group(2)),
                "anchor": match.group(3)
            })
        
        return citations
    
    def _is_verified(self, citations_check: Optional[Dict]) -> bool:
        """Проверяет что все цитаты верифицированы."""
        if not citations_check:
            return False
        
        failed = len(citations_check.get("failed", []))
        wrong_page = len(citations_check.get("wrong_page", []))
        wrong_doc = len(citations_check.get("wrong_doc", []))
        
        return failed == 0 and wrong_page == 0 and wrong_doc == 0


# ============================================================
# CONVENIENCE FUNCTION
# ============================================================

async def generate_rule_stream(
    code: str,
    document_ids: Optional[List[str]] = None,
    code_type: str = "ICD-10",
    parallel: bool = False,
    thinking_budget: int = 10000,
    level: Optional[str] = None,
) -> AsyncGenerator[str, None]:
    """
    Convenience function для стриминга генерации правила.

    Args:
        code: Code to generate rules for
        document_ids: Optional specific documents
        code_type: ICD-10, CPT, HCPCS
        parallel: Run validators in parallel
        thinking_budget: Token budget for thinking
        level: Hierarchical level (meta_category, category, subcategory, code)
               Auto-detected if not provided.

    Usage:
        async for event in generate_rule_stream("E11.9"):
            yield f"data: {event}\\n\\n"
    """
    generator = RuleGenerator(thinking_budget=thinking_budget)
    async for event in generator.stream_pipeline(
        code, document_ids, code_type, parallel, level=level
    ):
        yield event
