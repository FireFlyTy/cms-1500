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
from .core_ai import stream_gemini_generator
from .prompts import (
    PROMPT_CODE_RULE_DRAFT,
    PROMPT_CODE_RULE_VALIDATION_MENTOR,
    PROMPT_CODE_RULE_VALIDATION_REDTEAM,
    PROMPT_CODE_RULE_VALIDATION_ARBITRATION,
    PROMPT_CODE_RULE_FINALIZATION,
)

# Import validators
from src.validators.citations import (
    parse_sources_to_pages,
    verify_citations,
    format_citation_errors_for_prompt,
    apply_repairs_to_output,
)


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RULES_DIR = os.path.join(BASE_DIR, "data", "rules")

os.makedirs(RULES_DIR, exist_ok=True)


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
    
    def __init__(self, thinking_budget: int = 10000):
        self.thinking_budget = thinking_budget
        self._sources_ctx: Optional[SourcesContext] = None
        self._doc_pages: Optional[Dict] = None
        self._results: Dict[str, StepResult] = {}
    
    async def stream_pipeline(
        self,
        code: str,
        document_ids: Optional[List[str]] = None,
        code_type: str = "ICD-10",
        parallel_validators: bool = False,
    ) -> AsyncGenerator[str, None]:
        """
        Стримит полный пайплайн генерации правила.
        
        Args:
            code: ICD-10 код
            document_ids: Опционально - конкретные документы
            code_type: Тип кода (ICD-10, CPT, etc.)
            parallel_validators: True для параллельного запуска Mentor+RedTeam
            
        Yields:
            JSON SSE events
        """
        pipeline_start = time.time()
        
        try:
            # 1. Build sources context
            yield self._event("pipeline", "status", "Building sources context...").to_json()
            
            self._sources_ctx = build_sources_context(code, document_ids)
            
            if not self._sources_ctx.sources_text:
                yield self._event("pipeline", "error", f"No documents found for code {code}").to_json()
                return
            
            # Parse sources for citation verification
            self._doc_pages = parse_sources_to_pages(self._sources_ctx.sources_text)
            
            yield self._event("pipeline", "status", 
                f"Found {len(self._sources_ctx.source_documents)} documents, "
                f"{self._sources_ctx.total_pages} pages"
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
        yield self._event(step, "status", "Generating draft...").to_json()
        
        prompt = PROMPT_CODE_RULE_DRAFT.substitute(
            sources=self._sources_ctx.sources_text,
            code=code,
            code_type=code_type
        )
        
        # Stream with events
        full_text, thinking, duration_ms = "", "", 0
        async for event_json, final in self._stream_step_with_events(step, prompt):
            if final is not None:
                full_text, thinking, duration_ms = final
            elif event_json:
                yield event_json
        
        # Verify citations
        citations_check = verify_citations(full_text, self._doc_pages)
        
        # Apply auto-repairs if any
        if citations_check.get("repaired"):
            full_text = apply_repairs_to_output(full_text, citations_check["repaired"])
            yield self._event(step, "verification", 
                content=f"Auto-repaired {len(citations_check['repaired'])} citations"
            ).to_json()
        
        # Store result
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
        
        yield self._event("validation", "status", "Running Mentor + RedTeam in parallel...").to_json()
        
        # Prepare prompts
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
                async for chunk in stream_gemini_generator(prompt, self.thinking_budget):
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
        yield self._event(step, "status", "Running arbitration...").to_json()
        
        draft = self._results["draft"]
        mentor = self._results.get("mentor")
        redteam = self._results.get("redteam")
        
        if not mentor or not redteam:
            yield self._event(step, "error", "Missing validator results").to_json()
            return
        
        citation_errors = format_citation_errors_for_prompt(draft.citations_check)
        
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
        
        async for chunk in stream_gemini_generator(prompt, self.thinking_budget):
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
        count = 0
        for marker in ["**CLARIFY**", "**CHANGE**", "**ADD_SOURCE**", "**FIX_PAGE**", "**FIX_DOC**"]:
            count += text.count(marker)
        return count
    
    def _count_risks(self, text: str) -> int:
        """Считает количество рисков в output RedTeam."""
        return text.count("**FIX RISK**") + text.count("**Risk Scenario:**")
    
    def _count_approved_corrections(self, text: str) -> int:
        """Считает количество approved corrections в Arbitration."""
        count = 0
        for marker in ["[BLOCK_RISK]", "[ADD_STEP]", "[CLARIFY]", "[FIX_PAGE]", "[FIX_DOC]"]:
            count += text.count(marker)
        return count
    
    def _extract_verdict(self, text: str) -> str:
        """Извлекает verdict из output валидатора."""
        import re
        # Look for "## N. VERDICT: XXX" pattern
        match = re.search(r'VERDICT:\s*(\w+)', text, re.IGNORECASE)
        return match.group(1) if match else ""
    
    def _build_pipeline_result(self, code: str, total_ms: int) -> PipelineResult:
        """Собирает полный результат пайплайна."""
        return PipelineResult(
            code=code,
            version=1,  # TODO: versioning
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
        log_data = {
            "code": result.code,
            "version": result.version,
            "created_at": result.created_at,
            "source_documents": result.source_documents,
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
    thinking_budget: int = 10000
) -> AsyncGenerator[str, None]:
    """
    Convenience function для стриминга генерации правила.
    
    Usage:
        async for event in generate_rule_stream("E11.9"):
            yield f"data: {event}\\n\\n"
    """
    generator = RuleGenerator(thinking_budget=thinking_budget)
    async for event in generator.stream_pipeline(code, document_ids, code_type, parallel):
        yield event
