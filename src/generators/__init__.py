# Rule Generation Pipeline
# 
# Modules:
#   - core_ai.py: LLM streaming (Gemini/OpenAI)
#   - prompts.py: Multi-document prompt templates
#   - context_builder.py: Source assembly from KB
#   - rule_generator.py: Pipeline orchestrator

from .core_ai import stream_gemini_generator
from .context_builder import (
    build_sources_context, 
    format_sources_for_prompt,
    SourcesContext,
    SourceDocument,
)
from .rule_generator import (
    RuleGenerator,
    generate_rule_stream,
    PipelineResult,
    StepResult,
    SSEEvent,
)

__all__ = [
    # Core AI
    'stream_gemini_generator',
    
    # Context Builder
    'build_sources_context', 
    'format_sources_for_prompt',
    'SourcesContext',
    'SourceDocument',
    
    # Rule Generator
    'RuleGenerator',
    'generate_rule_stream',
    'PipelineResult',
    'StepResult',
    'SSEEvent',
]
