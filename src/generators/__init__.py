# Rule Generation Pipeline
#
# Modules:
#   - core_ai.py: LLM streaming (Gemini/OpenAI)
#   - prompts.py: Multi-document prompt templates
#   - context_builder.py: Source assembly from KB
#   - rule_generator.py: Guideline rule pipeline orchestrator
#   - cms_generator.py: CMS-1500 claim rules pipeline

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
from .cms_generator import (
    CMSRuleGenerator,
    generate_cms_rule_stream,
    CMSPipelineResult,
    CMSStepResult,
    NCCIEdits,
)

__all__ = [
    # Core AI
    'stream_gemini_generator',

    # Context Builder
    'build_sources_context',
    'format_sources_for_prompt',
    'SourcesContext',
    'SourceDocument',

    # Guideline Rule Generator
    'RuleGenerator',
    'generate_rule_stream',
    'PipelineResult',
    'StepResult',
    'SSEEvent',

    # CMS Rule Generator
    'CMSRuleGenerator',
    'generate_cms_rule_stream',
    'CMSPipelineResult',
    'CMSStepResult',
    'NCCIEdits',
]
