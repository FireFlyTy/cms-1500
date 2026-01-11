# Citation Validation Module
#
# Multi-document citation verification with:
#   - Two-pass verification (exact + fuzzy)
#   - Auto-repair for minor mismatches
#   - Cross-document validation
#   - doc_id tracking

from .citations import (
    parse_sources_to_pages,
    verify_citations,
    format_citation_errors_for_prompt,
    apply_repairs_to_output,
)

__all__ = [
    'parse_sources_to_pages',
    'verify_citations',
    'format_citation_errors_for_prompt',
    'apply_repairs_to_output',
]
