import React, { useState, useEffect, useCallback } from 'react';
import {
  ChevronRight, ChevronDown, CheckCircle, Clock, AlertCircle,
  Loader2, Hash, FileText, Search, RefreshCw, Zap, X, Trash2, Eye,
  BookOpen, BrainCircuit, ShieldCheck, Gavel, Users, Swords
} from 'lucide-react';
import GenerationProgress from './GenerationProgress';
import PdfViewer from './PdfViewer';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

const API_BASE = 'http://localhost:8001/api/rules';

// =============================================================================
// HOOKS
// =============================================================================

function useCategoriesByType() {
  const [data, setData] = useState({});
  const [loading, setLoading] = useState(true);

  const fetch_ = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/categories-by-type?rule_type=guideline`);
      const json = await res.json();
      setData(json);
    } catch (err) {
      console.error('Failed to fetch categories by type:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetch_(); }, [fetch_]);

  return { data, loading, refetch: fetch_ };
}

function useCategoryCode(metaCategory, codeType) {
  const [data, setData] = useState({
    diagnoses: [],
    procedures: [],
    total: 0,
    total_diagnoses: 0,
    total_procedures: 0,
    with_rules: 0
  });
  const [loading, setLoading] = useState(false);

  const fetchCodes = useCallback(() => {
    if (!metaCategory || !codeType) return;

    setLoading(true);
    fetch(`${API_BASE}/codes-by-meta/${encodeURIComponent(codeType)}/${encodeURIComponent(metaCategory)}`)
      .then(res => res.json())
      .then(json => setData(json))
      .catch(err => console.error('Failed to fetch codes:', err))
      .finally(() => setLoading(false));
  }, [metaCategory, codeType]);

  useEffect(() => {
    fetchCodes();
  }, [fetchCodes]);

  return { ...data, loading, refetch: fetchCodes };
}

// =============================================================================
// HIERARCHICAL CATEGORY LIST
// =============================================================================

const CODE_TYPE_ORDER = ['ICD-10', 'CPT', 'HCPCS'];

const HierarchicalCategoryList = ({
  categoriesByType,
  selectedCodeType,
  selectedMetaCategory,
  onSelectCategory,
  loading,
  viewFilter = 'actionable'
}) => {
  const [expandedTypes, setExpandedTypes] = useState(['ICD-10', 'CPT', 'HCPCS']);

  // Helper to get count based on filter
  const getFilteredCount = (cat) => {
    switch (viewFilter) {
      case 'generated': return cat.with_rules || 0;
      case 'ready': return cat.ready || 0;
      case 'actionable': return (cat.with_sources || 0);
      case 'all': return cat.total || 0;
      default: return cat.total || 0;
    }
  };

  const getFilteredTotal = (typeData) => {
    switch (viewFilter) {
      case 'generated': return typeData.codes_with_rules || 0;
      case 'ready': return typeData.codes_ready || 0;
      case 'actionable': return typeData.codes_with_sources || 0;
      case 'all': return typeData.total_codes || 0;
      default: return typeData.total_codes || 0;
    }
  };

  const toggleType = (codeType) => {
    setExpandedTypes(prev =>
      prev.includes(codeType) ? prev.filter(t => t !== codeType) : [...prev, codeType]
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
      </div>
    );
  }

  if (Object.keys(categoriesByType).length === 0) {
    return (
      <div className="text-center py-8 text-gray-400">
        <p>No categories found</p>
        <p className="text-sm mt-1">Parse documents first</p>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {CODE_TYPE_ORDER.map(codeType => {
        const typeData = categoriesByType[codeType];
        if (!typeData || typeData.categories.length === 0) return null;

        // Filter categories based on viewFilter
        const filteredCategories = typeData.categories.filter(cat => getFilteredCount(cat) > 0);
        const filteredTotal = getFilteredTotal(typeData);

        // Hide entire code type if no categories match filter
        if (filteredCategories.length === 0) return null;

        const isExpanded = expandedTypes.includes(codeType);
        const coveragePercent = filteredTotal > 0
          ? Math.round((typeData.codes_with_rules / filteredTotal) * 100)
          : 0;

        return (
          <div key={codeType} className="border rounded-lg overflow-hidden" style={{ borderColor: '#e5e7eb' }}>
            {/* Code Type Header */}
            <button
              onClick={() => toggleType(codeType)}
              className="w-full flex items-center gap-2 p-3 bg-gray-50 hover:bg-gray-100 transition-colors"
            >
              {isExpanded ? (
                <ChevronDown className="w-4 h-4 text-gray-500" />
              ) : (
                <ChevronRight className="w-4 h-4 text-gray-500" />
              )}
              <Hash className="w-4 h-4 text-gray-600" />
              <span className="font-semibold text-sm flex-1 text-left">{codeType}</span>
              <span className="text-xs text-gray-500 font-mono">
                {viewFilter === 'generated' || viewFilter === 'ready'
                  ? filteredTotal
                  : `${typeData.codes_with_rules}/${filteredTotal}`}
              </span>
              <div
                className="w-16 h-1.5 rounded-full overflow-hidden"
                style={{ background: '#e5e7eb' }}
              >
                <div
                  className="h-full rounded-full"
                  style={{
                    width: `${coveragePercent}%`,
                    background: coveragePercent === 100 ? '#059669' : coveragePercent > 0 ? '#3b82f6' : '#e5e7eb'
                  }}
                />
              </div>
            </button>

            {/* Meta Categories */}
            {isExpanded && (
              <div className="border-t" style={{ borderColor: '#e5e7eb' }}>
                {filteredCategories.map(cat => {
                  const isSelected = selectedCodeType === codeType && selectedMetaCategory === cat.name;
                  const filteredCount = getFilteredCount(cat);
                  const catCoverage = filteredCount > 0 ? Math.round((cat.with_rules / filteredCount) * 100) : 0;

                  return (
                    <button
                      key={cat.name}
                      onClick={() => onSelectCategory(codeType, cat.name)}
                      className="w-full text-left p-2.5 pl-10 transition-all border-b last:border-b-0"
                      style={{
                        background: isSelected ? '#eff6ff' : 'white',
                        borderColor: '#f3f4f6',
                      }}
                      onMouseEnter={(e) => {
                        if (!isSelected) e.currentTarget.style.background = '#f9fafb';
                      }}
                      onMouseLeave={(e) => {
                        if (!isSelected) e.currentTarget.style.background = 'white';
                      }}
                    >
                      <div className="flex items-center justify-between">
                        <span
                          className="font-mono font-bold text-sm"
                          style={{ color: isSelected ? '#2563eb' : '#374151' }}
                        >
                          {cat.name}
                        </span>
                        <span className="text-xs font-mono" style={{ color: '#6b7280' }}>
                          {viewFilter === 'generated' || viewFilter === 'ready'
                            ? filteredCount
                            : `${cat.with_rules}/${filteredCount}`}
                        </span>
                      </div>
                      <div className="text-xs text-gray-500 mt-0.5">
                        {cat.description}
                      </div>
                      {/* Mini progress bar */}
                      <div className="mt-1.5 flex items-center gap-2">
                        <div
                          className="flex-1 h-1 rounded-full overflow-hidden"
                          style={{ background: '#e5e7eb' }}
                        >
                          <div
                            className="h-full rounded-full"
                            style={{
                              width: `${catCoverage}%`,
                              background: catCoverage === 100 ? '#059669' : catCoverage > 0 ? '#3b82f6' : '#e5e7eb'
                            }}
                          />
                        </div>
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
};

// =============================================================================
// CODE ITEM
// =============================================================================

const CodeItem = ({ code, isSelected, canSelect, generating, onToggleCode, onDeleteRule, onViewRule, onGenerate }) => {
  const hasRule = code.rule_status?.has_rule;
  const isMock = code.rule_status?.is_mock;
  const isInheritedRule = code.rule_status?.is_inherited;
  const matchedPattern = code.rule_status?.matched_pattern;
  const [deleting, setDeleting] = useState(false);

  // Code type flags
  const isWildcard = code.is_wildcard || code.code?.includes('%');
  const isRange = code.is_range || code.code?.includes(':');

  // Document counts
  const ownDocs = code.documents?.length || 0;
  const inheritedDocs = code.inherited_documents?.length || 0;
  const totalDocs = code.total_docs || ownDocs + inheritedDocs;
  const hasSources = totalDocs > 0;

  const handleDelete = async (e) => {
    e.stopPropagation();
    if (!window.confirm(`Delete rule for ${code.code}?`)) return;

    setDeleting(true);
    try {
      await onDeleteRule(code.code);
    } finally {
      setDeleting(false);
    }
  };

  const handleView = (e) => {
    e.stopPropagation();
    if (onViewRule) onViewRule(code.code);
  };

  // Left border color based on status
  const borderLeftColor = hasRule && !isMock ? '#059669' : hasSources ? '#0090DA' : '#d1d5db';

  return (
    <div
      className="rounded-lg transition-all"
      style={{
        background: isSelected ? '#f0fdfa' : 'white',
        border: '1px solid #e5e7eb',
        borderLeft: `4px solid ${borderLeftColor}`
      }}
    >
      <div className="flex items-center gap-3 p-3">
        {/* Checkbox for all codes with sources */}
        {canSelect && hasSources ? (
          <input
            type="checkbox"
            checked={isSelected}
            onChange={() => onToggleCode(code.code)}
            disabled={generating}
            className="w-4 h-4 text-teal-600 rounded"
            onClick={(e) => e.stopPropagation()}
          />
        ) : canSelect ? (
          <div className="w-4 h-4" />
        ) : null}

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-mono font-medium text-sm" style={{ color: '#1a1a1a' }}>{code.code}</span>
            <span
              className="text-xs px-1.5 py-0.5 rounded"
              style={{ background: '#f3f4f6', color: '#6b7280' }}
            >
              {code.type || 'ICD-10'}
            </span>
            {isWildcard && (
              <span
                className="text-xs px-1.5 py-0.5 rounded"
                style={{ background: '#f3f4f6', color: '#6b7280' }}
                title="Wildcard pattern"
              >
                wildcard
              </span>
            )}
            {isRange && (
              <span
                className="text-xs px-1.5 py-0.5 rounded"
                style={{ background: '#f3f4f6', color: '#6b7280' }}
                title="Range pattern"
              >
                range
              </span>
            )}
          </div>

          <div className="mt-1.5 flex items-center gap-3 text-xs">
            <span className="flex items-center gap-1" style={{ color: ownDocs > 0 ? '#0090DA' : '#9ca3af' }}>
              <FileText className="w-3 h-3" />
              {ownDocs} docs
              {ownDocs > 0 && <CheckCircle className="w-3 h-3" style={{ color: '#059669' }} />}
            </span>
            {inheritedDocs > 0 && (
              <span className="flex items-center gap-1" style={{ color: '#7c3aed' }}>
                +{inheritedDocs} inherited
              </span>
            )}
          </div>
        </div>

        {/* Action zone - right side */}
        <div className="flex items-center gap-2 shrink-0">
          {hasRule && !isMock ? (
            <>
              <button
                onClick={handleView}
                className="px-3 py-1.5 text-xs rounded flex items-center gap-1.5 transition-colors"
                style={{
                  color: '#059669',
                  background: 'transparent',
                  border: '1px solid #059669'
                }}
                onMouseEnter={(e) => e.currentTarget.style.background = '#f0fdf4'}
                onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                title="View rule"
              >
                <Eye className="w-3.5 h-3.5" />
                View
              </button>
              <span
                className="text-xs font-medium px-2 py-1 rounded"
                style={{ background: '#f0fdf4', color: '#059669' }}
              >
                v{code.rule_status.guideline_version || code.rule_status.version || 1}
              </span>
              {isInheritedRule && matchedPattern && (
                <span
                  className="text-xs px-1.5 py-0.5 rounded"
                  style={{ background: '#f3f4f6', color: '#7c3aed' }}
                  title={`Inherited from ${matchedPattern}`}
                >
                  via {matchedPattern}
                </span>
              )}
              {/* Regenerate button */}
              <button
                onClick={(e) => { e.stopPropagation(); onGenerate?.(code.code, true); }}
                disabled={generating}
                className="px-2 py-1.5 text-xs rounded flex items-center gap-1 transition-colors"
                style={{
                  color: generating ? '#9ca3af' : '#6b7280',
                  background: 'transparent',
                  border: '1px solid #d1d5db',
                  cursor: generating ? 'not-allowed' : 'pointer'
                }}
                onMouseEnter={(e) => !generating && (e.currentTarget.style.background = '#f3f4f6')}
                onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                title="Regenerate rule"
              >
                <RefreshCw className="w-3 h-3" />
              </button>
              {/* Delete button */}
              <button
                onClick={handleDelete}
                disabled={deleting}
                className="px-2 py-1.5 text-xs rounded flex items-center gap-1 transition-colors"
                style={{
                  color: deleting ? '#9ca3af' : '#6b7280',
                  background: 'transparent',
                  border: '1px solid #d1d5db',
                  cursor: deleting ? 'not-allowed' : 'pointer'
                }}
                onMouseEnter={(e) => !deleting && (e.currentTarget.style.background = '#fef2f2', e.currentTarget.style.color = '#dc2626', e.currentTarget.style.borderColor = '#fecaca')}
                onMouseLeave={(e) => (e.currentTarget.style.background = 'transparent', e.currentTarget.style.color = '#6b7280', e.currentTarget.style.borderColor = '#d1d5db')}
                title="Delete rule"
              >
                {deleting ? <Loader2 className="w-3 h-3 animate-spin" /> : <X className="w-3 h-3" />}
              </button>
            </>
          ) : hasRule && isMock ? (
            <span className="text-xs px-2 py-1 rounded" style={{ background: '#fef3c7', color: '#d97706' }}>
              mock v{code.rule_status.guideline_version || code.rule_status.version || 1}
            </span>
          ) : hasSources ? (
            <button
              onClick={(e) => { e.stopPropagation(); onGenerate?.(code.code); }}
              disabled={generating}
              className="px-3 py-1.5 text-xs rounded flex items-center gap-1.5 transition-colors"
              style={{
                color: generating ? '#9ca3af' : '#0090DA',
                background: 'transparent',
                border: generating ? '1px solid #d1d5db' : '1px solid #0090DA',
                cursor: generating ? 'not-allowed' : 'pointer'
              }}
              onMouseEnter={(e) => !generating && (e.currentTarget.style.background = '#eff6ff')}
              onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
              title="Generate rule"
            >
              <Zap className="w-3.5 h-3.5" />
              Generate
            </button>
          ) : (
            <span className="text-xs" style={{ color: '#9ca3af' }}>No sources</span>
          )}
        </div>
      </div>
    </div>
  );
};

// =============================================================================
// RULE VIEWER MODAL
// =============================================================================

const STEP_CONFIG = {
  draft: { title: 'Draft', icon: BookOpen, color: 'blue' },
  mentor: { title: 'Mentor', icon: Users, color: 'purple' },
  redteam: { title: 'Red Team', icon: Swords, color: 'red' },
  arbitration: { title: 'Arbitration', icon: Gavel, color: 'amber' },
  finalization: { title: 'Final', icon: ShieldCheck, color: 'green' }
};

// Citation component - shows short filename + page, tooltip shows full details
const Citation = ({ docId, page, anchor, docMap }) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const doc = docMap?.[docId];
  const filename = doc?.filename || `Doc ${docId}`;

  // Create short filename: remove extension, truncate if too long
  const shortName = filename
    .replace(/\.(pdf|PDF)$/, '')
    .replace(/_/g, ' ')
    .slice(0, 20) + (filename.length > 24 ? '…' : '');

  return (
    <span
      className="relative inline-flex items-center align-baseline"
      onMouseEnter={() => setShowTooltip(true)}
      onMouseLeave={() => setShowTooltip(false)}
    >
      <sup className="inline-flex items-center gap-0.5 px-1 py-0.5 mx-0.5 bg-slate-100 hover:bg-slate-200 text-slate-600 text-[10px] rounded cursor-help transition-colors font-medium">
        <FileText className="w-2.5 h-2.5" />
        <span>{shortName}</span>
        <span className="text-slate-400">p.{page}</span>
      </sup>
      {showTooltip && (
        <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-xs rounded-lg shadow-lg z-50 max-w-xs">
          <div className="font-medium text-gray-100 mb-1">{filename}</div>
          <div className="text-gray-400 text-[10px] mb-1">Page {page} • ID: {docId}</div>
          {anchor && (
            <div className="text-gray-300 italic text-[10px] border-t border-gray-700 pt-1 mt-1">
              "{anchor.length > 80 ? anchor.slice(0, 80) + '…' : anchor}"
            </div>
          )}
          <span className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-gray-900"></span>
        </span>
      )}
    </span>
  );
};

// Parse content and replace citations with components
const RuleContent = ({ content, docMap }) => {
  if (!content) return null;

  // Remove TRACEABILITY LOG and everything after
  let cleanContent = content;
  const traceabilityIndex = cleanContent.indexOf('## TRACEABILITY LOG');
  if (traceabilityIndex > -1) {
    cleanContent = cleanContent.substring(0, traceabilityIndex).trim();
  }
  const logIndex = cleanContent.indexOf('TRACEABILITY LOG');
  if (logIndex > -1) {
    cleanContent = cleanContent.substring(0, logIndex).trim();
  }

  // Process text to replace citations with React components
  // Format: [[doc_id:page | "anchor"]]
  const processTextWithCitations = (text) => {
    if (typeof text !== 'string') return text;

    const regex = /\[\[([a-f0-9]+):(\d+)\s*\|\s*"([^"]+)"\]\]/gi;
    const result = [];
    let lastIndex = 0;
    let match;

    while ((match = regex.exec(text)) !== null) {
      // Add text before citation
      if (match.index > lastIndex) {
        result.push(text.substring(lastIndex, match.index));
      }

      const docId = match[1];
      const page = parseInt(match[2]);
      const anchor = match[3];

      result.push(
        <Citation
          key={`${match.index}-${docId}-${page}`}
          docId={docId}
          page={page}
          anchor={anchor}
          docMap={docMap}
        />
      );

      lastIndex = regex.lastIndex;
    }

    // Add remaining text
    if (lastIndex < text.length) {
      result.push(text.substring(lastIndex));
    }

    return result.length > 0 ? result : text;
  };

  // Custom markdown components that process citations in children
  const MarkdownComponents = {
    p: ({ children }) => (
      <p className="mb-3 leading-relaxed text-gray-700">
        {React.Children.map(children, processTextWithCitations)}
      </p>
    ),
    li: ({ children }) => (
      <li className="mb-1.5 text-gray-700">
        {React.Children.map(children, processTextWithCitations)}
      </li>
    ),
    h1: ({ children }) => (
      <h1 className="text-xl font-bold text-gray-900 mb-4 pb-2 border-b border-gray-200">
        {children}
      </h1>
    ),
    h2: ({ children }) => (
      <h2 className="text-lg font-bold text-gray-800 mt-6 mb-3">
        {children}
      </h2>
    ),
    h3: ({ children }) => (
      <h3 className="text-base font-semibold text-gray-800 mt-4 mb-2">
        {children}
      </h3>
    ),
    strong: ({ children }) => (
      <strong className="font-semibold text-gray-900">{children}</strong>
    ),
    ul: ({ children }) => (
      <ul className="list-disc pl-5 my-3 space-y-1">{children}</ul>
    ),
    ol: ({ children }) => (
      <ol className="list-decimal pl-5 my-3 space-y-1">{children}</ol>
    ),
  };

  return (
    <div className="rule-content prose prose-sm max-w-none">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={MarkdownComponents}
      >
        {cleanContent}
      </ReactMarkdown>
    </div>
  );
};

// Clickable Citation component for cross-reference with PDF
const ClickableCitation = ({ docId, page, anchor, docMap, onCitationClick, isActive }) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const doc = docMap?.[docId];
  const filename = doc?.filename || `Doc ${docId}`;

  const shortName = filename
    .replace(/\.(pdf|PDF)$/, '')
    .replace(/_/g, ' ')
    .slice(0, 20) + (filename.length > 24 ? '…' : '');

  return (
    <span
      className="relative inline-flex items-center align-baseline"
      onMouseEnter={() => setShowTooltip(true)}
      onMouseLeave={() => setShowTooltip(false)}
    >
      <button
        onClick={(e) => {
          e.stopPropagation();
          onCitationClick?.(docId, page, anchor);
        }}
        className={`inline-flex items-center gap-0.5 px-1.5 py-0.5 mx-0.5 text-[10px] rounded cursor-pointer transition-colors font-medium border ${
          isActive
            ? 'bg-teal-100 text-teal-800 border-teal-300 ring-1 ring-teal-400'
            : 'bg-slate-100 hover:bg-teal-50 text-slate-600 hover:text-teal-700 border-slate-200 hover:border-teal-300'
        }`}
        title={`Click to view in PDF: ${filename} p.${page}`}
      >
        <BookOpen className="w-2.5 h-2.5" />
        <span>{shortName}</span>
        <span className="text-slate-400">p.{page}</span>
      </button>
      {showTooltip && (
        <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-xs rounded-lg shadow-lg z-50 max-w-xs">
          <div className="font-medium text-gray-100 mb-1">{filename}</div>
          <div className="text-gray-400 text-[10px] mb-1">Page {page} • ID: {docId}</div>
          {anchor && (
            <div className="text-gray-300 italic text-[10px] border-t border-gray-700 pt-1 mt-1">
              "{anchor.length > 80 ? anchor.slice(0, 80) + '…' : anchor}"
            </div>
          )}
          <div className="text-teal-400 text-[10px] mt-1">Click to view in PDF →</div>
          <span className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-gray-900"></span>
        </span>
      )}
    </span>
  );
};

// RuleContent with clickable citations for PDF cross-reference
const RuleContentWithClickableCitations = ({ content, docMap, onCitationClick, currentDocId }) => {
  if (!content) return null;

  let cleanContent = content;
  const traceabilityIndex = cleanContent.indexOf('## TRACEABILITY LOG');
  if (traceabilityIndex > -1) {
    cleanContent = cleanContent.substring(0, traceabilityIndex).trim();
  }
  const logIndex = cleanContent.indexOf('TRACEABILITY LOG');
  if (logIndex > -1) {
    cleanContent = cleanContent.substring(0, logIndex).trim();
  }

  const processTextWithCitations = (text) => {
    if (typeof text !== 'string') return text;

    const regex = /\[\[([a-f0-9]+):(\d+)\s*\|\s*"([^"]+)"\]\]/gi;
    const result = [];
    let lastIndex = 0;
    let match;

    while ((match = regex.exec(text)) !== null) {
      if (match.index > lastIndex) {
        result.push(text.substring(lastIndex, match.index));
      }

      const docId = match[1];
      const page = parseInt(match[2]);
      const anchor = match[3];

      result.push(
        <ClickableCitation
          key={`${match.index}-${docId}-${page}`}
          docId={docId}
          page={page}
          anchor={anchor}
          docMap={docMap}
          onCitationClick={onCitationClick}
          isActive={docId === currentDocId}
        />
      );

      lastIndex = regex.lastIndex;
    }

    if (lastIndex < text.length) {
      result.push(text.substring(lastIndex));
    }

    return result.length > 0 ? result : text;
  };

  const MarkdownComponents = {
    p: ({ children }) => (
      <p className="mb-3 leading-relaxed text-gray-700">
        {React.Children.map(children, processTextWithCitations)}
      </p>
    ),
    li: ({ children }) => (
      <li className="mb-1.5 text-gray-700">
        {React.Children.map(children, processTextWithCitations)}
      </li>
    ),
    h1: ({ children }) => (
      <h1 className="text-xl font-bold text-gray-900 mb-4 pb-2 border-b border-gray-200">
        {children}
      </h1>
    ),
    h2: ({ children }) => (
      <h2 className="text-lg font-bold text-gray-800 mt-6 mb-3">
        {children}
      </h2>
    ),
    h3: ({ children }) => (
      <h3 className="text-base font-semibold text-gray-800 mt-4 mb-2">
        {children}
      </h3>
    ),
    strong: ({ children }) => (
      <strong className="font-semibold text-gray-900">{children}</strong>
    ),
    ul: ({ children }) => (
      <ul className="list-disc pl-5 my-3 space-y-1">{children}</ul>
    ),
    ol: ({ children }) => (
      <ol className="list-decimal pl-5 my-3 space-y-1">{children}</ol>
    ),
  };

  return (
    <div className="rule-content prose prose-sm max-w-none">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={MarkdownComponents}
      >
        {cleanContent}
      </ReactMarkdown>
    </div>
  );
};

// Citations summary with expandable details
const CitationsSummary = ({ check, docMap }) => {
  const [expanded, setExpanded] = useState(null);

  if (!check) return null;

  const { verified = [], failed = [], wrong_page = [], wrong_doc = [], repaired = [], ambiguous = [] } = check;
  const total = check.total || verified.length + failed.length + wrong_page.length + wrong_doc.length + repaired.length;

  const categories = [
    { key: 'verified', items: verified, label: 'Verified', icon: '✓', color: 'green', bg: 'bg-green-50' },
    { key: 'repaired', items: repaired, label: 'Repaired', icon: '🔧', color: 'blue', bg: 'bg-blue-50' },
    { key: 'wrong_page', items: wrong_page, label: 'Wrong Page', icon: '⚠', color: 'amber', bg: 'bg-amber-50' },
    { key: 'wrong_doc', items: wrong_doc, label: 'Wrong Doc', icon: '📄', color: 'orange', bg: 'bg-orange-50' },
    { key: 'ambiguous', items: ambiguous, label: 'Ambiguous', icon: '❓', color: 'purple', bg: 'bg-purple-50' },
    { key: 'failed', items: failed, label: 'Failed', icon: '✗', color: 'red', bg: 'bg-red-50' },
  ].filter(c => c.items.length > 0);

  return (
    <div className="mt-6 border rounded-lg overflow-hidden">
      <div className="p-3 bg-gray-50 border-b flex items-center justify-between">
        <div className="flex items-center gap-2">
          <ShieldCheck className="w-4 h-4 text-gray-600" />
          <span className="font-medium text-sm">Citation Verification</span>
        </div>
        <div className="flex items-center gap-2">
          {check.status && (
            <span className={`px-2 py-0.5 rounded text-xs font-medium ${
              check.status === 'VERIFIED' ? 'bg-green-100 text-green-700' :
              check.status === 'VERIFIED_WITH_REPAIRS' ? 'bg-blue-100 text-blue-700' :
              check.status === 'NEEDS_REVIEW' ? 'bg-amber-100 text-amber-700' : 
              'bg-red-100 text-red-700'
            }`}>
              {check.status.replace(/_/g, ' ')}
            </span>
          )}
          <span className="text-xs text-gray-500">{total} citations</span>
        </div>
      </div>

      <div className="p-3">
        <div className="flex flex-wrap gap-2 mb-3">
          {categories.map(cat => (
            <button
              key={cat.key}
              onClick={() => setExpanded(expanded === cat.key ? null : cat.key)}
              className={`flex items-center gap-1.5 px-2 py-1 rounded text-xs font-medium transition-colors ${
                expanded === cat.key 
                  ? `${cat.bg} text-${cat.color}-700 ring-2 ring-${cat.color}-300`
                  : `${cat.bg} text-${cat.color}-600 hover:ring-1 hover:ring-${cat.color}-200`
              }`}
            >
              <span>{cat.icon}</span>
              <span>{cat.items.length}</span>
              <span>{cat.label}</span>
            </button>
          ))}
        </div>

        {expanded && (
          <div className="mt-3 border-t pt-3 max-h-64 overflow-y-auto">
            {categories.find(c => c.key === expanded)?.items.map((item, idx) => {
              const doc = docMap?.[item.doc_id];
              return (
                <div key={idx} className="flex items-start gap-2 py-1.5 text-xs border-b last:border-0">
                  <span className="font-mono text-blue-600 whitespace-nowrap">[{item.doc_id}:{item.page}]</span>
                  <span className="text-gray-600 flex-1 truncate" title={item.phrase}>
                    "{item.phrase?.substring(0, 60)}{item.phrase?.length > 60 ? '...' : ''}"
                  </span>
                  {item.suggested_page && (
                    <span className="text-amber-600 whitespace-nowrap">→ p.{item.suggested_page}</span>
                  )}
                  {item.suggested_doc_id && (
                    <span className="text-orange-600 whitespace-nowrap">→ [{item.suggested_doc_id}]</span>
                  )}
                  {item.repaired_phrase && (
                    <span className="text-blue-600 whitespace-nowrap" title={item.repaired_phrase}>repaired</span>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
};

const RuleViewer = ({ code, onClose }) => {
  const [loading, setLoading] = useState(true);
  const [ruleResponse, setRuleResponse] = useState(null);
  const [log, setLog] = useState(null);
  const [activeTab, setActiveTab] = useState('final');
  const [error, setError] = useState(null);

  // PDF viewer state
  const [currentDocId, setCurrentDocId] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [searchTerm, setSearchTerm] = useState('');

  useEffect(() => {
    if (!code) return;

    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        const ruleRes = await fetch(`${API_BASE}/codes/${encodeURIComponent(code)}/rule`);
        if (ruleRes.ok) {
          setRuleResponse(await ruleRes.json());
        }

        const logRes = await fetch(`${API_BASE}/codes/${encodeURIComponent(code)}/generation-log`);
        if (logRes.ok) {
          const logData = await logRes.json();
          setLog(logData);
          // Set initial document
          if (logData?.source_documents?.length > 0) {
            setCurrentDocId(logData.source_documents[0].doc_id);
            setCurrentPage(logData.source_documents[0].pages?.[0] || 1);
          }
        }
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [code]);

  if (!code) return null;

  // Handle new API response structure
  const isInherited = ruleResponse?.is_inherited;
  const matchedPattern = ruleResponse?.matched_pattern;
  const rule = ruleResponse?.rule || ruleResponse;

  // Build doc_id → document info map
  const docMap = {};
  if (log?.source_documents) {
    log.source_documents.forEach(doc => {
      docMap[doc.doc_id] = doc;
    });
  }

  const sourceDocuments = log?.source_documents || [];
  const currentDoc = docMap[currentDocId];
  const pdfUrl = currentDocId ? `http://localhost:8001/api/kb/documents/${currentDocId}/pdf` : null;

  // Handle citation click - jump to document and page with highlighting
  const handleCitationClick = (docId, page, anchor) => {
    setCurrentDocId(docId);
    setCurrentPage(page);
    setSearchTerm(anchor || '');
  };

  const tabs = [
    { id: 'final', label: 'Final Rule', icon: ShieldCheck },
    { id: 'pipeline', label: 'Pipeline Log', icon: BrainCircuit },
  ];

  const renderStepContent = (stepName, stepData) => {
    if (!stepData) return null;

    const config = STEP_CONFIG[stepName] || { title: stepName, icon: FileText, color: 'gray' };
    const Icon = config.icon;

    return (
      <div key={stepName} className="border rounded-lg overflow-hidden mb-4">
        <div className="p-3 bg-gray-50 border-b flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Icon className="w-4 h-4 text-gray-600" />
            <span className="font-medium">{config.title}</span>
          </div>
          <div className="flex items-center gap-3 text-xs text-gray-500">
            {stepData.duration_ms && <span>{(stepData.duration_ms / 1000).toFixed(1)}s</span>}
            {stepData.corrections_count > 0 && <span className="text-purple-600">{stepData.corrections_count} corrections</span>}
            {stepData.risks_count > 0 && <span className="text-red-600">{stepData.risks_count} risks</span>}
            {stepData.verdict && <span className="font-medium px-2 py-0.5 bg-gray-200 rounded">{stepData.verdict}</span>}
          </div>
        </div>

        {stepData.thinking && (
          <details className="border-b">
            <summary className="p-2 text-xs text-gray-500 cursor-pointer hover:bg-gray-50">
              💭 Thinking ({Math.round(stepData.thinking.length / 1000)}k chars)
            </summary>
            <div className="p-3 bg-amber-50/50 text-xs font-mono whitespace-pre-wrap max-h-48 overflow-y-auto">
              {stepData.thinking}
            </div>
          </details>
        )}

        <div className="p-4 max-h-96 overflow-y-auto">
          <RuleContentWithClickableCitations
            content={stepData.output}
            docMap={docMap}
            onCitationClick={handleCitationClick}
          />
        </div>

        {stepData.citations_check && (
          <CitationsSummary check={stepData.citations_check} docMap={docMap} />
        )}
      </div>
    );
  };

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl w-full max-w-[95vw] max-h-[95vh] overflow-hidden shadow-2xl flex flex-col">
        {/* Header */}
        <div className="p-4 border-b flex items-center justify-between bg-gradient-to-r from-gray-50 to-white shrink-0">
          <div>
            <h3 className="font-semibold text-lg flex items-center gap-3">
              <span className="font-mono bg-gray-100 px-2 py-0.5 rounded">{code}</span>
              {isInherited && matchedPattern && (
                <span className="text-xs px-2 py-1 bg-purple-100 text-purple-700 rounded-full flex items-center gap-1">
                  ← via {matchedPattern}
                </span>
              )}
              {rule?.validation_status === 'verified' && (
                <span className="text-xs px-2 py-1 bg-green-100 text-green-700 rounded-full flex items-center gap-1">
                  <CheckCircle className="w-3 h-3" />
                  Verified
                </span>
              )}
            </h3>
            <p className="text-sm text-gray-500 mt-1">
              {rule?.created_at && new Date(rule.created_at).toLocaleString()}
              {log?.total_duration_ms && (
                <span className="ml-2 px-1.5 py-0.5 bg-gray-100 rounded text-xs">
                  {(log.total_duration_ms / 1000).toFixed(1)}s
                </span>
              )}
              {isInherited && (
                <span className="ml-2 px-1.5 py-0.5 bg-purple-50 text-purple-600 rounded text-xs">
                  Inherited rule
                </span>
              )}
            </p>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-gray-100 rounded-lg transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Tabs */}
        <div className="flex border-b bg-gray-50/50 shrink-0">
          {tabs.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`flex items-center gap-2 px-5 py-3 text-sm font-medium border-b-2 transition-colors ${
                activeTab === tab.id
                  ? 'border-blue-500 text-blue-600 bg-white'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:bg-gray-100'
              }`}
            >
              <tab.icon className="w-4 h-4" />
              {tab.label}
            </button>
          ))}
        </div>

        {/* Content - Split view for Final Rule tab */}
        <div className="flex-1 flex overflow-hidden">
          {loading ? (
            <div className="flex-1 flex items-center justify-center">
              <Loader2 className="w-8 h-8 animate-spin text-blue-500" />
            </div>
          ) : error ? (
            <div className="flex-1 p-6">
              <div className="text-red-500 p-4 bg-red-50 rounded-lg">{error}</div>
            </div>
          ) : activeTab === 'final' ? (
            <>
              {/* Left: PDF Viewer */}
              <div className="w-1/2 border-r bg-slate-100 flex flex-col overflow-hidden">
                {/* Document selector */}
                {sourceDocuments.length > 0 && (
                  <div className="p-2 border-b bg-white flex items-center gap-2 shrink-0">
                    <FileText className="w-4 h-4 text-gray-400" />
                    <select
                      value={currentDocId || ''}
                      onChange={(e) => {
                        const newDocId = e.target.value;
                        setCurrentDocId(newDocId);
                        const doc = docMap[newDocId];
                        if (doc?.pages?.[0]) {
                          setCurrentPage(doc.pages[0]);
                        }
                        setSearchTerm('');
                      }}
                      className="flex-1 text-sm border rounded px-2 py-1 bg-white"
                    >
                      {sourceDocuments.map(doc => (
                        <option key={doc.doc_id} value={doc.doc_id}>
                          [{doc.doc_id}] {doc.filename} (p.{doc.pages?.join(', ')})
                        </option>
                      ))}
                    </select>
                    <span className="text-xs text-gray-500">
                      Page {currentPage}
                    </span>
                  </div>
                )}

                {/* PDF Viewer */}
                <div className="flex-1 overflow-hidden">
                  {pdfUrl ? (
                    <PdfViewer
                      url={pdfUrl}
                      pageNumber={currentPage}
                      searchText={searchTerm}
                    />
                  ) : (
                    <div className="flex items-center justify-center h-full text-gray-400">
                      <div className="text-center">
                        <FileText className="w-12 h-12 mx-auto mb-2 opacity-30" />
                        <p>No PDF available</p>
                      </div>
                    </div>
                  )}
                </div>
              </div>

              {/* Right: Rule Content */}
              <div className="w-1/2 overflow-y-auto p-6">
                <RuleContentWithClickableCitations
                  content={rule?.content}
                  docMap={docMap}
                  onCitationClick={handleCitationClick}
                  currentDocId={currentDocId}
                />

                <CitationsSummary check={rule?.citations_summary} docMap={docMap} />

                {sourceDocuments.length > 0 && (
                  <div className="mt-6 p-4 bg-gray-50 rounded-lg">
                    <div className="font-medium text-sm mb-3 flex items-center gap-2">
                      <FileText className="w-4 h-4" />
                      Source Documents ({sourceDocuments.length})
                    </div>
                    <div className="grid grid-cols-1 gap-2">
                      {sourceDocuments.map(doc => (
                        <button
                          key={doc.doc_id}
                          onClick={() => {
                            setCurrentDocId(doc.doc_id);
                            setCurrentPage(doc.pages?.[0] || 1);
                            setSearchTerm('');
                          }}
                          className={`flex items-center gap-2 p-2 rounded border text-sm text-left transition-colors ${
                            currentDocId === doc.doc_id
                              ? 'bg-blue-50 border-blue-300 ring-2 ring-blue-200'
                              : doc.via_pattern
                                ? 'bg-purple-50 border-purple-200 hover:bg-purple-100'
                                : 'bg-white hover:bg-gray-50'
                          }`}
                        >
                          <span className="font-mono text-xs px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded">{doc.doc_id}</span>
                          <span className="text-gray-700 truncate flex-1" title={doc.filename}>{doc.filename}</span>
                          {doc.via_pattern && (
                            <span className="text-xs px-1 py-0.5 bg-purple-100 text-purple-600 rounded">
                              {doc.via_pattern}
                            </span>
                          )}
                          <span className="text-gray-400 text-xs whitespace-nowrap">p.{doc.pages?.join(', ')}</span>
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </>
          ) : (
            <div className="flex-1 overflow-y-auto p-6">
              <div className="space-y-4">
                {log?.pipeline ? (
                  Object.entries(log.pipeline).map(([step, data]) => renderStepContent(step, data))
                ) : (
                  <div className="text-gray-500 text-center py-8">No pipeline log available</div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

// =============================================================================
// CODE LIST
// =============================================================================

const CodeList = ({
  categoryName,
  diagnoses = [],
  procedures = [],
  loading,
  selectedCodes,
  onToggleCode,
  onSelectAll,
  onGenerate,
  onGenerateSingle,
  onDeleteRule,
  onViewRule,
  generating
}) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [expandedSections, setExpandedSections] = useState({
    generated: true,
    ready: true
  });
  const [expandedSubSections, setExpandedSubSections] = useState({});

  const toggleSection = (section) => {
    setExpandedSections(prev => ({ ...prev, [section]: !prev[section] }));
  };

  const toggleSubSection = (sectionId, subLabel) => {
    const key = `${sectionId}-${subLabel}`;
    setExpandedSubSections(prev => ({ ...prev, [key]: !prev[key] }));
  };

  const filterCodes = (codes) => codes.filter(c => {
    const matchesSearch = c.code.toLowerCase().includes(searchQuery.toLowerCase());
    return matchesSearch;
  });

  const filteredDiagnoses = filterCodes(diagnoses);
  const filteredProcedures = filterCodes(procedures);

  // Split by rule status
  const diagnosesWithRules = filteredDiagnoses.filter(c => c.rule_status?.has_rule);
  const diagnosesWithoutRules = filteredDiagnoses.filter(c => !c.rule_status?.has_rule);
  const proceduresWithRules = filteredProcedures.filter(c => c.rule_status?.has_rule);
  const proceduresWithoutRules = filteredProcedures.filter(c => !c.rule_status?.has_rule);

  const generatedCodes = [...diagnosesWithRules, ...proceduresWithRules];
  const readyToGenerate = [...diagnosesWithoutRules, ...proceduresWithoutRules];
  const allFiltered = [...filteredDiagnoses, ...filteredProcedures];

  // All codes with docs can be selected (for generation or regeneration)
  const selectableCodes = allFiltered.filter(c => (c.documents?.length || 0) + (c.inherited_documents?.length || 0) > 0);
  const allSelected = selectableCodes.length > 0 &&
    selectableCodes.every(c => selectedCodes.has(c.code));

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-6 h-6 animate-spin text-teal-600" />
      </div>
    );
  }

  // Section component (matching ClaimRules)
  const Section = ({ id, icon, title, count, color, children }) => {
    const isExpanded = expandedSections[id];
    if (count === 0) return null;

    return (
      <div className="border rounded-lg overflow-hidden" style={{ borderColor: '#e5e7eb' }}>
        <button
          onClick={() => toggleSection(id)}
          className="w-full flex items-center gap-3 px-4 py-3 transition-colors hover:bg-gray-50"
          style={{ background: 'white' }}
        >
          <span style={{ color }}>{icon}</span>
          <span className="font-medium text-sm" style={{ color: '#1a1a1a' }}>{title}</span>
          <span
            className="text-xs font-semibold px-2 py-0.5 rounded-full"
            style={{ background: color, color: 'white' }}
          >
            {count}
          </span>
          <div className="flex-1" />
          <ChevronRight
            className="w-4 h-4 transition-transform"
            style={{ color: '#9ca3af', transform: isExpanded ? 'rotate(90deg)' : 'rotate(0deg)' }}
          />
        </button>
        {isExpanded && <div className="border-t" style={{ borderColor: '#e5e7eb' }}>{children}</div>}
      </div>
    );
  };

  // SubSection for diagnoses/procedures (collapsed by default)
  const SubSection = ({ sectionId, label, codes }) => {
    if (codes.length === 0) return null;
    const key = `${sectionId}-${label}`;
    const isExpanded = expandedSubSections[key] ?? false; // collapsed by default

    return (
      <div className="px-3 py-2">
        <button
          onClick={() => toggleSubSection(sectionId, label)}
          className="w-full flex items-center gap-2 text-xs font-medium uppercase tracking-wider mb-2 hover:text-gray-600 transition-colors"
          style={{ color: '#9ca3af' }}
        >
          <ChevronRight
            className="w-3 h-3 transition-transform"
            style={{ transform: isExpanded ? 'rotate(90deg)' : 'rotate(0deg)' }}
          />
          {label} ({codes.length})
        </button>
        {isExpanded && (
          <div className="space-y-1.5">
            {codes.map(code => (
              <CodeItem
                key={code.code}
                code={code}
                isSelected={selectedCodes.has(code.code)}
                canSelect={true}
                generating={generating}
                onToggleCode={onToggleCode}
                onDeleteRule={onDeleteRule}
                onViewRule={onViewRule}
                onGenerate={onGenerateSingle}
              />
            ))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="p-4 border-b" style={{ background: '#f9fafb' }}>
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <h3 className="font-semibold" style={{ color: '#1a1a1a' }}>{categoryName}</h3>
            <span className="text-sm" style={{ color: '#6b7280' }}>
              {diagnoses.length} dx · {procedures.length} proc
            </span>
          </div>
          <div className="flex items-center gap-2">
            {selectedCodes.size > 0 && (
              <button
                onClick={onGenerate}
                disabled={generating}
                className="px-4 py-2 text-sm rounded-lg flex items-center gap-1.5 font-medium transition-colors"
                style={{
                  color: generating ? '#9ca3af' : '#0090DA',
                  background: generating ? '#f9fafb' : 'transparent',
                  border: generating ? '1px solid #d1d5db' : '2px solid #0090DA'
                }}
                onMouseEnter={(e) => !generating && (e.currentTarget.style.background = '#eff6ff')}
                onMouseLeave={(e) => e.currentTarget.style.background = generating ? '#f9fafb' : 'transparent'}
              >
                {generating ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Zap className="w-4 h-4" />
                )}
                Generate ({selectedCodes.size})
              </button>
            )}
          </div>
        </div>

        <div className="flex items-center gap-2">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search codes..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-9 pr-3 py-2 border rounded-lg text-sm"
              style={{ borderColor: '#e5e7eb' }}
            />
          </div>

          {selectableCodes.length > 0 && (
            <button
              onClick={() => onSelectAll(selectableCodes.map(c => c.code))}
              className="px-3 py-2 text-sm rounded-lg whitespace-nowrap"
              style={{ color: '#0090DA', background: 'transparent' }}
            >
              {allSelected ? 'Deselect all' : 'Select all'} ({selectableCodes.length})
            </button>
          )}
        </div>
      </div>

      {/* Code sections */}
      <div className="flex-1 overflow-auto p-3 space-y-3">
        {allFiltered.length === 0 ? (
          <div className="text-center py-12" style={{ color: '#9ca3af' }}>
            No codes found
          </div>
        ) : (
          <>
            {/* Generated Rules */}
            <Section
              id="generated"
              icon="✓"
              title="Generated Rules"
              count={generatedCodes.length}
              color="#059669"
            >
              <SubSection sectionId="generated" label="Diagnoses" codes={diagnosesWithRules} />
              <SubSection sectionId="generated" label="Procedures" codes={proceduresWithRules} />
            </Section>

            {/* Ready to Generate */}
            <Section
              id="ready"
              icon="⚡"
              title="Ready to Generate"
              count={readyToGenerate.length}
              color="#0090DA"
            >
              <SubSection sectionId="ready" label="Diagnoses" codes={diagnosesWithoutRules} />
              <SubSection sectionId="ready" label="Procedures" codes={proceduresWithoutRules} />
            </Section>
          </>
        )}
      </div>
    </div>
  );
};

// =============================================================================
// SSE PARSER - handles new streaming format
// =============================================================================

function parseSSELine(line) {
  if (!line.startsWith('data: ')) return null;
  try {
    return JSON.parse(line.slice(6));
  } catch (e) {
    console.error('SSE parse error:', e, line);
    return null;
  }
}

// =============================================================================
// MAIN COMPONENT
// =============================================================================

export default function RuleGeneration() {
  const { data: categoriesByType, loading, refetch } = useCategoriesByType();

  // Selection state: code_type + meta_category
  const [selectedCodeType, setSelectedCodeType] = useState(null);
  const [selectedMetaCategory, setSelectedMetaCategory] = useState(null);
  const [viewFilter, setViewFilter] = useState('actionable');

  // Fetch codes for selected category
  const { diagnoses, procedures, loading: codesLoading, refetch: refetchCodes } = useCategoryCode(selectedMetaCategory, selectedCodeType);

  const [selectedCodes, setSelectedCodes] = useState(new Set());
  const [generating, setGenerating] = useState(false);
  const [showProgress, setShowProgress] = useState(false);
  const [jsonValidators, setJsonValidators] = useState(true); // Faster JSON mode for validators

  // Auto-select first category when data loads
  useEffect(() => {
    if (!selectedMetaCategory && Object.keys(categoriesByType).length > 0) {
      for (const codeType of ['ICD-10', 'CPT', 'HCPCS']) {
        const typeData = categoriesByType[codeType];
        if (typeData?.categories?.length > 0) {
          setSelectedCodeType(codeType);
          setSelectedMetaCategory(typeData.categories[0].name);
          break;
        }
      }
    }
  }, [categoriesByType, selectedMetaCategory]);

  // Handle category selection
  const handleSelectCategory = (codeType, metaCategory) => {
    setSelectedCodeType(codeType);
    setSelectedMetaCategory(metaCategory);
    setSelectedCodes(new Set());
  };
  const [generationProgress, setGenerationProgress] = useState({});
  const [viewingCode, setViewingCode] = useState(null);

  const handleViewRule = (code) => {
    setViewingCode(code);
  };

  const handleToggleCode = (code) => {
    setSelectedCodes(prev => {
      const next = new Set(prev);
      if (next.has(code)) {
        next.delete(code);
      } else {
        next.add(code);
      }
      return next;
    });
  };

  const handleSelectAll = (codesToSelect) => {
    setSelectedCodes(prev => {
      const allSelected = codesToSelect.every(c => prev.has(c));
      if (allSelected) {
        const next = new Set(prev);
        codesToSelect.forEach(c => next.delete(c));
        return next;
      } else {
        return new Set([...prev, ...codesToSelect]);
      }
    });
  };

  const handleDeleteRule = async (code) => {
    try {
      const res = await fetch(`${API_BASE}/codes/${encodeURIComponent(code)}/rule`, {
        method: 'DELETE'
      });
      if (res.ok) {
        refetch();
        if (refetchCodes) refetchCodes();
      } else {
        alert('Failed to delete rule');
      }
    } catch (err) {
      console.error('Failed to delete rule:', err);
      alert('Failed to delete rule');
    }
  };

  // Generate a single rule (used by parallel processor)
  const generateSingleRule = async (code, forceRegenerate = false) => {
    console.log(`[Guideline] Starting generation for: ${code}`, forceRegenerate ? '(force)' : '');

    // Mark as generating (preserve any existing data)
    setGenerationProgress(prev => ({
      ...prev,
      [code]: {
        ...(prev[code] || {}),
        status: 'generating',
        steps: {
          draft: { status: 'idle' }
        }
      }
    }));

    try {
      const response = await fetch(`${API_BASE}/generate/${encodeURIComponent(code)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          code,
          code_type: 'ICD-10',
          parallel_validators: true,
          force_regenerate: forceRegenerate,
          json_validators: jsonValidators
        })
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed || !trimmed.startsWith('data: ')) continue;

          const data = parseSSELine(trimmed);
          if (!data) continue;

          const { step, type } = data;

          if (step === 'done') {
            setGenerationProgress(prev => ({
              ...prev,
              [code]: { ...prev[code], status: 'complete' }
            }));
            continue;
          }

          if (step === 'error') {
            setGenerationProgress(prev => ({
              ...prev,
              [code]: { ...prev[code], status: 'error', error: data.message || 'Unknown error' }
            }));
            continue;
          }

          setGenerationProgress(prev => {
            const prevCode = prev[code] || { status: 'generating', steps: {}, cascade: null };
            const prevStep = prevCode.steps[step] || { status: 'idle', thinking: '', content: '' };
            let updatedStep = { ...prevStep };
            let updatedCascade = prevCode.cascade;

            switch (type) {
              // Cascade events (from HierarchyRuleGenerator)
              case 'plan':
                updatedCascade = {
                  patterns_to_generate: data.patterns_to_generate || [],
                  existing_patterns: data.existing_patterns || [],
                  current_index: 0,
                  current_pattern: null
                };
                updatedStep.status = 'planning';
                updatedStep.message = data.content;
                break;
              case 'generating':
                if (updatedCascade) {
                  const idx = updatedCascade.patterns_to_generate.indexOf(data.pattern);
                  updatedCascade = {
                    ...updatedCascade,
                    current_index: idx >= 0 ? idx : updatedCascade.current_index,
                    current_pattern: data.pattern,
                    parent_rule: data.parent_rule
                  };
                }
                // Reset all step outputs for new code in cascade
                prevCode.steps = {
                  draft: { status: 'idle', thinking: '', content: '' },
                  mentor: { status: 'idle', thinking: '', content: '' },
                  redteam: { status: 'idle', thinking: '', content: '' },
                  arbitration: { status: 'idle', thinking: '', content: '' },
                  finalization: { status: 'idle', thinking: '', content: '' }
                };
                updatedStep.status = 'streaming';
                updatedStep.message = data.content;
                break;
              case 'exists':
              case 'skip':
                updatedStep.status = 'done';
                updatedStep.message = data.content;
                break;
              case 'complete':
                updatedStep.status = 'done';
                updatedStep.message = data.content;
                if (updatedCascade) {
                  updatedCascade.current_index = updatedCascade.patterns_to_generate.length;
                }
                break;
              // Standard pipeline events
              case 'status':
                updatedStep.status = data.status || 'streaming';
                if (data.message) updatedStep.message = data.message;
                break;
              case 'thought':
              case 'thinking':
                updatedStep.status = 'streaming';
                updatedStep.thinking = (updatedStep.thinking || '') + (data.thinking || data.content || '');
                break;
              case 'content':
              case 'text':
                updatedStep.status = 'streaming';
                updatedStep.content = (updatedStep.content || '') + (data.content || data.text || '');
                break;
              case 'done':
                updatedStep.status = 'done';
                if (data.full_text) updatedStep.content = data.full_text;
                if (data.full_thinking) updatedStep.thinking = data.full_thinking;
                if (data.duration_ms) updatedStep.duration_ms = data.duration_ms;
                if (data.corrections_count !== undefined) updatedStep.corrections_count = data.corrections_count;
                if (data.verdict) updatedStep.verdict = data.verdict;
                if (data.citations_check) updatedStep.citations_check = data.citations_check;
                break;
              case 'error':
                updatedStep.status = 'error';
                updatedStep.error = data.message || data.content;
                break;
            }

            return {
              ...prev,
              [code]: { ...prevCode, cascade: updatedCascade, steps: { ...prevCode.steps, [step]: updatedStep } }
            };
          });
        }
      }

      console.log(`[Guideline] Completed generation for: ${code}`);
      setGenerationProgress(prev => {
        const currentStatus = prev[code]?.status;
        if (currentStatus === 'generating' || currentStatus === 'pending') {
          return { ...prev, [code]: { ...prev[code], status: 'complete' } };
        }
        return prev;
      });

    } catch (err) {
      console.error(`[Guideline] Failed to generate rule for ${code}:`, err);
      setGenerationProgress(prev => ({
        ...prev,
        [code]: { ...(prev[code] || {}), status: 'error', error: err.message }
      }));
    }
  };

  // Generate rules for given codes (batch or single) - parallel with concurrency limit
  const generateRules = async (codes, concurrency = 5, forceRegenerate = false) => {
    if (codes.length === 0) return;

    setGenerating(true);
    setShowProgress(true);

    // Initialize progress with new structure
    const initialProgress = {};
    codes.forEach(code => {
      initialProgress[code] = {
        status: 'pending',
        steps: {},
        error: null
      };
    });
    setGenerationProgress(initialProgress);

    // Wait a tick for React to apply initial state
    await new Promise(resolve => setTimeout(resolve, 0));

    // Process codes in parallel with concurrency limit using index-based approach
    let currentIndex = 0;

    const processNext = async () => {
      while (currentIndex < codes.length) {
        const index = currentIndex++;
        const code = codes[index];
        if (code) {
          await generateSingleRule(code, forceRegenerate);
        }
      }
    };

    // Start up to 'concurrency' parallel workers
    const workers = [];
    for (let i = 0; i < Math.min(concurrency, codes.length); i++) {
      workers.push(processNext());
    }

    // Wait for all workers to complete
    await Promise.all(workers);

    setGenerating(false);
    setSelectedCodes(new Set());
    refetch();
    if (refetchCodes) refetchCodes();
  };

  // Single code generation (from individual Generate button)
  const handleGenerateSingle = (code, forceRegenerate = false) => {
    generateRules([code], 5, forceRegenerate);
  };

  // Batch generation (from header Generate button)
  const handleGenerate = async () => {
    if (selectedCodes.size === 0) return;
    generateRules(Array.from(selectedCodes));
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="p-4 border-b bg-white">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="font-semibold text-lg flex items-center gap-2">
              <BookOpen className="w-5 h-5 text-blue-600" />
              Guideline Rules
            </h2>
            <p className="text-sm text-gray-500">
              ICD-10/CPT/HCPCS validation rules from clinical guidelines
            </p>
          </div>
          <div className="flex items-center gap-3">
            {/* View filters */}
            <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-1">
              {[
                { key: 'actionable', label: 'With Docs' },
                { key: 'generated', label: 'Generated' },
                { key: 'all', label: 'All' },
              ].map(({ key, label }) => (
                <button
                  key={key}
                  onClick={() => setViewFilter(key)}
                  className="px-3 py-1.5 text-xs font-medium rounded-md transition-colors"
                  style={{
                    background: viewFilter === key ? 'white' : 'transparent',
                    color: viewFilter === key ? '#2563eb' : '#6b7280',
                    boxShadow: viewFilter === key ? '0 1px 2px rgba(0,0,0,0.05)' : 'none',
                  }}
                >
                  {label}
                </button>
              ))}
            </div>
            <label className="flex items-center gap-1.5 text-xs text-gray-500 cursor-pointer" title="Use JSON format for validator outputs (faster)">
              <input
                type="checkbox"
                checked={jsonValidators}
                onChange={(e) => setJsonValidators(e.target.checked)}
                className="w-3.5 h-3.5 rounded"
              />
              Fast mode
            </label>
            <button
              onClick={() => { refetch(); if (selectedMetaCategory) refetchCodes(); }}
              className="p-2 hover:bg-gray-100 rounded-lg"
              title="Refresh data"
            >
              <RefreshCw className="w-4 h-4" />
            </button>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Categories sidebar */}
        <div className="w-80 border-r bg-gray-50 p-4 overflow-auto">
          <h3 className="text-xs font-semibold text-gray-500 uppercase mb-3">Code Hierarchy</h3>
          <HierarchicalCategoryList
            categoriesByType={categoriesByType}
            selectedCodeType={selectedCodeType}
            selectedMetaCategory={selectedMetaCategory}
            onSelectCategory={handleSelectCategory}
            loading={loading}
            viewFilter={viewFilter}
          />
        </div>

        {/* Codes panel */}
        <div className="flex-1 overflow-hidden">
          {selectedMetaCategory ? (
            <CodeList
              categoryName={`${selectedMetaCategory} (${selectedCodeType})`}
              diagnoses={diagnoses}
              procedures={procedures}
              loading={codesLoading}
              selectedCodes={selectedCodes}
              onToggleCode={handleToggleCode}
              onSelectAll={handleSelectAll}
              onGenerate={handleGenerate}
              onGenerateSingle={handleGenerateSingle}
              onDeleteRule={handleDeleteRule}
              onViewRule={handleViewRule}
              generating={generating}
            />
          ) : (
            <div className="h-full flex items-center justify-center text-gray-400">
              <div className="text-center">
                <BookOpen className="w-16 h-16 mx-auto mb-4 opacity-30" />
                <p className="text-lg font-medium">Select a category</p>
                <p className="text-sm">Choose a category to generate guideline rules</p>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Generation Progress Modal */}
      <GenerationProgress
        isOpen={showProgress}
        codes={Array.from(selectedCodes.size > 0 ? selectedCodes : Object.keys(generationProgress))}
        progress={generationProgress}
        onClose={() => !generating && setShowProgress(false)}
        generating={generating}
      />

      {/* Rule Viewer Modal */}
      <RuleViewer
        code={viewingCode}
        onClose={() => setViewingCode(null)}
      />
    </div>
  );
}