import React, { useState, useEffect, useCallback } from 'react';
import {
  ChevronRight, ChevronDown, CheckCircle, Clock, AlertCircle,
  Loader2, Hash, FileText, Search, RefreshCw, Zap, X, Trash2, Eye,
  BookOpen, BrainCircuit, ShieldCheck, Gavel, Users, Swords
} from 'lucide-react';
import GenerationProgress from './GenerationProgress';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

const API_BASE = 'http://localhost:8000/api/rules';

// =============================================================================
// HOOKS
// =============================================================================

function useCategories() {
  const [data, setData] = useState({ categories: [], total_codes: 0, total_with_rules: 0 });
  const [loading, setLoading] = useState(true);

  const fetch_ = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/categories`);
      const json = await res.json();
      setData(json);
    } catch (err) {
      console.error('Failed to fetch categories:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetch_(); }, [fetch_]);

  return { ...data, loading, refetch: fetch_ };
}

function useCategoryCode(categoryName) {
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
    if (!categoryName) return;

    setLoading(true);
    fetch(`${API_BASE}/categories/${encodeURIComponent(categoryName)}/codes`)
      .then(res => res.json())
      .then(json => setData(json))
      .catch(err => console.error('Failed to fetch codes:', err))
      .finally(() => setLoading(false));
  }, [categoryName]);

  useEffect(() => {
    fetchCodes();
  }, [fetchCodes]);

  return { ...data, loading, refetch: fetchCodes };
}

// =============================================================================
// CATEGORY LIST
// =============================================================================

const CategoryList = ({ categories, selectedCategory, onSelectCategory, loading }) => {
  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
      </div>
    );
  }

  if (categories.length === 0) {
    return (
      <div className="text-center py-8 text-gray-400">
        <p>No categories with codes</p>
        <p className="text-sm mt-1">Parse documents first</p>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {categories.map(cat => {
        const isSelected = selectedCategory === cat.name;
        const coveragePercent = cat.coverage_percent || 0;

        return (
          <button
            key={cat.name}
            onClick={() => onSelectCategory(cat.name)}
            className={`w-full text-left p-3 rounded-lg border transition-all ${
              isSelected 
                ? 'bg-blue-50 border-blue-300 ring-2 ring-blue-200' 
                : 'bg-white hover:bg-gray-50 border-gray-200'
            }`}
          >
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <div
                  className="w-3 h-3 rounded-full"
                  style={{ backgroundColor: cat.color || '#6B7280' }}
                />
                <span className="font-medium text-sm">{cat.name}</span>
              </div>
              <ChevronRight className={`w-4 h-4 text-gray-400 transition-transform ${isSelected ? 'rotate-90' : ''}`} />
            </div>

            <div className="mt-2 flex items-center gap-3">
              <span className="text-xs text-gray-500">
                {cat.total_codes} codes
              </span>
              <div className="flex-1 h-1.5 bg-gray-200 rounded-full overflow-hidden">
                <div
                  className="h-full transition-all"
                  style={{
                    width: `${coveragePercent}%`,
                    backgroundColor: coveragePercent === 100 ? '#10B981' : coveragePercent > 50 ? '#F59E0B' : '#6B7280'
                  }}
                />
              </div>
              <span className="text-xs font-mono text-gray-500">
                {cat.codes_with_rules}/{cat.total_codes}
              </span>
            </div>
          </button>
        );
      })}
    </div>
  );
};

// =============================================================================
// CODE ITEM
// =============================================================================

const CodeItem = ({ code, isSelected, canSelect, generating, onToggleCode, onDeleteRule, onViewRule }) => {
  const hasRule = code.rule_status?.has_rule;
  const isMock = code.rule_status?.is_mock;
  const isInheritedRule = code.rule_status?.is_inherited;
  const matchedPattern = code.rule_status?.matched_pattern;
  const [deleting, setDeleting] = useState(false);

  // Code type flags
  const isWildcard = code.is_wildcard || code.code?.includes('%');
  const isRange = code.is_range || code.code?.includes(':');
  const isPattern = isWildcard || isRange;

  // Document counts
  const ownDocs = code.documents?.length || 0;
  const inheritedDocs = code.inherited_documents?.length || 0;
  const totalDocs = code.total_docs || ownDocs + inheritedDocs;

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

  return (
    <div
      className={`p-3 rounded-lg border transition-all ${
        isSelected 
          ? 'bg-blue-50 border-blue-300' 
          : isPattern
            ? 'bg-purple-50 border-purple-200 hover:border-purple-300'
            : hasRule && !isMock
              ? 'bg-green-50 border-green-200 hover:border-green-300 cursor-pointer' 
              : hasRule && isMock
                ? 'bg-yellow-50 border-yellow-200'
                : 'bg-white border-gray-200 hover:border-gray-300'
      }`}
      onClick={hasRule && !isMock ? handleView : undefined}
    >
      <div className="flex items-center gap-3">
        {canSelect && (
          <input
            type="checkbox"
            checked={isSelected}
            onChange={() => onToggleCode(code.code)}
            disabled={generating}
            className="w-4 h-4 text-blue-600 rounded"
            onClick={(e) => e.stopPropagation()}
          />
        )}

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-mono font-medium text-sm">{code.code}</span>
            <span className="text-xs px-1.5 py-0.5 bg-gray-100 rounded text-gray-500">
              {code.type || 'ICD-10'}
            </span>
            {isWildcard && (
              <span className="text-xs px-1.5 py-0.5 bg-purple-100 rounded text-purple-600" title="Wildcard pattern - applies to all matching codes">
                wildcard
              </span>
            )}
            {isRange && (
              <span className="text-xs px-1.5 py-0.5 bg-indigo-100 rounded text-indigo-600" title="Range pattern - applies to codes in range">
                range
              </span>
            )}
          </div>

          <div className="mt-1 flex items-center gap-3 text-xs text-gray-500">
            <span className="flex items-center gap-1" title={`${ownDocs} own docs`}>
              <FileText className="w-3 h-3" />
              {ownDocs} docs
            </span>
            {inheritedDocs > 0 && (
              <span className="flex items-center gap-1 text-purple-500" title={`${inheritedDocs} inherited from parent patterns`}>
                +{inheritedDocs} inherited
              </span>
            )}
            <span className="text-gray-400">= {totalDocs} total</span>
          </div>
        </div>

        <div className="flex items-center gap-2">
          {hasRule ? (
            isMock ? (
              <div className="flex items-center gap-1 text-yellow-600">
                <Clock className="w-4 h-4" />
                <span className="text-xs">mock v{code.rule_status.version}</span>
              </div>
            ) : (
              <div className="flex items-center gap-2">
                <button
                  onClick={handleView}
                  className="p-1.5 text-green-600 hover:bg-green-100 rounded transition-colors"
                  title="View rule"
                >
                  <Eye className="w-4 h-4" />
                </button>
                <div className="flex flex-col items-end">
                  <div className="flex items-center gap-1 text-green-600">
                    <CheckCircle className="w-4 h-4" />
                    <span className="text-xs">v{code.rule_status.version}</span>
                  </div>
                  {isInheritedRule && matchedPattern && (
                    <span className="text-xs text-purple-500" title={`Rule inherited from ${matchedPattern}`}>
                      via {matchedPattern}
                    </span>
                  )}
                </div>
                <button
                  onClick={handleDelete}
                  disabled={deleting}
                  className="p-1 text-gray-400 hover:text-red-500 hover:bg-red-50 rounded transition-colors"
                  title="Delete rule"
                >
                  {deleting ? (
                    <Loader2 className="w-3 h-3 animate-spin" />
                  ) : (
                    <X className="w-3 h-3" />
                  )}
                </button>
              </div>
            )
          ) : (
            <div className="flex items-center gap-1 text-gray-400">
              <Clock className="w-4 h-4" />
              <span className="text-xs">No rule</span>
            </div>
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

// Citation component with tooltip
const Citation = ({ docId, page, anchor, docMap }) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const doc = docMap?.[docId];
  const filename = doc?.filename || `Document ${docId}`;

  return (
    <span
      className="relative inline-flex items-center"
      onMouseEnter={() => setShowTooltip(true)}
      onMouseLeave={() => setShowTooltip(false)}
    >
      <span className="inline-flex items-center gap-1 px-1.5 py-0.5 mx-0.5 bg-blue-50 hover:bg-blue-100 text-blue-700 text-xs rounded border border-blue-200 cursor-help transition-colors">
        <FileText className="w-3 h-3" />
        <span className="font-medium">{anchor}</span>
      </span>
      {showTooltip && (
        <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-xs rounded-lg shadow-lg whitespace-nowrap z-50">
          <div className="font-medium">{filename}</div>
          <div className="text-gray-300">Page {page} • [{docId}]</div>
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
  // Also try other variations
  const logIndex = cleanContent.indexOf('TRACEABILITY LOG');
  if (logIndex > -1) {
    cleanContent = cleanContent.substring(0, logIndex).trim();
  }

  // Parse citations: [[doc_id:page | "anchor"]]
  const citationRegex = /\[\[([a-f0-9]+):(\d+)\s*\|\s*"([^"]+)"\]\]/gi;

  // Split content by citations
  const parts = [];
  let lastIndex = 0;
  let match;

  while ((match = citationRegex.exec(cleanContent)) !== null) {
    // Add text before citation
    if (match.index > lastIndex) {
      parts.push({
        type: 'text',
        content: cleanContent.substring(lastIndex, match.index)
      });
    }

    // Add citation
    parts.push({
      type: 'citation',
      docId: match[1],
      page: parseInt(match[2]),
      anchor: match[3]
    });

    lastIndex = match.index + match[0].length;
  }

  // Add remaining text
  if (lastIndex < cleanContent.length) {
    parts.push({
      type: 'text',
      content: cleanContent.substring(lastIndex)
    });
  }

  // Render parts
  return (
    <div className="prose prose-sm max-w-none prose-headings:text-gray-800 prose-h1:text-xl prose-h2:text-lg prose-h3:text-base prose-li:my-0.5">
      {parts.map((part, idx) => {
        if (part.type === 'citation') {
          return (
            <Citation
              key={idx}
              docId={part.docId}
              page={part.page}
              anchor={part.anchor}
              docMap={docMap}
            />
          );
        }
        // Render text as markdown
        return (
          <ReactMarkdown key={idx} remarkPlugins={[remarkGfm]}>
            {part.content}
          </ReactMarkdown>
        );
      })}
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
          setLog(await logRes.json());
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
  const rule = ruleResponse?.rule || ruleResponse; // Backward compatibility

  // Build doc_id → filename map
  const docMap = {};
  if (log?.source_documents) {
    log.source_documents.forEach(doc => {
      docMap[doc.doc_id] = doc;
    });
  }

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
          <RuleContent content={stepData.output} docMap={docMap} />
        </div>

        {stepData.citations_check && (
          <CitationsSummary check={stepData.citations_check} docMap={docMap} />
        )}
      </div>
    );
  };

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl w-full max-w-5xl max-h-[90vh] overflow-hidden shadow-2xl flex flex-col">
        {/* Header */}
        <div className="p-4 border-b flex items-center justify-between bg-gradient-to-r from-gray-50 to-white">
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
        <div className="flex border-b bg-gray-50/50">
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

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {loading ? (
            <div className="flex items-center justify-center h-64">
              <Loader2 className="w-8 h-8 animate-spin text-blue-500" />
            </div>
          ) : error ? (
            <div className="text-red-500 p-4 bg-red-50 rounded-lg">{error}</div>
          ) : activeTab === 'final' ? (
            <div>
              <RuleContent content={rule?.content} docMap={docMap} />

              <CitationsSummary check={rule?.citations_summary} docMap={docMap} />

              {log?.source_documents?.length > 0 && (
                <div className="mt-6 p-4 bg-gray-50 rounded-lg">
                  <div className="font-medium text-sm mb-3 flex items-center gap-2">
                    <FileText className="w-4 h-4" />
                    Source Documents ({log.source_documents.length})
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                    {log.source_documents.map(doc => (
                      <div
                        key={doc.doc_id}
                        className={`flex items-center gap-2 p-2 rounded border text-sm ${
                          doc.via_pattern ? 'bg-purple-50 border-purple-200' : 'bg-white'
                        }`}
                      >
                        <span className="font-mono text-xs px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded">{doc.doc_id}</span>
                        <span className="text-gray-700 truncate flex-1" title={doc.filename}>{doc.filename}</span>
                        {doc.via_pattern && (
                          <span className="text-xs px-1 py-0.5 bg-purple-100 text-purple-600 rounded" title={`Inherited via ${doc.via_pattern}`}>
                            {doc.via_pattern}
                          </span>
                        )}
                        <span className="text-gray-400 text-xs whitespace-nowrap">p.{doc.pages?.join(', ')}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ) : (
            <div className="space-y-4">
              {log?.pipeline ? (
                Object.entries(log.pipeline).map(([step, data]) => renderStepContent(step, data))
              ) : (
                <div className="text-gray-500 text-center py-8">No pipeline log available</div>
              )}
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
  categoryColor,
  diagnoses = [],
  procedures = [],
  loading,
  selectedCodes,
  onToggleCode,
  onSelectAll,
  onGenerate,
  onDeleteRule,
  onViewRule,
  generating
}) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [showMockOnly, setShowMockOnly] = useState(false);
  const [showDiagnoses, setShowDiagnoses] = useState(true);
  const [showProcedures, setShowProcedures] = useState(true);

  const filterCodes = (codes) => codes.filter(c => {
    const matchesSearch = c.code.toLowerCase().includes(searchQuery.toLowerCase());
    const matchesMockFilter = !showMockOnly || (c.rule_status?.has_rule && c.rule_status?.is_mock);
    return matchesSearch && matchesMockFilter;
  });

  const filteredDiagnoses = filterCodes(diagnoses);
  const filteredProcedures = filterCodes(procedures);
  const allFiltered = [...filteredDiagnoses, ...filteredProcedures];

  const selectableCodes = allFiltered.filter(c =>
    !c.rule_status?.has_rule || c.rule_status?.is_mock
  );
  const allSelected = selectableCodes.length > 0 &&
    selectableCodes.every(c => selectedCodes.has(c.code));

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="p-4 border-b bg-gray-50">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <div
              className="w-3 h-3 rounded-full"
              style={{ backgroundColor: categoryColor || '#6B7280' }}
            />
            <h3 className="font-semibold text-gray-900">{categoryName}</h3>
            <span className="text-sm text-gray-500">
              {diagnoses.length} dx · {procedures.length} proc
            </span>
          </div>
          <div className="flex items-center gap-2">
            {selectedCodes.size > 0 && (
              <button
                onClick={onGenerate}
                disabled={generating}
                className="px-3 py-1.5 bg-green-600 text-white text-sm rounded-lg hover:bg-green-700 disabled:bg-gray-300 flex items-center gap-1"
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
            />
          </div>

          <button
            onClick={() => setShowMockOnly(!showMockOnly)}
            className={`px-3 py-2 text-sm rounded-lg whitespace-nowrap ${
              showMockOnly ? 'bg-yellow-100 text-yellow-700' : 'text-gray-500 hover:bg-gray-100'
            }`}
          >
            Mock only
          </button>

          {selectableCodes.length > 0 && (
            <button
              onClick={() => onSelectAll(selectableCodes.map(c => c.code))}
              className="px-3 py-2 text-sm text-blue-600 hover:bg-blue-50 rounded-lg whitespace-nowrap"
            >
              {allSelected ? 'Deselect' : 'Select'} ({selectableCodes.length})
            </button>
          )}
        </div>
      </div>

      {/* Code lists */}
      <div className="flex-1 overflow-auto p-2">
        {allFiltered.length === 0 ? (
          <div className="text-center py-12 text-gray-400">
            No codes found
          </div>
        ) : (
          <div className="space-y-2">
            {/* Diagnoses Section */}
            {filteredDiagnoses.length > 0 && (
              <div className="border rounded-lg overflow-hidden">
                <button
                  onClick={() => setShowDiagnoses(!showDiagnoses)}
                  className="w-full flex items-center gap-2 px-3 py-2 bg-gray-50 hover:bg-gray-100 transition-colors"
                >
                  {showDiagnoses ? (
                    <ChevronDown className="w-4 h-4 text-gray-400" />
                  ) : (
                    <ChevronRight className="w-4 h-4 text-gray-400" />
                  )}
                  <span className="text-xs font-semibold text-gray-600 uppercase">
                    Diagnoses
                  </span>
                  <span className="text-xs text-gray-400">
                    ({filteredDiagnoses.length})
                  </span>
                </button>
                {showDiagnoses && (
                  <div className="p-2 space-y-1">
                    {filteredDiagnoses.map(code => {
                      const canSelect = !code.rule_status?.has_rule || code.rule_status?.is_mock;
                      return (
                        <CodeItem
                          key={code.code}
                          code={code}
                          isSelected={selectedCodes.has(code.code)}
                          canSelect={canSelect}
                          generating={generating}
                          onToggleCode={onToggleCode}
                          onDeleteRule={onDeleteRule}
                          onViewRule={onViewRule}
                        />
                      );
                    })}
                  </div>
                )}
              </div>
            )}

            {/* Procedures Section */}
            {filteredProcedures.length > 0 && (
              <div className="border rounded-lg overflow-hidden">
                <button
                  onClick={() => setShowProcedures(!showProcedures)}
                  className="w-full flex items-center gap-2 px-3 py-2 bg-gray-50 hover:bg-gray-100 transition-colors"
                >
                  {showProcedures ? (
                    <ChevronDown className="w-4 h-4 text-gray-400" />
                  ) : (
                    <ChevronRight className="w-4 h-4 text-gray-400" />
                  )}
                  <span className="text-xs font-semibold text-gray-600 uppercase">
                    Procedures
                  </span>
                  <span className="text-xs text-gray-400">
                    ({filteredProcedures.length})
                  </span>
                </button>
                {showProcedures && (
                  <div className="p-2 space-y-1">
                    {filteredProcedures.map(code => {
                      const canSelect = !code.rule_status?.has_rule || code.rule_status?.is_mock;
                      return (
                        <CodeItem
                          key={code.code}
                          code={code}
                          isSelected={selectedCodes.has(code.code)}
                          canSelect={canSelect}
                          generating={generating}
                          onToggleCode={onToggleCode}
                          onDeleteRule={onDeleteRule}
                          onViewRule={onViewRule}
                        />
                      );
                    })}
                  </div>
                )}
              </div>
            )}
          </div>
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
  const { categories, total_codes, total_with_rules, loading, refetch } = useCategories();
  const [selectedCategory, setSelectedCategory] = useState(null);
  const { diagnoses, procedures, loading: codesLoading, refetch: refetchCodes } = useCategoryCode(selectedCategory);

  const [selectedCodes, setSelectedCodes] = useState(new Set());
  const [generating, setGenerating] = useState(false);
  const [showProgress, setShowProgress] = useState(false);
  const [generationProgress, setGenerationProgress] = useState({});
  const [clearing, setClearing] = useState(false);
  const [viewingCode, setViewingCode] = useState(null);

  const selectedCategoryInfo = categories.find(c => c.name === selectedCategory);
  const categoryColor = selectedCategoryInfo?.color || '#6B7280';

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

  const handleGenerate = async () => {
    if (selectedCodes.size === 0) return;

    setGenerating(true);
    setShowProgress(true);

    const codesToGenerate = Array.from(selectedCodes);

    // Initialize progress with new structure
    const initialProgress = {};
    codesToGenerate.forEach(code => {
      initialProgress[code] = {
        status: 'pending',
        steps: {},
        error: null
      };
    });
    setGenerationProgress(initialProgress);

    // Generate sequentially
    for (const code of codesToGenerate) {
      // Mark as generating
      setGenerationProgress(prev => ({
        ...prev,
        [code]: {
          ...prev[code],
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
            parallel_validators: true  // Enable parallel mentor/redteam
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
          buffer = lines.pop() || ''; // Keep incomplete line in buffer

          for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed || !trimmed.startsWith('data: ')) continue;

            const data = parseSSELine(trimmed);
            if (!data) continue;

            // Handle different event types
            const { step, type } = data;

            if (step === 'done') {
              // Pipeline complete
              setGenerationProgress(prev => ({
                ...prev,
                [code]: {
                  ...prev[code],
                  status: 'complete'
                }
              }));
              continue;
            }

            if (step === 'error') {
              // Pipeline error
              setGenerationProgress(prev => ({
                ...prev,
                [code]: {
                  ...prev[code],
                  status: 'error',
                  error: data.message || 'Unknown error'
                }
              }));
              continue;
            }

            // Update step progress
            setGenerationProgress(prev => {
              const prevCode = prev[code] || { status: 'generating', steps: {} };
              const prevStep = prevCode.steps[step] || { status: 'idle', thinking: '', content: '' };

              let updatedStep = { ...prevStep };

              switch (type) {
                case 'status':
                  // Step started/streaming
                  updatedStep.status = data.status || 'streaming';
                  if (data.message) updatedStep.message = data.message;
                  break;

                case 'thought':
                case 'thinking':
                  // Append thinking chunk
                  updatedStep.status = 'streaming';
                  updatedStep.thinking = (updatedStep.thinking || '') + (data.thinking || data.content || '');
                  break;

                case 'content':
                case 'text':
                  // Append content chunk
                  updatedStep.status = 'streaming';
                  updatedStep.content = (updatedStep.content || '') + (data.content || data.text || '');
                  break;

                case 'done':
                  // Step complete
                  updatedStep.status = 'done';
                  if (data.full_text) updatedStep.content = data.full_text;
                  if (data.full_thinking) updatedStep.thinking = data.full_thinking;
                  if (data.duration_ms) updatedStep.duration_ms = data.duration_ms;
                  if (data.corrections_count !== undefined) {
                    updatedStep.corrections_count = data.corrections_count;
                  }
                  if (data.verdict) updatedStep.verdict = data.verdict;
                  if (data.citations_check) updatedStep.citations_check = data.citations_check;
                  break;

                case 'error':
                  updatedStep.status = 'error';
                  updatedStep.error = data.message;
                  break;
              }

              return {
                ...prev,
                [code]: {
                  ...prevCode,
                  steps: {
                    ...prevCode.steps,
                    [step]: updatedStep
                  }
                }
              };
            });
          }
        }

        // Ensure marked as complete if not already
        setGenerationProgress(prev => {
          if (prev[code]?.status === 'generating') {
            return {
              ...prev,
              [code]: { ...prev[code], status: 'complete' }
            };
          }
          return prev;
        });

      } catch (err) {
        console.error(`Failed to generate rule for ${code}:`, err);
        setGenerationProgress(prev => ({
          ...prev,
          [code]: {
            ...prev[code],
            status: 'error',
            error: err.message
          }
        }));
      }
    }

    setGenerating(false);
    setSelectedCodes(new Set());
    refetch();
    if (refetchCodes) refetchCodes();
  };

  const handleClearMocks = async () => {
    if (!window.confirm('Clear all mock rules? This will allow regeneration.')) return;

    setClearing(true);
    try {
      const res = await fetch(`${API_BASE}/mock`, { method: 'DELETE' });
      const data = await res.json();
      alert(`Cleared ${data.deleted} mock rules`);
      refetch();
      if (refetchCodes) refetchCodes();
    } catch (err) {
      console.error('Failed to clear mocks:', err);
      alert('Failed to clear mock rules');
    } finally {
      setClearing(false);
    }
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="p-4 border-b bg-white">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="font-semibold text-lg">Rule Generation</h2>
            <p className="text-sm text-gray-500">
              {total_with_rules} / {total_codes} codes have rules
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={handleClearMocks}
              disabled={clearing}
              className="px-3 py-1.5 text-sm text-red-600 hover:bg-red-50 rounded-lg flex items-center gap-1"
              title="Clear mock rules for regeneration"
            >
              {clearing ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Trash2 className="w-4 h-4" />
              )}
              Clear mocks
            </button>
            <button
              onClick={refetch}
              className="p-2 hover:bg-gray-100 rounded-lg"
              title="Refresh"
            >
              <RefreshCw className="w-4 h-4" />
            </button>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Categories sidebar */}
        <div className="w-72 border-r bg-gray-50 p-4 overflow-auto">
          <h3 className="text-xs font-semibold text-gray-500 uppercase mb-3">Categories</h3>
          <CategoryList
            categories={categories}
            selectedCategory={selectedCategory}
            onSelectCategory={setSelectedCategory}
            loading={loading}
          />
        </div>

        {/* Codes panel */}
        <div className="flex-1 overflow-hidden">
          {selectedCategory ? (
            <CodeList
              categoryName={selectedCategory}
              categoryColor={categoryColor}
              diagnoses={diagnoses}
              procedures={procedures}
              loading={codesLoading}
              selectedCodes={selectedCodes}
              onToggleCode={handleToggleCode}
              onSelectAll={handleSelectAll}
              onGenerate={handleGenerate}
              onDeleteRule={handleDeleteRule}
              onViewRule={handleViewRule}
              generating={generating}
            />
          ) : (
            <div className="h-full flex items-center justify-center text-gray-400">
              <div className="text-center">
                <Hash className="w-16 h-16 mx-auto mb-4 opacity-30" />
                <p className="text-lg font-medium">Select a category</p>
                <p className="text-sm">Choose a category to view codes</p>
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