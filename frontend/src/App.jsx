import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import {
  Database, FileText, Settings, Search, ChevronDown, ChevronRight,
  CheckCircle, Clock, AlertCircle, X, Eye, Edit, Folder, File,
  Code, Tag, BookOpen, Layers, ExternalLink, Upload, RefreshCw,
  ZoomIn, ZoomOut, ChevronLeft, Hash, Play, Loader2, Filter, FolderSearch,
  Zap, Shield, Trash2
} from 'lucide-react';
import RuleGeneration from './components/RuleGeneration';
import ClaimRules from './components/ClaimRules';
import PdfViewer from './components/PdfViewer';

// API base URL
const API_BASE = 'http://localhost:8001/api/kb';

// ============================================================
// DOCUMENT TYPE ICONS
// ============================================================

const DocTypeIcon = ({ docType, className = "w-4 h-4" }) => {
  const type = (docType || '').toLowerCase();

  if (type.includes('guideline')) {
    return <BookOpen className={`${className} text-gray-500`} />;
  }
  if (type.includes('polic')) {
    return <Shield className={`${className} text-gray-500`} />;
  }
  if (type.includes('codebook') || type.includes('coding')) {
    return <Hash className={`${className} text-gray-500`} />;
  }

  // Default
  return <FileText className={`${className} text-gray-400`} />;
};

// ============================================================
// API HOOKS
// ============================================================

function useDocuments() {
  const [documents, setDocuments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchDocuments = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/documents`);
      const data = await res.json();
      setDocuments(data);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchDocuments();
  }, [fetchDocuments]);

  return { documents, loading, error, refetch: fetchDocuments };
}

function useCodes() {
  const [codes, setCodes] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API_BASE}/codes`)
      .then(res => res.json())
      .then(data => {
        setCodes(data);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  return { codes, loading };
}

function useStats() {
  const [stats, setStats] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE}/stats`)
      .then(res => res.json())
      .then(setStats)
      .catch(() => {});
  }, []);

  return stats;
}

// ============================================================
// PDF VIEWER COMPONENT
// ============================================================

const PdfPageViewer = ({ docId, page, zoom }) => {
  const canvasRef = useRef(null);
  const [pdfDoc, setPdfDoc] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [rendering, setRendering] = useState(false);

  // Load PDF.js library
  useEffect(() => {
    if (window.pdfjsLib) return;

    const script = document.createElement('script');
    script.src = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js';
    script.async = true;
    script.onload = () => {
      window.pdfjsLib.GlobalWorkerOptions.workerSrc =
        'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js';
    };
    document.body.appendChild(script);
  }, []);

  // Load PDF document
  useEffect(() => {
    if (!docId) return;

    const loadPdf = async () => {
      setLoading(true);
      setError(null);

      // Wait for PDF.js to load
      let attempts = 0;
      while (!window.pdfjsLib && attempts < 50) {
        await new Promise(r => setTimeout(r, 100));
        attempts++;
      }

      if (!window.pdfjsLib) {
        setError('PDF.js failed to load');
        setLoading(false);
        return;
      }

      try {
        const pdfUrl = `${API_BASE}/documents/${docId}/pdf`;
        const doc = await window.pdfjsLib.getDocument(pdfUrl).promise;
        setPdfDoc(doc);
      } catch (err) {
        console.error('PDF load error:', err);
        setError('Failed to load PDF');
      } finally {
        setLoading(false);
      }
    };

    loadPdf();
  }, [docId]);

  // Render page
  useEffect(() => {
    if (!pdfDoc || !canvasRef.current || rendering) return;

    const renderPage = async () => {
      setRendering(true);

      try {
        const pageNum = Math.min(Math.max(1, page), pdfDoc.numPages);
        const pdfPage = await pdfDoc.getPage(pageNum);

        const scale = zoom / 100 * 1.5; // Base scale for good quality
        const viewport = pdfPage.getViewport({ scale });

        const canvas = canvasRef.current;
        const context = canvas.getContext('2d');

        canvas.height = viewport.height;
        canvas.width = viewport.width;

        await pdfPage.render({
          canvasContext: context,
          viewport: viewport
        }).promise;

      } catch (err) {
        console.error('Render error:', err);
      } finally {
        setRendering(false);
      }
    };

    renderPage();
  }, [pdfDoc, page, zoom]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center">
          <Loader2 className="w-8 h-8 animate-spin text-blue-600 mx-auto" />
          <p className="mt-2 text-sm text-gray-500">Loading PDF...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center text-gray-400">
          <FileText className="w-16 h-16 mx-auto mb-3 opacity-30" />
          <p className="font-medium">{error}</p>
          <a
            href={`${API_BASE}/documents/${docId}/pdf`}
            target="_blank"
            rel="noopener noreferrer"
            className="text-blue-600 text-sm mt-2 inline-block hover:underline"
          >
            Open PDF in new tab
          </a>
        </div>
      </div>
    );
  }

  return (
    <div className="p-4 relative">
      {rendering && (
        <div className="absolute inset-0 bg-white/50 flex items-center justify-center z-10">
          <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
        </div>
      )}
      <div className="bg-white shadow-lg rounded overflow-hidden inline-block">
        <canvas ref={canvasRef} className="max-w-full" />
      </div>
    </div>
  );
};

// ============================================================
// COMPONENTS
// ============================================================

const StatusBadge = ({ status }) => {
  const styles = {
    ready: 'bg-green-100 text-green-800',
    pending: 'bg-yellow-100 text-yellow-800',
    error: 'bg-red-100 text-red-800',
    clinical: 'bg-green-100 text-green-800',
    administrative: 'bg-blue-100 text-blue-800',
    reference: 'bg-gray-100 text-gray-500',
    empty: 'bg-gray-100 text-gray-400',
    clinical_guideline: 'bg-green-100 text-green-800',
    pa_policy: 'bg-blue-100 text-blue-800',
    coding_rules: 'bg-purple-100 text-purple-800',
  };
  const label = status?.replace(/_/g, ' ') || status;
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${styles[status] || 'bg-gray-100'}`}>
      {label}
    </span>
  );
};

// Document statistics summary - compact version
const DocumentStats = ({ doc }) => {
  const codes = doc.codes || [];
  const topics = doc.topics || [];
  const medications = doc.medications || [];

  // Group codes by type
  const codesByType = codes.reduce((acc, c) => {
    const type = c.type || 'Other';
    if (!acc[type]) acc[type] = [];
    acc[type].push(c);
    return acc;
  }, {});

  const icdCount = (codesByType['ICD-10'] || []).length;
  const cptCount = (codesByType['CPT'] || []).length;
  const hcpcsCount = (codesByType['HCPCS'] || []).length;

  // No data - show nothing
  if (codes.length === 0 && topics.length === 0 && medications.length === 0) {
    return null;
  }

  // Build code type breakdown
  const codeBreakdown = [];
  if (icdCount > 0) codeBreakdown.push(`${icdCount} ICD-10`);
  if (cptCount > 0) codeBreakdown.push(`${cptCount} CPT`);
  if (hcpcsCount > 0) codeBreakdown.push(`${hcpcsCount} HCPCS`);

  return (
    <div className="mt-2 flex items-center gap-4 text-xs" style={{ color: '#6b7280' }}>
      {codes.length > 0 && (
        <span className="flex items-center gap-1">
          <Hash className="w-3 h-3" />
          <span>Codes ({codes.length})</span>
          {codeBreakdown.length > 0 && (
            <span style={{ color: '#9ca3af' }} className="ml-1">
              {codeBreakdown.join(', ')}
            </span>
          )}
        </span>
      )}
      {topics.length > 0 && (
        <span className="flex items-center gap-1">
          <Tag className="w-3 h-3" />
          <span>Topics ({topics.length})</span>
        </span>
      )}
      {medications.length > 0 && (
        <span className="flex items-center gap-1">
          <span>💊</span>
          <span>Meds ({medications.length})</span>
        </span>
      )}
    </div>
  );
};

const TopicTag = ({ topic }) => (
  <span className="inline-flex items-center px-2 py-0.5 rounded bg-slate-100 text-slate-600 text-xs">
    {topic}
  </span>
);

// ============================================================
// DOCUMENT VIEWER
// ============================================================

const DocumentViewer = ({ docId, initialPage, highlightCode, codeToExpand, onClose, onCodeClick }) => {
  const [document, setDocument] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedPage, setSelectedPage] = useState(initialPage || 1);
  const [zoom, setZoom] = useState(100);
  const [activeTab, setActiveTab] = useState(codeToExpand ? 'codes' : 'content');
  // Store both code and type for expanded state (code+type makes unique key)
  const [expandedCodeKey, setExpandedCodeKey] = useState(
    codeToExpand ? `${codeToExpand.type}:${codeToExpand.code?.toString().toUpperCase()}` : null
  );
  const [expandedTypes, setExpandedTypes] = useState(codeToExpand?.type ? { [codeToExpand.type]: true } : {});
  const [expandedTopic, setExpandedTopic] = useState(null);
  const [expandedMed, setExpandedMed] = useState(null);
  const [showPageCodes, setShowPageCodes] = useState(false);
  const [showPageTopics, setShowPageTopics] = useState(false);
  const [showPageMeds, setShowPageMeds] = useState(false);
  const [codeSearch, setCodeSearch] = useState('');
  const [searchText, setSearchText] = useState('');  // For PDF text highlighting
  const [metaCategories, setMetaCategories] = useState(null);
  const highlightedPageRef = useRef(null);

  // Load meta_categories
  useEffect(() => {
    fetch(`${API_BASE}/meta-categories`)
      .then(res => res.json())
      .then(setMetaCategories)
      .catch(() => {});
  }, []);

  // Auto-scroll to highlighted page when it becomes visible
  useEffect(() => {
    // Wait for React to render the expanded category with page buttons
    const scrollToHighlighted = () => {
      if (highlightedPageRef.current) {
        highlightedPageRef.current.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    };
    // Try multiple times with increasing delays to catch the render
    const timers = [
      setTimeout(scrollToHighlighted, 100),
      setTimeout(scrollToHighlighted, 300),
      setTimeout(scrollToHighlighted, 500)
    ];
    return () => timers.forEach(t => clearTimeout(t));
  }, [expandedCodeKey, selectedPage]);

  useEffect(() => {
    fetch(`${API_BASE}/documents/${docId}`)
      .then(res => res.json())
      .then(data => {
        setDocument(data);
        setLoading(false);
        // Set initial page if provided, otherwise first content page
        if (initialPage) {
          setSelectedPage(initialPage);
        } else {
          const firstContent = data.pages?.find(p => p.content);
          if (firstContent) setSelectedPage(firstContent.page);
        }
      })
      .catch(() => setLoading(false));
  }, [docId, initialPage]);

  // Set searchText from highlightCode when document is opened
  useEffect(() => {
    if (highlightCode) {
      setSearchText(highlightCode);
    }
  }, [highlightCode]);

  // Expand code and switch to Codes tab when codeToExpand is provided
  useEffect(() => {
    if (codeToExpand && document) {
      setActiveTab('codes');

      const targetCode = codeToExpand.code?.toString().toUpperCase();
      const targetType = codeToExpand.type;
      const targetPage = selectedPage;

      // Expand the type group
      if (targetType) {
        setExpandedTypes(prev => ({ ...prev, [targetType]: true }));
      }

      // Trust the passed codeToExpand - set expandedCodeKey directly
      // This avoids issues with summary.all_codes having merged codes
      if (targetCode && targetType) {
        setExpandedCodeKey(`${targetType}:${targetCode}`);
      }
    }
  }, [codeToExpand, document, selectedPage]);

  // Build allCodes from pages to ensure type:code uniqueness
  // This fixes the issue where summary.all_codes may have merged ICD-10 "A" with HCPCS "A"
  // Only include codes that exist in meta_categories dictionary
  // Must be called before early returns to maintain hook order
  const allCodes = useMemo(() => {
    if (!document?.pages) return [];
    const codesMap = {};
    for (const page of document.pages) {
      for (const code of page.codes || []) {
        // Filter: only include codes that exist in meta_categories dictionary
        if (metaCategories) {
          const typeCategories = metaCategories[code.type]?.categories || {};
          if (!typeCategories[code.code]) continue; // Skip codes not in dictionary
        }

        const key = `${code.type}:${code.code}`;
        if (!codesMap[key]) {
          codesMap[key] = {
            code: code.code,
            type: code.type,
            pages: [],
            contexts: [],
            anchors: []
          };
        }
        if (!codesMap[key].pages.includes(page.page)) {
          codesMap[key].pages.push(page.page);
        }
        if (code.context || code.reason) {
          codesMap[key].contexts.push(code.context || code.reason);
        }
        // Always add anchor info for this page (even if no anchor text)
        // Note: JSON uses "start"/"end" not "anchor_start"/"anchor_end"
        const anchorInfo = { page: page.page, reason: code.reason || code.context };
        if (code.start && code.end) {
          anchorInfo.start = code.start;
          anchorInfo.end = code.end;
        } else if (code.anchor_start && code.anchor_end) {
          // Fallback for old format
          anchorInfo.start = code.anchor_start;
          anchorInfo.end = code.anchor_end;
        } else if (code.anchor) {
          anchorInfo.text = code.anchor;
        }
        codesMap[key].anchors.push(anchorInfo);
      }
    }
    return Object.values(codesMap);
  }, [document, metaCategories]);

  if (loading) {
    return (
      <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center">
        <div className="bg-white rounded-xl p-8">
          <Loader2 className="w-8 h-8 animate-spin text-blue-600 mx-auto" />
          <p className="mt-4 text-gray-500">Loading document...</p>
        </div>
      </div>
    );
  }

  if (!document) {
    return (
      <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center">
        <div className="bg-white rounded-xl p-8">
          <p className="text-red-500">Document not found</p>
          <button onClick={onClose} className="mt-4 px-4 py-2 bg-gray-100 rounded">Close</button>
        </div>
      </div>
    );
  }

  const currentPage = document.pages?.find(p => p.page === selectedPage);
  const allTopics = document.summary?.topics || [];
  const allMedications = document.summary?.medications || [];

  // Filter codes by search (searches code, contexts, and anchors)
  const filteredCodes = codeSearch
    ? allCodes.filter(c => {
        const search = codeSearch.toLowerCase();
        // Search in code
        if (c.code.toLowerCase().includes(search)) return true;
        // Search in contexts
        if (c.contexts?.some(ctx => ctx.toLowerCase().includes(search))) return true;
        // Search in anchors (support both old text and new start/end formats)
        if (c.anchors?.some(a => {
          const text = a.text || a.start || '';
          const end = a.end || '';
          return text.toLowerCase().includes(search) || end.toLowerCase().includes(search);
        })) return true;
        return false;
      })
    : allCodes;

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl w-full max-w-7xl h-[90vh] flex flex-col overflow-hidden shadow-2xl">

        {/* Header */}
        <div className="border-b bg-slate-50">
          {/* Row 1: Title */}
          <div className="px-4 py-3 flex items-center justify-between border-b border-slate-200">
            <div className="flex items-center gap-3">
              <DocTypeIcon docType={document.summary?.doc_type} className="w-6 h-6" />
              <div>
                <h2 className="font-bold text-lg text-gray-900">{document.filename}</h2>
                <div className="flex items-center gap-3 text-xs text-gray-500">
                  <span>{document.summary?.content_page_count || 0} content / {document.total_pages} total pages</span>
                  <StatusBadge status={document.summary?.doc_type || 'document'} />
                </div>
              </div>
            </div>
            <button onClick={onClose} className="p-2 hover:bg-gray-200 rounded-lg">
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Row 2: Compact page info */}
          <div className="px-4 py-2 flex items-center gap-4 border-b border-slate-200">
            <div className="flex items-center gap-2">
              <span className="text-xs font-medium text-gray-500 uppercase">Page:</span>
              <input
                type="number"
                min={1}
                max={document.total_pages}
                value={selectedPage}
                onChange={(e) => {
                  const val = parseInt(e.target.value);
                  if (val >= 1 && val <= document.total_pages) {
                    setSelectedPage(val);
                  }
                }}
                className="w-16 px-2 py-1 border rounded text-sm text-center"
              />
              <span className="text-sm text-gray-500">/ {document.total_pages}</span>
            </div>

            {/* Highlight code indicator */}
            {highlightCode && (
              <div className="flex items-center gap-2 px-2 py-1 bg-yellow-100 rounded-lg max-w-md">
                <Search className="w-3 h-3 text-yellow-600 flex-shrink-0" />
                <span className="text-xs font-medium text-yellow-700 truncate">
                  Citation: {highlightCode.startsWith('[RANGE]')
                    ? `"${highlightCode.replace('[RANGE]', '').split('|||')[0].substring(0, 30)}..."`
                    : `"${highlightCode.substring(0, 40)}..."`
                  }
                </span>
              </div>
            )}

            {/* Citation search indicator */}
            {searchText && (
              <div className="flex items-center gap-2 px-2 py-1 bg-yellow-100 rounded-lg">
                <Search className="w-3 h-3 text-yellow-600" />
                <span className="text-xs font-medium text-yellow-700 max-w-48 truncate">
                  Citation: "{searchText}"
                </span>
                <button
                  onClick={() => setSearchText('')}
                  className="p-0.5 hover:bg-yellow-200 rounded"
                  title="Clear citation highlight"
                >
                  <X className="w-3 h-3 text-yellow-600" />
                </button>
              </div>
            )}

            {/* Content pages indicator */}
            <div className="flex items-center gap-2 text-xs text-gray-500">
              <span className="text-green-600 font-medium">
                {document.summary?.content_page_count || 0} content
              </span>
              <span>•</span>
              <span className="text-gray-400">
                {(document.total_pages || 0) - (document.summary?.content_page_count || 0)} skipped
              </span>
            </div>

            {currentPage && (
              <div className="ml-auto flex items-center gap-2">
                {currentPage.skip_reason && (
                  <span className="text-xs text-gray-400 italic">{currentPage.skip_reason}</span>
                )}
                <StatusBadge status={currentPage.page_type} />
              </div>
            )}
          </div>

          {/* Row 3: Codes, Topics, Meds - clickable to open tabs */}
          {(allCodes.length > 0 || allTopics.length > 0 || allMedications.length > 0) && (
            <div className="px-4 py-2 flex items-center gap-4 text-xs overflow-x-auto">
              {allCodes.length > 0 && (
                <button
                  onClick={() => setActiveTab('codes')}
                  className="flex items-center gap-1.5 shrink-0 hover:bg-gray-100 rounded px-2 py-1 -mx-2"
                >
                  <Hash className="w-3 h-3 text-gray-400" />
                  <span className="text-gray-500 font-medium">{allCodes.length} codes</span>
                  {allCodes.slice(0, 3).map((c, i) => (
                    <span key={i} className="px-1.5 py-0.5 rounded bg-gray-100 text-gray-600 text-[10px] font-mono">
                      {c.code}
                    </span>
                  ))}
                  {allCodes.length > 3 && (
                    <span className="text-gray-400">+{allCodes.length - 3}</span>
                  )}
                </button>
              )}

              {allTopics.length > 0 && (
                <button
                  onClick={() => setActiveTab('topics')}
                  className="flex items-center gap-1.5 shrink-0 border-l pl-4 hover:bg-gray-100 rounded px-2 py-1"
                >
                  <Tag className="w-3 h-3 text-gray-400" />
                  <span className="text-gray-500 font-medium">{allTopics.length} topics</span>
                </button>
              )}

              {allMedications.length > 0 && (
                <button
                  onClick={() => setActiveTab('meds')}
                  className="flex items-center gap-1.5 shrink-0 border-l pl-4 hover:bg-gray-100 rounded px-2 py-1"
                >
                  <span className="text-emerald-600">💊</span>
                  <span className="text-gray-500 font-medium">{allMedications.length} meds</span>
                </button>
              )}
            </div>
          )}
        </div>

        {/* Content: PDF + Text */}
        <div className="flex-1 flex overflow-hidden">
          {/* Left: PDF */}
          <div className="w-1/2 border-r bg-slate-100 flex flex-col">
            <div className="p-2 border-b bg-white flex items-center justify-between">
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setSelectedPage(p => Math.max(1, p - 1))}
                  disabled={selectedPage === 1}
                  className="p-1.5 hover:bg-gray-100 rounded disabled:opacity-30"
                >
                  <ChevronLeft className="w-4 h-4" />
                </button>
                <span className="text-sm font-medium px-2">
                  {selectedPage} / {document.total_pages}
                </span>
                <button
                  onClick={() => setSelectedPage(p => Math.min(document.total_pages, p + 1))}
                  disabled={selectedPage === document.total_pages}
                  className="p-1.5 hover:bg-gray-100 rounded disabled:opacity-30"
                >
                  <ChevronRight className="w-4 h-4" />
                </button>
              </div>
              <div className="flex items-center gap-2">
                <div className="flex items-center gap-1 bg-gray-100 rounded-full px-2 py-1">
                  <button onClick={() => setZoom(z => Math.max(50, z - 25))} className="p-1 hover:bg-gray-200 rounded-full">
                    <ZoomOut className="w-4 h-4" />
                  </button>
                  <span className="text-xs font-mono w-10 text-center">{zoom}%</span>
                  <button onClick={() => setZoom(z => Math.min(200, z + 25))} className="p-1 hover:bg-gray-200 rounded-full">
                    <ZoomIn className="w-4 h-4" />
                  </button>
                </div>
                <a
                  href={`${API_BASE}/documents/${docId}/pdf`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="p-1.5 hover:bg-gray-100 rounded text-gray-500 hover:text-gray-700"
                  title="Open PDF in new tab"
                >
                  <ExternalLink className="w-4 h-4" />
                </a>
              </div>
            </div>

            {/* PDF Viewer with text highlighting */}
            <div className="flex-1 overflow-hidden bg-slate-200">
              <PdfViewer
                url={`${API_BASE}/documents/${docId}/pdf`}
                pageNumber={selectedPage}
                searchText={searchText}
              />
            </div>
          </div>

          {/* Right: Tabs panel */}
          <div className="w-1/2 flex flex-col overflow-hidden bg-white">
            {/* Tabs */}
            <div className="border-b bg-slate-50 flex">
              {['content', 'codes', 'topics', 'meds'].map(tab => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                    activeTab === tab 
                      ? 'border-blue-600 text-blue-600 bg-white' 
                      : 'border-transparent text-gray-500 hover:text-gray-700'
                  }`}
                >
                  {tab === 'content' && <span className="flex items-center gap-1"><BookOpen className="w-3 h-3" /> Text</span>}
                  {tab === 'codes' && <span className="flex items-center gap-1"><Hash className="w-3 h-3" /> Codes ({allCodes.length})</span>}
                  {tab === 'topics' && <span className="flex items-center gap-1"><Tag className="w-3 h-3" /> Topics ({allTopics.length})</span>}
                  {tab === 'meds' && <span className="flex items-center gap-1">💊 Meds ({allMedications.length})</span>}
                </button>
              ))}
            </div>

            {/* Tab content */}
            <div className="flex-1 overflow-auto">
              {/* Content tab */}
              {activeTab === 'content' && (
                <>
                  {currentPage?.content ? (
                    <div className="p-4">
                      {/* Collapsible metadata sections */}
                      {(currentPage.codes?.length > 0 || currentPage.topics?.length > 0 || currentPage.medications?.length > 0) && (
                        <div className="mb-4 space-y-1">
                          {/* Codes section */}
                          {currentPage.codes?.length > 0 && (
                            <div className="border rounded">
                              <button
                                onClick={() => setShowPageCodes(!showPageCodes)}
                                className="w-full px-3 py-1.5 flex items-center justify-between hover:bg-gray-50 text-left"
                              >
                                <span className="text-xs font-medium text-gray-600 flex items-center gap-1">
                                  <Hash className="w-3 h-3" /> Codes ({currentPage.codes.length})
                                </span>
                                <ChevronDown className={`w-3 h-3 text-gray-400 transition-transform ${showPageCodes ? 'rotate-180' : ''}`} />
                              </button>
                              {showPageCodes && (
                                <div className="px-3 py-2 bg-gray-50 border-t flex flex-wrap gap-1">
                                  {currentPage.codes.map((c, i) => (
                                    <span
                                      key={i}
                                      className="text-xs px-2 py-0.5 bg-gray-100 text-gray-600 rounded font-mono cursor-pointer hover:bg-gray-200"
                                      onClick={() => {
                                        setActiveTab('codes');
                                        const key = `${c.type}:${c.code?.toString().toUpperCase()}`;
                                        setExpandedCodeKey(key);
                                        if (c.type) {
                                          setExpandedTypes(prev => ({ ...prev, [c.type]: true }));
                                        }
                                      }}
                                    >
                                      {c.code}
                                    </span>
                                  ))}
                                </div>
                              )}
                            </div>
                          )}

                          {/* Topics section */}
                          {currentPage.topics?.length > 0 && (
                            <div className="border rounded">
                              <button
                                onClick={() => setShowPageTopics(!showPageTopics)}
                                className="w-full px-3 py-1.5 flex items-center justify-between hover:bg-gray-50 text-left"
                              >
                                <span className="text-xs font-medium text-blue-700 flex items-center gap-1">
                                  <Tag className="w-3 h-3" /> Topics ({currentPage.topics.length})
                                </span>
                                <ChevronDown className={`w-3 h-3 text-gray-400 transition-transform ${showPageTopics ? 'rotate-180' : ''}`} />
                              </button>
                              {showPageTopics && (
                                <div className="px-3 py-2 bg-gray-50 border-t flex flex-wrap gap-1">
                                  {currentPage.topics.map((t, i) => (
                                    <span
                                      key={i}
                                      className="text-xs px-2 py-0.5 bg-blue-100 text-blue-700 rounded cursor-pointer hover:bg-blue-200"
                                      onClick={() => {
                                        setActiveTab('topics');
                                        setExpandedTopic(t);
                                      }}
                                    >
                                      {t}
                                    </span>
                                  ))}
                                </div>
                              )}
                            </div>
                          )}

                          {/* Medications section */}
                          {currentPage.medications?.length > 0 && (
                            <div className="border rounded">
                              <button
                                onClick={() => setShowPageMeds(!showPageMeds)}
                                className="w-full px-3 py-1.5 flex items-center justify-between hover:bg-gray-50 text-left"
                              >
                                <span className="text-xs font-medium text-emerald-700 flex items-center gap-1">
                                  💊 Medications ({currentPage.medications.length})
                                </span>
                                <ChevronDown className={`w-3 h-3 text-gray-400 transition-transform ${showPageMeds ? 'rotate-180' : ''}`} />
                              </button>
                              {showPageMeds && (
                                <div className="px-3 py-2 bg-gray-50 border-t flex flex-wrap gap-1">
                                  {currentPage.medications.map((m, i) => (
                                    <span
                                      key={i}
                                      className="text-xs px-2 py-0.5 bg-emerald-100 text-emerald-700 rounded cursor-pointer hover:bg-emerald-200"
                                      onClick={() => {
                                        setActiveTab('meds');
                                        setExpandedMed(m);
                                      }}
                                    >
                                      {m}
                                    </span>
                                  ))}
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      )}
                      <div className="whitespace-pre-wrap text-sm leading-relaxed text-gray-800">
                        {highlightCode ? (
                          // Highlight the code in content
                          (() => {
                            const content = currentPage.content;
                            const regex = new RegExp(`(${highlightCode.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
                            const parts = content.split(regex);
                            return parts.map((part, i) =>
                              regex.test(part) ? (
                                <mark key={i} className="bg-yellow-300 px-0.5 rounded">{part}</mark>
                              ) : part
                            );
                          })()
                        ) : (
                          currentPage.content
                        )}
                      </div>
                    </div>
                  ) : (
                    <div className="h-full flex items-center justify-center text-gray-400">
                      <div className="text-center">
                        <BookOpen className="w-16 h-16 mx-auto mb-3 opacity-30" />
                        <p className="font-medium">No Content</p>
                        <p className="text-xs mt-1">{currentPage?.skip_reason || 'Page skipped'}</p>
                      </div>
                    </div>
                  )}
                </>
              )}

              {/* Codes tab */}
              {activeTab === 'codes' && (
                <div className="flex flex-col h-full">
                  {/* Search */}
                  <div className="p-2 border-b">
                    <div className="relative">
                      <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                      <input
                        type="text"
                        placeholder="Search categories..."
                        value={codeSearch}
                        onChange={(e) => setCodeSearch(e.target.value)}
                        className="w-full pl-9 pr-3 py-2 border rounded-lg text-sm"
                      />
                    </div>
                  </div>

                  <div className="flex-1 overflow-auto">
                    {(() => {
                      // Helper to get category info
                      const getCategoryInfo = (codeType, codePattern) => {
                        if (!metaCategories || !metaCategories[codeType]) {
                          return { name: codePattern, range: '', exists: false };
                        }
                        const categories = metaCategories[codeType]?.categories || {};
                        const info = categories[codePattern];
                        return info
                          ? { name: info.name, range: info.range, exists: true }
                          : { name: codePattern, range: '', exists: false };
                      };

                      // Group codes by type, filtering out codes not in dictionary
                      const codesByType = filteredCodes.reduce((acc, c) => {
                        const type = c.type || 'Other';
                        // Only include codes that exist in meta_categories dictionary
                        const info = getCategoryInfo(type, c.code);
                        if (!info.exists) return acc;

                        if (!acc[type]) acc[type] = [];
                        acc[type].push(c);
                        return acc;
                      }, {});

                      // Filter by search
                      const searchFiltered = Object.entries(codesByType).reduce((acc, [type, codes]) => {
                        if (!codeSearch) {
                          acc[type] = codes;
                          return acc;
                        }
                        const query = codeSearch.toLowerCase();
                        const filtered = codes.filter(c => {
                          const info = getCategoryInfo(type, c.code);
                          return c.code.toLowerCase().includes(query) || info.name.toLowerCase().includes(query);
                        });
                        if (filtered.length > 0) acc[type] = filtered;
                        return acc;
                      }, {});

                      // Sort types
                      const typeOrder = ['ICD-10', 'CPT', 'HCPCS'];
                      const sortedTypes = Object.keys(searchFiltered).sort((a, b) => {
                        const aIdx = typeOrder.indexOf(a);
                        const bIdx = typeOrder.indexOf(b);
                        if (aIdx === -1 && bIdx === -1) return a.localeCompare(b);
                        if (aIdx === -1) return 1;
                        if (bIdx === -1) return -1;
                        return aIdx - bIdx;
                      });

                      if (sortedTypes.length === 0) {
                        return <p className="text-center text-gray-400 py-8">No codes found</p>;
                      }

                      return (
                        <div className="divide-y">
                          {sortedTypes.map(type => {
                            const typeCodes = searchFiltered[type] || [];
                            const isTypeExpanded = expandedTypes[type] !== false; // Default expanded
                            const sortedCodes = [...typeCodes].sort((a, b) => a.code.localeCompare(b.code));

                            return (
                              <div key={type}>
                                {/* Type header */}
                                <button
                                  onClick={() => setExpandedTypes(prev => ({ ...prev, [type]: !isTypeExpanded }))}
                                  className="w-full px-3 py-2 flex items-center justify-between hover:bg-gray-50 transition-colors bg-slate-50"
                                >
                                  <div className="flex items-center gap-2">
                                    <ChevronRight className={`w-4 h-4 text-gray-400 transition-transform ${isTypeExpanded ? 'rotate-90' : ''}`} />
                                    <span className="font-semibold text-gray-800 text-sm">{type}</span>
                                    <span className="text-xs text-gray-500">({typeCodes.length})</span>
                                  </div>
                                </button>

                                {/* Type content - categories */}
                                {isTypeExpanded && (
                                  <div className="bg-white">
                                    {sortedCodes.map(codeInfo => {
                                      const info = getCategoryInfo(type, codeInfo.code);
                                      // Compare using type:code key for uniqueness
                                      const codeKey = `${type}:${codeInfo.code?.toString().toUpperCase()}`;
                                      const isCodeExpanded = expandedCodeKey === codeKey;
                                      const pageCount = codeInfo.pages?.length || 0;

                                      // Collect reasons
                                      const reasons = [...new Set([
                                        ...(codeInfo.contexts || []),
                                        ...(codeInfo.anchors?.map(a => a.reason).filter(Boolean) || [])
                                      ])].filter(r => r);

                                      return (
                                        <div key={codeInfo.code} className="border-b border-gray-100 last:border-b-0">
                                          {/* Category row */}
                                          <button
                                            onClick={() => setExpandedCodeKey(isCodeExpanded ? null : codeKey)}
                                            className="w-full px-4 py-2 flex items-center justify-between hover:bg-gray-50 transition-colors text-left"
                                          >
                                            <div className="flex items-center gap-2 flex-1 min-w-0">
                                              <ChevronRight className={`w-3 h-3 text-gray-400 transition-transform flex-shrink-0 ${isCodeExpanded ? 'rotate-90' : ''}`} />
                                              <span className="text-sm text-gray-700 truncate">
                                                {info.name}
                                                {info.range && <span className="text-gray-400 ml-1">({info.range})</span>}
                                              </span>
                                              {reasons.length > 0 && (
                                                <div className="relative group flex-shrink-0">
                                                  <svg className="w-4 h-4 text-gray-400 hover:text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                                    <circle cx="12" cy="12" r="10" strokeWidth="2"/>
                                                    <path strokeWidth="2" d="M12 16v-4M12 8h.01"/>
                                                  </svg>
                                                  <div className="hidden group-hover:block absolute left-6 top-0 z-50 w-64 p-2 bg-gray-900 text-white text-xs rounded shadow-lg">
                                                    {reasons.slice(0, 3).map((r, i) => (
                                                      <div key={i} className="text-gray-300">• {r}</div>
                                                    ))}
                                                  </div>
                                                </div>
                                              )}
                                            </div>
                                            <span className="text-xs text-gray-400 flex-shrink-0 ml-2">
                                              {pageCount} page{pageCount !== 1 ? 's' : ''}
                                            </span>
                                          </button>

                                          {/* Expanded: pages list */}
                                          {isCodeExpanded && codeInfo.pages?.length > 0 && (
                                            <div className="px-4 pb-2 space-y-1">
                                              {codeInfo.pages.map((pageNum, idx) => {
                                                const anchor = codeInfo.anchors?.find(a => a.page === pageNum);
                                                const hasAnchor = !!(anchor?.text || anchor?.start);
                                                const searchValue = anchor?.start && anchor?.end
                                                  ? `[RANGE]${anchor.start}|||${anchor.end}[/RANGE]`
                                                  : anchor?.text || anchor?.start;
                                                const displayText = anchor?.text
                                                  ? `"${anchor.text}"`
                                                  : anchor?.start && anchor?.end
                                                    ? `"${anchor.start}" ... "${anchor.end}"`
                                                    : null;

                                                const isCurrentPage = pageNum === selectedPage;

                                                return (
                                                  <button
                                                    key={idx}
                                                    ref={isCurrentPage ? highlightedPageRef : null}
                                                    onClick={() => {
                                                      setSelectedPage(pageNum);
                                                      if (searchValue) setSearchText(searchValue);
                                                    }}
                                                    className={`w-full text-left px-2 py-1.5 rounded transition-colors group/page ${
                                                      isCurrentPage
                                                        ? 'bg-blue-100 border border-blue-300'
                                                        : 'hover:bg-blue-50'
                                                    }`}
                                                  >
                                                    <div className="flex items-center gap-2">
                                                      <span className={`text-xs font-medium group-hover/page:underline ${
                                                        isCurrentPage ? 'text-blue-700' : 'text-blue-600'
                                                      }`}>
                                                        Page {pageNum}
                                                      </span>
                                                      {hasAnchor && <span className="text-yellow-500 text-xs">•</span>}
                                                      <ChevronRight className={`w-3 h-3 ml-auto ${
                                                        isCurrentPage ? 'text-blue-500' : 'text-gray-300 group-hover/page:text-blue-400'
                                                      }`} />
                                                    </div>
                                                    {displayText && (
                                                      <p className="text-xs text-gray-500 truncate mt-0.5 italic">
                                                        {displayText}
                                                      </p>
                                                    )}
                                                  </button>
                                                );
                                              })}
                                            </div>
                                          )}
                                        </div>
                                      );
                                    })}
                                  </div>
                                )}
                              </div>
                            );
                          })}
                        </div>
                      );
                    })()}
                  </div>
                </div>
              )}

              {/* Topics tab */}
              {activeTab === 'topics' && (
                <div className="p-2">
                  {allTopics.length === 0 ? (
                    <p className="text-center text-gray-400 py-8">No topics found</p>
                  ) : (
                    <div className="space-y-1">
                      {allTopics.map((topicInfo, i) => {
                        // Handle both old format (string) and new format (object with name, pages, anchors)
                        const topicName = typeof topicInfo === 'string' ? topicInfo : topicInfo.name;
                        const topicPages = typeof topicInfo === 'string'
                          ? document.pages?.filter(p => p.topics?.some(t => (typeof t === 'string' ? t : t.name) === topicName)).map(p => p.page) || []
                          : topicInfo.pages || [];
                        const topicAnchors = typeof topicInfo === 'string' ? [] : topicInfo.anchors || [];
                        const isExpanded = expandedTopic === topicName;

                        return (
                          <div key={i} className="border rounded">
                            <button
                              onClick={() => setExpandedTopic(isExpanded ? null : topicName)}
                              className="w-full px-3 py-2 flex items-center justify-between hover:bg-gray-50 text-left"
                            >
                              <div className="flex items-center gap-2">
                                <ChevronRight className={`w-3 h-3 text-gray-400 transition-transform ${isExpanded ? 'rotate-90' : ''}`} />
                                <span className="text-sm">{topicName}</span>
                              </div>
                              <span className="text-xs text-gray-400">{topicPages.length} pages</span>
                            </button>
                            {/* Expanded: pages list */}
                            {isExpanded && topicPages.length > 0 && (
                              <div className="px-4 pb-2 space-y-1">
                                {topicPages.map((pageNum, idx) => {
                                  const anchor = topicAnchors.find(a => a.page === pageNum);
                                  const hasAnchor = !!(anchor?.text || anchor?.start);
                                  const searchValue = anchor?.start && anchor?.end
                                    ? `[RANGE]${anchor.start}|||${anchor.end}[/RANGE]`
                                    : anchor?.text || anchor?.start || topicName;
                                  const displayText = anchor?.text
                                    ? `"${anchor.text}"`
                                    : anchor?.start && anchor?.end
                                      ? `"${anchor.start}" ... "${anchor.end}"`
                                      : null;

                                  const isCurrentPage = pageNum === selectedPage;

                                  return (
                                    <button
                                      key={idx}
                                      onClick={() => {
                                        setSelectedPage(pageNum);
                                        setSearchText(searchValue);
                                      }}
                                      className={`w-full text-left px-2 py-1.5 rounded transition-colors group/page ${
                                        isCurrentPage
                                          ? 'bg-blue-100 border border-blue-300'
                                          : 'hover:bg-blue-50'
                                      }`}
                                    >
                                      <div className="flex items-center gap-2">
                                        <span className={`text-xs font-medium group-hover/page:underline ${
                                          isCurrentPage ? 'text-blue-700' : 'text-blue-600'
                                        }`}>
                                          Page {pageNum}
                                        </span>
                                        {hasAnchor && <span className="text-yellow-500 text-xs">•</span>}
                                        <ChevronRight className={`w-3 h-3 ml-auto ${
                                          isCurrentPage ? 'text-blue-500' : 'text-gray-300 group-hover/page:text-blue-400'
                                        }`} />
                                      </div>
                                      {displayText && (
                                        <p className="text-xs text-gray-500 truncate mt-0.5 italic">
                                          {displayText}
                                        </p>
                                      )}
                                    </button>
                                  );
                                })}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              )}

              {/* Medications tab */}
              {activeTab === 'meds' && (
                <div className="p-2">
                  {allMedications.length === 0 ? (
                    <p className="text-center text-gray-400 py-8">No medications found</p>
                  ) : (
                    <div className="space-y-1">
                      {allMedications.map((medInfo, i) => {
                        // Handle both old format (string) and new format (object with name, pages, anchors)
                        const medName = typeof medInfo === 'string' ? medInfo : medInfo.name;
                        const medPages = typeof medInfo === 'string'
                          ? document.pages?.filter(p => p.medications?.some(m => (typeof m === 'string' ? m : m.name) === medName)).map(p => p.page) || []
                          : medInfo.pages || [];
                        const medAnchors = typeof medInfo === 'string' ? [] : medInfo.anchors || [];
                        const isExpanded = expandedMed === medName;

                        return (
                          <div key={i} className="border rounded">
                            <button
                              onClick={() => setExpandedMed(isExpanded ? null : medName)}
                              className="w-full px-3 py-2 flex items-center justify-between hover:bg-gray-50 text-left"
                            >
                              <div className="flex items-center gap-2">
                                <ChevronRight className={`w-3 h-3 text-gray-400 transition-transform ${isExpanded ? 'rotate-90' : ''}`} />
                                <span className="text-sm font-medium text-emerald-700">{medName}</span>
                              </div>
                              <span className="text-xs text-gray-400">{medPages.length} pages</span>
                            </button>
                            {/* Expanded: pages list */}
                            {isExpanded && medPages.length > 0 && (
                              <div className="px-4 pb-2 space-y-1">
                                {medPages.map((pageNum, idx) => {
                                  const anchor = medAnchors.find(a => a.page === pageNum);
                                  const hasAnchor = !!(anchor?.text || anchor?.start);
                                  const searchValue = anchor?.start && anchor?.end
                                    ? `[RANGE]${anchor.start}|||${anchor.end}[/RANGE]`
                                    : anchor?.text || anchor?.start || medName;
                                  const displayText = anchor?.text
                                    ? `"${anchor.text}"`
                                    : anchor?.start && anchor?.end
                                      ? `"${anchor.start}" ... "${anchor.end}"`
                                      : null;

                                  const isCurrentPage = pageNum === selectedPage;

                                  return (
                                    <button
                                      key={idx}
                                      onClick={() => {
                                        setSelectedPage(pageNum);
                                        setSearchText(searchValue);
                                      }}
                                      className={`w-full text-left px-2 py-1.5 rounded transition-colors group/page ${
                                        isCurrentPage
                                          ? 'bg-emerald-100 border border-emerald-300'
                                          : 'hover:bg-emerald-50'
                                      }`}
                                    >
                                      <div className="flex items-center gap-2">
                                        <span className={`text-xs font-medium group-hover/page:underline ${
                                          isCurrentPage ? 'text-emerald-700' : 'text-emerald-600'
                                        }`}>
                                          Page {pageNum}
                                        </span>
                                        {hasAnchor && <span className="text-yellow-500 text-xs">•</span>}
                                        <ChevronRight className={`w-3 h-3 ml-auto ${
                                          isCurrentPage ? 'text-emerald-500' : 'text-gray-300 group-hover/page:text-emerald-400'
                                        }`} />
                                      </div>
                                      {displayText && (
                                        <p className="text-xs text-gray-500 truncate mt-0.5 italic">
                                          {displayText}
                                        </p>
                                      )}
                                    </button>
                                  );
                                })}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

// ============================================================
// CODE INDEX
// ============================================================

// Hook to load meta_categories
function useMetaCategories() {
  const [metaCategories, setMetaCategories] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API_BASE}/meta-categories`)
      .then(res => res.json())
      .then(data => {
        setMetaCategories(data);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  return { metaCategories, loading };
}

// Info icon with tooltip
const InfoTooltip = ({ reasons }) => {
  const [showTooltip, setShowTooltip] = useState(false);

  if (!reasons || reasons.length === 0) return null;

  // Merge and dedupe reasons
  const uniqueReasons = [...new Set(reasons)].filter(r => r && r.trim());
  if (uniqueReasons.length === 0) return null;

  return (
    <div className="relative inline-block">
      <button
        onMouseEnter={() => setShowTooltip(true)}
        onMouseLeave={() => setShowTooltip(false)}
        className="text-gray-400 hover:text-blue-500 transition-colors"
      >
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <circle cx="12" cy="12" r="10" strokeWidth="2"/>
          <path strokeWidth="2" d="M12 16v-4M12 8h.01"/>
        </svg>
      </button>
      {showTooltip && (
        <div className="absolute left-6 top-0 z-50 w-72 p-3 bg-gray-900 text-white text-xs rounded-lg shadow-lg">
          <div className="font-medium mb-1">Why this category was found:</div>
          <ul className="space-y-1">
            {uniqueReasons.map((reason, i) => (
              <li key={i} className="text-gray-300">• {reason}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
};

const CodeIndexView = ({ onDocumentClick, onClose }) => {
  const { codes, loading: codesLoading } = useCodes();
  const { metaCategories, loading: metaLoading } = useMetaCategories();
  const [expandedTypes, setExpandedTypes] = useState({});
  const [expandedCodes, setExpandedCodes] = useState({});
  const [codeDetails, setCodeDetails] = useState({});
  const [searchQuery, setSearchQuery] = useState('');

  const loading = codesLoading || metaLoading;

  // Get category info from meta_categories
  const getCategoryInfo = (codeType, codePattern) => {
    if (!metaCategories || !metaCategories[codeType]) {
      return { name: codePattern, range: '', exists: false };
    }

    const categories = metaCategories[codeType].categories || {};
    const info = categories[codePattern];

    if (info) {
      return { name: info.name, range: info.range, exists: true };
    }

    return { name: codePattern, range: '', exists: false };
  };

  // Group codes by type, filtering out codes not in dictionary
  const codesByType = codes.reduce((acc, c) => {
    if (!c || !c.code) return acc;
    const type = c.type || 'Other';
    // Only include codes that exist in meta_categories dictionary
    const info = getCategoryInfo(type, c.code);
    if (!info.exists) return acc;

    if (!acc[type]) acc[type] = [];
    acc[type].push(c);
    return acc;
  }, {});

  // Filter codes by search
  const filteredCodesByType = Object.entries(codesByType).reduce((acc, [type, typeCodes]) => {
    if (!searchQuery) {
      acc[type] = typeCodes;
      return acc;
    }

    const query = searchQuery.toLowerCase();
    const filtered = typeCodes.filter(c => {
      const info = getCategoryInfo(type, c.code);
      return c.code.toLowerCase().includes(query) ||
             info.name.toLowerCase().includes(query);
    });

    if (filtered.length > 0) {
      acc[type] = filtered;
    }
    return acc;
  }, {});

  // Sort types: ICD-10, CPT, HCPCS, then others
  const typeOrder = ['ICD-10', 'CPT', 'HCPCS'];
  const sortedTypes = Object.keys(filteredCodesByType).sort((a, b) => {
    const aIdx = typeOrder.indexOf(a);
    const bIdx = typeOrder.indexOf(b);
    if (aIdx === -1 && bIdx === -1) return a.localeCompare(b);
    if (aIdx === -1) return 1;
    if (bIdx === -1) return -1;
    return aIdx - bIdx;
  });

  // Load code details when expanded - include type for filtering
  const loadCodeDetails = async (code, codeType) => {
    const key = `${codeType}:${code}`;
    if (codeDetails[key]) return;

    try {
      // Pass code_type as query param to filter by type
      const url = `${API_BASE}/codes/${encodeURIComponent(code)}?code_type=${encodeURIComponent(codeType)}`;
      const res = await fetch(url);
      const data = await res.json();
      setCodeDetails(prev => ({ ...prev, [key]: data }));
    } catch (e) {
      console.error('Failed to load code details:', e);
    }
  };

  const toggleType = (type) => {
    setExpandedTypes(prev => ({ ...prev, [type]: !prev[type] }));
  };

  const toggleCode = (code, codeType) => {
    const key = `${codeType}:${code}`;
    const newExpanded = !expandedCodes[key];
    setExpandedCodes(prev => ({ ...prev, [key]: newExpanded }));
    if (newExpanded) {
      loadCodeDetails(code, codeType);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl w-full max-w-4xl h-[85vh] flex flex-col overflow-hidden shadow-2xl">
        {/* Header */}
        <div className="px-4 py-3 border-b flex items-center justify-between bg-slate-50">
          <div className="flex items-center gap-3">
            <Code className="w-5 h-5 text-blue-600" />
            <h2 className="font-semibold text-gray-900">Code Index</h2>
            <span className="text-sm text-gray-500">{codes.length} categories</span>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-gray-200 rounded-lg">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Search */}
        <div className="px-4 py-3 border-b bg-white">
          <div className="relative">
            <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search categories..."
              className="w-full pl-9 pr-3 py-2 border rounded-lg text-sm"
            />
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-auto">
          {loading ? (
            <div className="p-8 text-center">
              <Loader2 className="w-8 h-8 animate-spin mx-auto text-gray-400" />
              <p className="mt-2 text-gray-500 text-sm">Loading categories...</p>
            </div>
          ) : sortedTypes.length === 0 ? (
            <div className="p-8 text-center text-gray-400">
              <Hash className="w-12 h-12 mx-auto mb-2 opacity-50" />
              <p>No categories found</p>
            </div>
          ) : (
            <div className="divide-y">
              {sortedTypes.map(type => {
                const typeCodes = filteredCodesByType[type] || [];
                const isTypeExpanded = expandedTypes[type];

                // Sort codes by letter/number
                const sortedCodes = [...typeCodes].sort((a, b) => a.code.localeCompare(b.code));

                return (
                  <div key={type}>
                    {/* Type header */}
                    <button
                      onClick={() => toggleType(type)}
                      className="w-full px-4 py-3 flex items-center justify-between hover:bg-gray-50 transition-colors"
                    >
                      <div className="flex items-center gap-2">
                        <ChevronRight className={`w-4 h-4 text-gray-400 transition-transform ${isTypeExpanded ? 'rotate-90' : ''}`} />
                        <span className="font-semibold text-gray-900">{type}</span>
                        <span className="text-sm text-gray-500">({typeCodes.length})</span>
                      </div>
                    </button>

                    {/* Type content - categories list */}
                    {isTypeExpanded && (
                      <div className="bg-gray-50 border-t">
                        {sortedCodes.map(code => {
                          const codeKey = `${type}:${code.code}`;
                          const info = getCategoryInfo(type, code.code);
                          const isCodeExpanded = expandedCodes[codeKey];
                          const details = codeDetails[codeKey];
                          const docCount = code.documents?.[0]?.count || code.documents?.length || 0;

                          // Collect all reasons from details
                          const allReasons = [];
                          if (details?.documents) {
                            details.documents.forEach(doc => {
                              doc.pages?.forEach(page => {
                                if (page.reason) allReasons.push(page.reason);
                                if (page.context) allReasons.push(page.context);
                              });
                            });
                          }

                          return (
                            <div key={codeKey} className="border-b border-gray-200 last:border-b-0">
                              {/* Category row */}
                              <button
                                onClick={() => toggleCode(code.code, type)}
                                className="w-full px-6 py-2.5 flex items-center justify-between hover:bg-white transition-colors text-left"
                              >
                                <div className="flex items-center gap-2 flex-1 min-w-0">
                                  <ChevronRight className={`w-3 h-3 text-gray-400 transition-transform flex-shrink-0 ${isCodeExpanded ? 'rotate-90' : ''}`} />
                                  <span className="text-sm text-gray-800 truncate">
                                    {info.name}
                                    {info.range && (
                                      <span className="text-gray-500 ml-1">({info.range})</span>
                                    )}
                                  </span>
                                  {isCodeExpanded && allReasons.length > 0 && (
                                    <InfoTooltip reasons={allReasons} />
                                  )}
                                </div>
                                <span className="text-xs text-gray-400 flex-shrink-0 ml-2">
                                  {docCount} doc{docCount !== 1 ? 's' : ''}
                                </span>
                              </button>

                              {/* Expanded details */}
                              {isCodeExpanded && (
                                <div className="px-6 pb-3 bg-white">
                                  {!details ? (
                                    <div className="py-2 text-center">
                                      <Loader2 className="w-4 h-4 animate-spin mx-auto text-gray-400" />
                                    </div>
                                  ) : details.documents?.length === 0 ? (
                                    <p className="text-xs text-gray-400 py-2">No documents found</p>
                                  ) : (
                                    <div className="space-y-3">
                                      {details.documents?.map(doc => {
                                        // Group pages by page number and merge reasons
                                        const pageGroups = {};
                                        doc.pages?.forEach(p => {
                                          if (!pageGroups[p.page]) {
                                            pageGroups[p.page] = {
                                              page: p.page,
                                              anchors: [],
                                              reasons: []
                                            };
                                          }
                                          if (p.anchor) pageGroups[p.page].anchors.push(p.anchor);
                                          if (p.reason) pageGroups[p.page].reasons.push(p.reason);
                                          if (p.context) pageGroups[p.page].reasons.push(p.context);
                                        });

                                        const pages = Object.values(pageGroups).sort((a, b) => a.page - b.page);

                                        return (
                                          <div key={doc.id} className="border rounded-lg overflow-hidden">
                                            <div className="px-3 py-2 bg-slate-50 flex items-center justify-between">
                                              <div className="flex items-center gap-2 min-w-0">
                                                <DocTypeIcon docType={doc.doc_type} className="w-4 h-4 flex-shrink-0" />
                                                <span className="text-sm font-medium truncate">{doc.filename}</span>
                                              </div>
                                              <button
                                                onClick={() => onDocumentClick(doc.id)}
                                                className="text-xs text-blue-600 hover:underline flex items-center gap-1 flex-shrink-0"
                                              >
                                                Open <ExternalLink className="w-3 h-3" />
                                              </button>
                                            </div>
                                            <div className="p-2 space-y-1">
                                              {pages.map((pageInfo, i) => {
                                                const anchor = pageInfo.anchors[0];
                                                let highlightText = null;
                                                if (anchor?.start && anchor?.end) {
                                                  highlightText = `[RANGE]${anchor.start}|||${anchor.end}[/RANGE]`;
                                                } else if (anchor?.text) {
                                                  highlightText = anchor.text;
                                                } else if (anchor?.start) {
                                                  highlightText = anchor.start;
                                                }

                                                // Get unique reasons for this page
                                                const pageReasons = [...new Set(pageInfo.reasons)].filter(r => r);

                                                return (
                                                  <button
                                                    key={i}
                                                    onClick={() => onDocumentClick(doc.id, pageInfo.page, highlightText, { code: code.code, type })}
                                                    className="w-full text-left hover:bg-blue-50 px-2 py-1.5 rounded transition-colors group"
                                                  >
                                                    <div className="flex items-center gap-2">
                                                      <span className="text-xs font-medium text-blue-600 group-hover:underline">
                                                        Page {pageInfo.page}
                                                      </span>
                                                      {pageInfo.anchors.length > 1 && (
                                                        <span className="text-xs text-gray-400">
                                                          ({pageInfo.anchors.length} citations)
                                                        </span>
                                                      )}
                                                      {pageReasons.length > 0 && (
                                                        <InfoTooltip reasons={pageReasons} />
                                                      )}
                                                      <ChevronRight className="w-3 h-3 text-gray-300 group-hover:text-blue-400 ml-auto" />
                                                    </div>
                                                    {anchor?.start && anchor?.end ? (
                                                      <p className="text-xs italic text-gray-500 mt-0.5 truncate">
                                                        "{anchor.start}" ... "{anchor.end}"
                                                      </p>
                                                    ) : anchor?.text ? (
                                                      <p className="text-xs italic text-gray-500 mt-0.5 truncate">
                                                        "{anchor.text}"
                                                      </p>
                                                    ) : null}
                                                  </button>
                                                );
                                              })}
                                            </div>
                                          </div>
                                        );
                                      })}
                                    </div>
                                  )}
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

// ============================================================
// DOCUMENT LIST
// ============================================================

const DocumentList = ({ documents, onDocumentClick, onCodeClick, onRefresh, selectedDocs, setSelectedDocs, isGlobalParsing, globalParseProgress, batchDocStatuses = {} }) => {
  // Динамически определяем папки из filepath документов
  const extractFolder = (filepath) => {
    if (!filepath) return 'other';

    // Разбиваем путь на части
    const parts = filepath.replace(/^\//, '').split('/');

    // Если только имя файла без папки
    if (parts.length === 1) return 'other';

    // Берём папки (всё кроме последнего элемента - имени файла)
    const folderParts = parts.slice(0, -1);

    // Ищем известную папку
    const knownFolders = ['guidelines', 'policies', 'coding', 'codebooks'];
    for (const folder of knownFolders) {
      if (folderParts.includes(folder)) return folder;
    }

    // Возвращаем первую папку в пути
    return folderParts[0] || 'other';
  };

  // Группируем документы по папкам
  const documentsByFolder = {};
  documents.forEach(doc => {
    const folder = extractFolder(doc.filepath);
    if (!documentsByFolder[folder]) {
      documentsByFolder[folder] = [];
    }
    documentsByFolder[folder].push(doc);
  });

  // Сортируем папки: известные сначала, потом остальные
  const knownOrder = ['guidelines', 'policies', 'coding', 'codebooks'];
  const folders = Object.keys(documentsByFolder).sort((a, b) => {
    const aIdx = knownOrder.indexOf(a);
    const bIdx = knownOrder.indexOf(b);
    if (aIdx >= 0 && bIdx >= 0) return aIdx - bIdx;
    if (aIdx >= 0) return -1;
    if (bIdx >= 0) return 1;
    if (a === 'other') return 1;
    if (b === 'other') return -1;
    return a.localeCompare(b);
  });

  const [expandedFolders, setExpandedFolders] = useState(folders);
  const [parsing, setParsing] = useState(null);
  const [parseProgress, setParseProgress] = useState({}); // {docId: {percent, message}}
  const [deleting, setDeleting] = useState(null); // docId being deleted

  const toggleFolder = (folder) => {
    setExpandedFolders(prev =>
      prev.includes(folder) ? prev.filter(f => f !== folder) : [...prev, folder]
    );
  };

  const toggleSelect = (docId, e) => {
    e.stopPropagation();
    setSelectedDocs(prev => {
      const next = new Set(prev);
      if (next.has(docId)) {
        next.delete(docId);
      } else {
        next.add(docId);
      }
      return next;
    });
  };

  const toggleSelectAll = (folder) => {
    const folderDocs = documentsByFolder[folder] || [];
    // Выбираем все документы в папке (включая уже парсенные для reparse)
    const docIds = folderDocs.map(d => d.id);

    const allSelected = docIds.every(id => selectedDocs.has(id));

    setSelectedDocs(prev => {
      const next = new Set(prev);
      if (allSelected) {
        docIds.forEach(id => next.delete(id));
      } else {
        docIds.forEach(id => next.add(id));
      }
      return next;
    });
  };

  const handleParse = async (e, docId, force = false) => {
    e.stopPropagation();
    if (isGlobalParsing) return;

    setParsing(docId);
    setParseProgress(prev => ({ ...prev, [docId]: { pages_done: 0, total_pages: 0 } }));

    try {
      const forceParam = force ? '&force=true' : '';
      const eventSource = new EventSource(`${API_BASE}/documents/${docId}/parse-stream?_=${Date.now()}${forceParam}`);

      eventSource.onmessage = (event) => {
        const data = JSON.parse(event.data);

        setParseProgress(prev => ({
          ...prev,
          [docId]: {
            pages_done: data.pages_done || 0,
            total_pages: data.total_pages || 0
          }
        }));

        if (data.status === 'complete' || data.status === 'already_parsed' || data.status === 'error') {
          eventSource.close();
          setParsing(null);
          setParseProgress(prev => {
            const next = { ...prev };
            delete next[docId];
            return next;
          });
          onRefresh?.();
        }
      };

      eventSource.onerror = () => {
        eventSource.close();
        setParsing(null);
        setParseProgress(prev => {
          const next = { ...prev };
          delete next[docId];
          return next;
        });
      };

    } catch (err) {
      console.error('Parse failed:', err);
      setParsing(null);
      setParseProgress(prev => {
        const next = { ...prev };
        delete next[docId];
        return next;
      });
    }
  };

  const handleDelete = async (e, docId, filename) => {
    e.stopPropagation();
    if (isGlobalParsing || deleting) return;

    if (!window.confirm(`Delete "${filename}"?\n\nThis will remove the document from the database. The source PDF will be preserved.`)) {
      return;
    }

    setDeleting(docId);
    try {
      const res = await fetch(`${API_BASE}/documents/${docId}`, {
        method: 'DELETE'
      });

      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.detail || 'Delete failed');
      }

      onRefresh?.();
    } catch (err) {
      console.error('Delete failed:', err);
      alert(`Failed to delete: ${err.message}`);
    } finally {
      setDeleting(null);
    }
  };

  return (
    <div className="space-y-3">
      {folders.map(folder => {
        const folderDocs = documentsByFolder[folder] || [];
        if (folderDocs.length === 0) return null;

        const allSelected = folderDocs.length > 0 &&
          folderDocs.every(d => selectedDocs.has(d.id));

        return (
          <div key={folder}>
            <div className="flex items-center gap-2 mb-2">
              <button
                onClick={() => toggleFolder(folder)}
                className="flex items-center gap-2 text-sm font-medium transition-colors"
                style={{ color: '#1a1a1a' }}
                onMouseEnter={(e) => e.currentTarget.style.color = '#0090DA'}
                onMouseLeave={(e) => e.currentTarget.style.color = '#1a1a1a'}
              >
                <ChevronRight
                  className="w-4 h-4 transition-transform"
                  style={{ color: '#9ca3af', transform: expandedFolders.includes(folder) ? 'rotate(90deg)' : 'rotate(0deg)' }}
                />
                <Folder className="w-4 h-4" style={{ color: '#9ca3af' }} />
                {folder}/
                <span style={{ color: '#9ca3af', fontWeight: 'normal' }}>({folderDocs.length})</span>
              </button>

              {expandedFolders.includes(folder) && (
                <button
                  onClick={() => toggleSelectAll(folder)}
                  className="text-xs ml-2 transition-colors"
                  style={{ color: '#0090DA' }}
                  onMouseEnter={(e) => e.currentTarget.style.color = '#0070aa'}
                  onMouseLeave={(e) => e.currentTarget.style.color = '#0090DA'}
                >
                  {allSelected ? 'Deselect all' : `Select all`}
                </button>
              )}
            </div>

            {expandedFolders.includes(folder) && (
              <div className="ml-6 space-y-2">
                {folderDocs.map(doc => {
                  const isCurrentlyParsing = globalParseProgress?.currentDocId === doc.id;
                  const batchStatus = batchDocStatuses[doc.id]; // 'queued' | 'parsing' | 'done' | 'error'

                  // Determine card style based on batch status
                  let cardStyle = 'bg-white';
                  if (batchStatus === 'queued') cardStyle = 'bg-yellow-50 border-yellow-200';
                  else if (batchStatus === 'parsing') cardStyle = 'bg-blue-50 border-blue-300';
                  else if (batchStatus === 'done') cardStyle = 'bg-green-50 border-green-200';
                  else if (batchStatus === 'error') cardStyle = 'bg-red-50 border-red-200';

                  // Determine left border color based on status
                  const borderLeftColor = doc.parsed_at ? '#059669' : '#0090DA';

                  return (
                  <div
                    key={doc.id}
                    className="rounded-lg p-3 transition-all"
                    style={{
                      background: selectedDocs.has(doc.id) && !batchStatus ? '#f0fdfa' : batchStatus === 'parsing' ? '#eff6ff' : batchStatus === 'done' ? '#f0fdf4' : batchStatus === 'error' ? '#fef2f2' : batchStatus === 'queued' ? '#fffbeb' : 'white',
                      border: '1px solid #e5e7eb',
                      borderLeft: `4px solid ${batchStatus === 'error' ? '#dc2626' : batchStatus === 'done' ? '#059669' : batchStatus === 'parsing' ? '#0090DA' : batchStatus === 'queued' ? '#d97706' : borderLeftColor}`
                    }}
                  >
                    <div className="flex items-start justify-between">
                      <div className="flex items-center gap-2 flex-1">
                        {/* Batch status indicator */}
                        {batchStatus === 'queued' && (
                          <Clock className="w-4 h-4 text-yellow-500" title="In queue" />
                        )}
                        {batchStatus === 'done' && (
                          <CheckCircle className="w-4 h-4 text-green-500" title="Completed" />
                        )}
                        {batchStatus === 'error' && (
                          <AlertCircle className="w-4 h-4 text-red-500" title="Error" />
                        )}

                        {/* Чекбокс для всех документов (включая reparse) */}
                        {!batchStatus && (
                          <input
                            type="checkbox"
                            checked={selectedDocs.has(doc.id)}
                            onChange={(e) => toggleSelect(doc.id, e)}
                            disabled={isGlobalParsing}
                            className="w-4 h-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500 disabled:opacity-50"
                          />
                        )}
                        <div
                          className="flex items-center gap-2 cursor-pointer"
                          onClick={() => onDocumentClick(doc.id)}
                        >
                          <DocTypeIcon docType={doc.doc_type} />
                          <span className="font-medium text-sm" style={{ color: '#1a1a1a' }}>{doc.filename}</span>
                          {doc.payer && (
                            <span className="px-1.5 py-0.5 rounded text-xs" style={{ background: '#f3f4f6', color: '#6b7280' }}>
                              {doc.payer}
                            </span>
                          )}
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        {/* Show batch status first if present */}
                        {batchStatus === 'queued' && (
                          <span className="text-xs text-yellow-600 font-medium">Queued</span>
                        )}
                        {batchStatus === 'done' && (
                          <span className="text-xs text-green-600 font-medium">Done ✓</span>
                        )}
                        {batchStatus === 'error' && (
                          <span className="text-xs text-red-600 font-medium">Error</span>
                        )}

                        {doc.parsed_at ? (
                          <>
                            {(parsing === doc.id || isCurrentlyParsing) ? (
                              <div className="flex items-center gap-2">
                                <Loader2 className="w-3 h-3 animate-spin text-blue-600" />
                                <span className="text-xs text-blue-600 font-mono">
                                  {isCurrentlyParsing
                                    ? `${globalParseProgress.pages_done}/${globalParseProgress.total_pages}`
                                    : `${parseProgress[doc.id]?.pages_done || 0}/${parseProgress[doc.id]?.total_pages || doc.total_pages || '?'}`
                                  }
                                </span>
                              </div>
                            ) : !batchStatus ? (
                              <>
                                <CheckCircle className="w-4 h-4" style={{ color: '#059669' }} />
                                <span className="text-xs" style={{ color: '#059669' }}>
                                  {doc.total_pages || 0}/{doc.total_pages || 0} pages
                                </span>
                                <button
                                  onClick={(e) => handleParse(e, doc.id, true)}
                                  disabled={isGlobalParsing}
                                  className="px-2 py-1 text-xs rounded-lg flex items-center gap-1 transition-colors disabled:opacity-30"
                                  style={{ color: '#6b7280', border: '1px solid #d1d5db' }}
                                  onMouseEnter={(e) => !isGlobalParsing && (e.currentTarget.style.background = '#f3f4f6')}
                                  onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                                  title="Reparse document"
                                >
                                  <RefreshCw className="w-3 h-3" />
                                </button>
                                <button
                                  onClick={(e) => handleDelete(e, doc.id, doc.filename)}
                                  disabled={isGlobalParsing || deleting === doc.id}
                                  className="px-2 py-1 text-xs rounded-lg flex items-center gap-1 transition-colors disabled:opacity-30"
                                  style={{ color: '#dc2626', border: '1px solid #fecaca' }}
                                  onMouseEnter={(e) => !isGlobalParsing && (e.currentTarget.style.background = '#fef2f2')}
                                  onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                                  title="Delete document"
                                >
                                  {deleting === doc.id ? <Loader2 className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
                                </button>
                              </>
                            ) : null}
                          </>
                        ) : (
                          <>
                            {(parsing === doc.id || isCurrentlyParsing) ? (
                              <div className="flex items-center gap-2">
                                <Loader2 className="w-3 h-3 animate-spin text-blue-600" />
                                <span className="text-xs text-blue-600 font-mono">
                                  {isCurrentlyParsing
                                    ? `${globalParseProgress.pages_done}/${globalParseProgress.total_pages}`
                                    : `${parseProgress[doc.id]?.pages_done || 0}/${parseProgress[doc.id]?.total_pages || doc.total_pages || '?'}`
                                  }
                                </span>
                              </div>
                            ) : (
                              <>
                                <span className="text-xs text-gray-400">
                                  {doc.total_pages || 0} pages
                                </span>
                                <button
                                  onClick={(e) => handleParse(e, doc.id)}
                                  disabled={isGlobalParsing}
                                  className="px-2.5 py-1 text-xs rounded-lg flex items-center gap-1 font-medium transition-colors"
                                  style={{
                                    color: isGlobalParsing ? '#9ca3af' : '#0090DA',
                                    background: 'transparent',
                                    border: isGlobalParsing ? '1px solid #d1d5db' : '1px solid #0090DA',
                                    cursor: isGlobalParsing ? 'not-allowed' : 'pointer'
                                  }}
                                  onMouseEnter={(e) => !isGlobalParsing && (e.currentTarget.style.background = '#eff6ff')}
                                  onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                                >
                                  <Zap className="w-3 h-3" />
                                  Parse
                                </button>
                                <button
                                  onClick={(e) => handleDelete(e, doc.id, doc.filename)}
                                  disabled={isGlobalParsing || deleting === doc.id}
                                  className="px-2 py-1 text-xs rounded-lg flex items-center gap-1 transition-colors disabled:opacity-30"
                                  style={{ color: '#dc2626', border: '1px solid #fecaca' }}
                                  onMouseEnter={(e) => !isGlobalParsing && (e.currentTarget.style.background = '#fef2f2')}
                                  onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                                  title="Delete document"
                                >
                                  {deleting === doc.id ? <Loader2 className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
                                </button>
                              </>
                            )}
                          </>
                        )}
                      </div>
                    </div>

                    {/* Document statistics */}
                    <DocumentStats doc={doc} />
                  </div>
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

// ============================================================
// UPLOAD COMPONENT
// ============================================================

const UploadZone = ({ onUploadComplete }) => {
  const [uploading, setUploading] = useState(false);
  const [folder, setFolder] = useState('guidelines');

  const handleUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setUploading(true);

    const formData = new FormData();
    formData.append('file', file);
    formData.append('folder', folder);

    try {
      const res = await fetch(`${API_BASE}/documents/upload`, {
        method: 'POST',
        body: formData
      });
      const data = await res.json();

      if (data.status === 'success' || data.status === 'exists') {
        onUploadComplete?.();
      }
    } catch (err) {
      console.error('Upload failed:', err);
    } finally {
      setUploading(false);
      e.target.value = ''; // Reset input
    }
  };

  return (
    <div className="flex items-center gap-2">
      <select
        value={folder}
        onChange={(e) => setFolder(e.target.value)}
        className="px-3 py-1.5 rounded-lg text-sm"
        style={{ border: '1px solid #e5e7eb', color: '#374151' }}
        disabled={uploading}
      >
        <option value="guidelines">guidelines/</option>
        <option value="policies">policies/</option>
        <option value="coding">coding/</option>
        <option value="codebooks">codebooks/</option>
      </select>

      <label
        className="inline-flex items-center gap-2 px-4 py-1.5 rounded-lg cursor-pointer transition-colors font-medium text-sm"
        style={{
          color: uploading ? '#9ca3af' : '#0090DA',
          background: 'transparent',
          border: uploading ? '1px solid #d1d5db' : '2px solid #0090DA',
          cursor: uploading ? 'not-allowed' : 'pointer'
        }}
        onMouseEnter={(e) => !uploading && (e.currentTarget.style.background = '#eff6ff')}
        onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
      >
        {uploading ? (
          <>
            <Loader2 className="w-4 h-4 animate-spin" />
            Uploading...
          </>
        ) : (
          <>
            <Upload className="w-4 h-4" />
            Upload PDF
          </>
        )}
        <input
          type="file"
          accept=".pdf"
          onChange={handleUpload}
          disabled={uploading}
          className="hidden"
        />
      </label>
    </div>
  );
};

// ============================================================
// MAIN APP
// ============================================================

export default function KnowledgeBaseApp() {
  const { documents, loading, refetch } = useDocuments();
  const stats = useStats();

  const [activeTab, setActiveTab] = useState('kb'); // 'kb' | 'rules' | 'cms'
  const [selectedDocId, setSelectedDocId] = useState(null);
  const [initialPage, setInitialPage] = useState(null);
  const [highlightCode, setHighlightCode] = useState(null);
  const [showCodeIndex, setShowCodeIndex] = useState(false);
  const [scanning, setScanning] = useState(false);
  const [parsingAll, setParsingAll] = useState(false);
  const [parseAllProgress, setParseAllProgress] = useState({
    currentDoc: 0,
    totalDocs: 0,
    currentFile: '',
    currentDocId: null,
    pages_done: 0,
    total_pages: 0
  });
  // Batch parsing statuses: { docId: 'queued' | 'parsing' | 'done' | 'error' }
  const [batchDocStatuses, setBatchDocStatuses] = useState({});
  const [selectedDocs, setSelectedDocs] = useState(new Set());

  // Global parsing state - blocks all parse buttons
  const isGlobalParsing = parsingAll;

  const [codeToExpand, setCodeToExpand] = useState(null);

  const handleDocumentClick = (docId, page = null, highlightCode = null, codeInfo = null) => {
    setSelectedDocId(docId);
    setInitialPage(page);
    setHighlightCode(highlightCode);
    setCodeToExpand(codeInfo);  // { code, type } to expand in Codes tab
    setShowCodeIndex(false);
  };

  const handleCodeClick = (code) => {
    setSelectedDocId(null);
    setShowCodeIndex(true);
  };

  const handleScan = async () => {
    setScanning(true);
    try {
      await fetch(`${API_BASE}/scan`);
      refetch();
    } catch (err) {
      console.error('Scan failed:', err);
    } finally {
      setScanning(false);
    }
  };

  const handleParseAll = async () => {
    // Parse selected or all unparsed
    let toParse = [];

    if (selectedDocs.size > 0) {
      // Parse selected documents (including reparsing already parsed)
      toParse = documents.filter(d => selectedDocs.has(d.id));
    } else {
      // Parse all unparsed
      toParse = documents.filter(d => !d.parsed_at);
    }

    if (toParse.length === 0) return;

    setParsingAll(true);
    const totalDocs = toParse.length;

    // Initialize all documents as 'queued'
    const initialStatuses = {};
    toParse.forEach(doc => {
      initialStatuses[doc.id] = 'queued';
    });
    setBatchDocStatuses(initialStatuses);

    for (let i = 0; i < toParse.length; i++) {
      const doc = toParse[i];

      // Mark current as 'parsing'
      setBatchDocStatuses(prev => ({ ...prev, [doc.id]: 'parsing' }));

      setParseAllProgress({
        currentDoc: i + 1,
        totalDocs,
        currentFile: doc.filename,
        currentDocId: doc.id,
        pages_done: 0,
        total_pages: doc.total_pages || 0
      });

      let finalStatus = 'done';

      try {
        // force=true для перепарсинга уже парсенных документов
        const forceParam = doc.parsed_at ? '&force=true' : '';

        // Используем SSE для отслеживания прогресса
        await new Promise((resolve, reject) => {
          const eventSource = new EventSource(`${API_BASE}/documents/${doc.id}/parse-stream?_=${Date.now()}${forceParam}`);

          eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);

            setParseAllProgress(prev => ({
              ...prev,
              pages_done: data.pages_done || 0,
              total_pages: data.total_pages || prev.total_pages
            }));

            if (data.status === 'complete' || data.status === 'already_parsed') {
              eventSource.close();
              finalStatus = 'done';
              resolve();
            } else if (data.status === 'error') {
              eventSource.close();
              finalStatus = 'error';
              resolve();
            }
          };

          eventSource.onerror = () => {
            eventSource.close();
            finalStatus = 'error';
            resolve(); // Continue even on error
          };
        });

      } catch (err) {
        console.error(`Parse failed for ${doc.filename}:`, err);
        finalStatus = 'error';
      }

      // Update status after completion
      setBatchDocStatuses(prev => ({ ...prev, [doc.id]: finalStatus }));
    }

    setParsingAll(false);
    setParseAllProgress({ currentDoc: 0, totalDocs: 0, currentFile: '', currentDocId: null, pages_done: 0, total_pages: 0 });
    setSelectedDocs(new Set()); // Clear selection after parsing

    // Clear batch statuses after 3 seconds
    setTimeout(() => {
      setBatchDocStatuses({});
    }, 3000);

    refetch();
  };

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Header */}
      <header className="bg-white border-b">
        <div className="max-w-7xl mx-auto px-4">
          <div className="flex items-center justify-between py-4">
            <div>
              <h1 className="text-xl font-bold text-gray-900">CMS-1500 Rule Builder</h1>
              <p className="text-sm text-gray-500">Knowledge Base & Rules</p>
            </div>
          </div>

          {/* Tabs */}
          <div className="flex gap-1">
            <button
              onClick={() => setActiveTab('kb')}
              className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                activeTab === 'kb'
                  ? 'border-blue-600 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              <span className="flex items-center gap-2">
                <Database className="w-4 h-4" />
                Knowledge Base
              </span>
            </button>
            <button
              onClick={() => setActiveTab('rules')}
              className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                activeTab === 'rules'
                  ? 'border-blue-600 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              <span className="flex items-center gap-2">
                <Zap className="w-4 h-4" />
                ICD10/CPT Rules
              </span>
            </button>
            <button
              onClick={() => setActiveTab('cms')}
              className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                activeTab === 'cms'
                  ? 'border-blue-600 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              <span className="flex items-center gap-2">
                <Shield className="w-4 h-4" />
                CMS-1500 Rules
              </span>
            </button>
          </div>
        </div>
      </header>

      {/* Content */}
      {activeTab === 'kb' ? (
      <main className="max-w-7xl mx-auto px-4 py-6">
        {/* Stats Cards */}
        <div className="grid grid-cols-4 gap-4 mb-6">
          <div className="bg-white rounded-lg p-4 border">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-gray-100 rounded-lg">
                <FileText className="w-5 h-5 text-gray-500" />
              </div>
              <div>
                <div className="text-2xl font-bold">{stats?.documents || documents.length}</div>
                <div className="text-xs text-gray-500">Documents</div>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg p-4 border">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-gray-100 rounded-lg">
                <Hash className="w-5 h-5 text-gray-500" />
              </div>
              <div>
                <div className="text-2xl font-bold">{stats?.codes_indexed || 0}</div>
                <div className="text-xs text-gray-500">Codes Indexed</div>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg p-4 border">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-gray-100 rounded-lg">
                <Database className="w-5 h-5 text-gray-500" />
              </div>
              <div>
                <div className="text-2xl font-bold">
                  {stats?.reference_data?.reduce((sum, t) => sum + t.records, 0)?.toLocaleString() || 0}
                </div>
                <div className="text-xs text-gray-500">Reference Records</div>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg p-4 border">
            <button
              onClick={() => setShowCodeIndex(true)}
              className="w-full h-full flex items-center justify-center gap-2 text-blue-600 hover:text-blue-700 font-medium"
            >
              <Search className="w-5 h-5" />
              Search Codes
            </button>
          </div>
        </div>

        {/* Documents Section */}
        <div className="bg-white rounded-lg" style={{ border: '1px solid #e5e7eb' }}>
          <div className="p-4 flex items-center justify-between" style={{ borderBottom: '1px solid #e5e7eb', background: '#f9fafb' }}>
            <h2 className="font-semibold flex items-center gap-2" style={{ color: '#1a1a1a' }}>
              <Database className="w-4 h-4" style={{ color: '#6b7280' }} />
              Documents
            </h2>
            <div className="flex items-center gap-2">
              <button
                onClick={handleScan}
                disabled={scanning || parsingAll}
                className="px-3 py-1.5 rounded-lg text-sm flex items-center gap-1.5 font-medium transition-colors disabled:opacity-50"
                style={{
                  color: scanning || parsingAll ? '#9ca3af' : '#059669',
                  background: 'transparent',
                  border: scanning || parsingAll ? '1px solid #d1d5db' : '1px solid #059669'
                }}
                onMouseEnter={(e) => !(scanning || parsingAll) && (e.currentTarget.style.background = '#f0fdf4')}
                onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                title="Scan folders for PDF files"
              >
                {scanning ? <Loader2 className="w-4 h-4 animate-spin" /> : <FolderSearch className="w-4 h-4" />}
                Scan
              </button>
              <button
                onClick={handleParseAll}
                disabled={parsingAll || (selectedDocs.size === 0 && documents.filter(d => !d.parsed_at).length === 0)}
                className="px-3 py-1.5 rounded-lg text-sm flex items-center gap-1.5 font-medium transition-colors disabled:opacity-50"
                style={{
                  color: parsingAll ? '#9ca3af' : '#0090DA',
                  background: 'transparent',
                  border: parsingAll ? '1px solid #d1d5db' : '1px solid #0090DA'
                }}
                onMouseEnter={(e) => !parsingAll && (e.currentTarget.style.background = '#eff6ff')}
                onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                title={selectedDocs.size > 0 ? "Parse selected documents" : "Parse all unparsed documents"}
              >
                {parsingAll ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    <span className="font-mono">
                      [{parseAllProgress.currentDoc}/{parseAllProgress.totalDocs}] {parseAllProgress.pages_done}/{parseAllProgress.total_pages}
                    </span>
                  </>
                ) : selectedDocs.size > 0 ? (
                  <>
                    <Zap className="w-4 h-4" />
                    Parse Selected ({selectedDocs.size})
                  </>
                ) : (
                  <>
                    <Zap className="w-4 h-4" />
                    Parse All
                  </>
                )}
              </button>
              {selectedDocs.size > 0 && !parsingAll && (
                <button
                  onClick={() => setSelectedDocs(new Set())}
                  className="px-2 py-1.5 rounded-lg text-sm transition-colors"
                  style={{ color: '#6b7280' }}
                  onMouseEnter={(e) => e.currentTarget.style.background = '#f3f4f6'}
                  onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                  title="Clear selection"
                >
                  <X className="w-4 h-4" />
                </button>
              )}
              <UploadZone onUploadComplete={refetch} />
              <button
                onClick={refetch}
                className="p-2 rounded-lg transition-colors"
                style={{ color: '#6b7280' }}
                onMouseEnter={(e) => e.currentTarget.style.background = '#f3f4f6'}
                onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                title="Refresh"
              >
                <RefreshCw className="w-4 h-4" />
              </button>
            </div>
          </div>

          <div className="p-4">
            {loading ? (
              <div className="py-8 text-center">
                <Loader2 className="w-8 h-8 animate-spin mx-auto text-gray-400" />
                <p className="mt-2 text-gray-500">Loading documents...</p>
              </div>
            ) : documents.length === 0 ? (
              <div className="py-8 text-center text-gray-400">
                <FileText className="w-12 h-12 mx-auto mb-2 opacity-50" />
                <p>No documents yet</p>
                <p className="text-sm mt-1">Upload a PDF to get started</p>
              </div>
            ) : (
              <DocumentList
                documents={documents}
                onDocumentClick={handleDocumentClick}
                onCodeClick={handleCodeClick}
                onRefresh={refetch}
                selectedDocs={selectedDocs}
                setSelectedDocs={setSelectedDocs}
                isGlobalParsing={isGlobalParsing}
                globalParseProgress={parseAllProgress}
                batchDocStatuses={batchDocStatuses}
              />
            )}
          </div>
        </div>
      </main>
      ) : activeTab === 'rules' ? (
        <main className="max-w-7xl mx-auto px-4 py-6">
          <div className="bg-white rounded-lg border h-[calc(100vh-180px)]">
            <RuleGeneration />
          </div>
        </main>
      ) : (
        <main className="max-w-7xl mx-auto px-4 py-6">
          <div className="bg-white rounded-lg border h-[calc(100vh-180px)]">
            <ClaimRules />
          </div>
        </main>
      )}
      
      {/* Document Viewer Modal */}
      {selectedDocId && (
        <DocumentViewer
          docId={selectedDocId}
          initialPage={initialPage}
          highlightCode={highlightCode}
          codeToExpand={codeToExpand}
          onClose={() => {
            setSelectedDocId(null);
            setInitialPage(null);
            setHighlightCode(null);
            setCodeToExpand(null);
          }}
          onCodeClick={handleCodeClick}
        />
      )}
      
      {/* Code Index Modal */}
      {showCodeIndex && (
        <CodeIndexView 
          onDocumentClick={handleDocumentClick}
          onClose={() => setShowCodeIndex(false)}
        />
      )}
    </div>
  );
}