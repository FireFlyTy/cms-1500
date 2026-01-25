import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Database, FileText, Settings, Search, ChevronDown, ChevronRight,
  CheckCircle, Clock, AlertCircle, X, Eye, Edit, Folder, File,
  Code, Tag, BookOpen, Layers, ExternalLink, Upload, RefreshCw,
  ZoomIn, ZoomOut, ChevronLeft, Hash, Play, Loader2, Filter, FolderSearch,
  Zap, Shield
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

const DocumentViewer = ({ docId, initialPage, highlightCode, onClose, onCodeClick }) => {
  const [document, setDocument] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedPage, setSelectedPage] = useState(initialPage || 1);
  const [zoom, setZoom] = useState(100);
  const [activeTab, setActiveTab] = useState('content');
  const [expandedCode, setExpandedCode] = useState(null);
  const [expandedTopic, setExpandedTopic] = useState(null);
  const [expandedMed, setExpandedMed] = useState(null);
  const [showPageCodes, setShowPageCodes] = useState(false);
  const [showPageTopics, setShowPageTopics] = useState(false);
  const [showPageMeds, setShowPageMeds] = useState(false);
  const [codeSearch, setCodeSearch] = useState('');
  const [searchText, setSearchText] = useState('');  // For PDF text highlighting

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
  const allCodes = document.summary?.all_codes || [];
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
                                        setExpandedCode(c.code);
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
                        placeholder="Search codes, citations..."
                        value={codeSearch}
                        onChange={(e) => setCodeSearch(e.target.value)}
                        className="w-full pl-9 pr-3 py-2 border rounded-lg text-sm"
                      />
                    </div>
                  </div>

                  <div className="flex-1 overflow-auto p-2">
                    {filteredCodes.length === 0 ? (
                      <p className="text-center text-gray-400 py-8">No codes found</p>
                    ) : (
                      <div className="space-y-1">
                        {filteredCodes.map((codeInfo, i) => (
                          <div key={i} className="border rounded">
                            <button
                              onClick={() => setExpandedCode(expandedCode === codeInfo.code ? null : codeInfo.code)}
                              className="w-full px-3 py-2 flex items-center justify-between hover:bg-gray-50 text-left"
                            >
                              <div className="flex items-center gap-2">
                                <span className="font-mono text-sm font-medium">{codeInfo.code}</span>
                                <span className="text-xs px-1.5 py-0.5 bg-gray-100 rounded text-gray-500">{codeInfo.type}</span>
                              </div>
                              <div className="flex items-center gap-2">
                                <span className="text-xs text-gray-400">{codeInfo.pages?.length || 0} pages</span>
                                <ChevronDown className={`w-4 h-4 text-gray-400 transition-transform ${expandedCode === codeInfo.code ? 'rotate-180' : ''}`} />
                              </div>
                            </button>
                            {/* Inline page buttons with anchor support */}
                            {codeInfo.pages?.length > 0 && (
                              <div className="px-3 py-1.5 border-t bg-gray-50/50 flex flex-wrap gap-1">
                                {codeInfo.pages?.slice(0, expandedCode === codeInfo.code ? undefined : 10).map(pageNum => {
                                  // Find anchor for this page
                                  const anchor = codeInfo.anchors?.find(a => a.page === pageNum);
                                  // Support both old format (text) and new format (start/end)
                                  const anchorText = anchor?.text || anchor?.start;
                                  const hasAnchor = !!anchorText;
                                  // Build search text: use range format if we have start+end
                                  const searchValue = anchor?.start && anchor?.end
                                    ? `[RANGE]${anchor.start}|||${anchor.end}[/RANGE]`
                                    : anchorText;
                                  return (
                                    <button
                                      key={pageNum}
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        setSelectedPage(pageNum);
                                        // Set anchor text for highlighting in PDF
                                        if (searchValue) {
                                          setSearchText(searchValue);
                                        } else {
                                          // Fallback: use context or code itself
                                          setSearchText(codeInfo.contexts?.[0] || codeInfo.code);
                                        }
                                      }}
                                      title={hasAnchor ? `📍 "${anchorText}"` : 'Click to view page (no citation anchor)'}
                                      className={`px-2 py-0.5 text-xs rounded transition-colors ${
                                        selectedPage === pageNum
                                          ? 'bg-blue-600 text-white'
                                          : hasAnchor
                                            ? 'bg-yellow-50 border border-yellow-300 hover:bg-yellow-100'
                                            : 'bg-white border hover:bg-blue-50 hover:border-blue-300'
                                      }`}
                                    >
                                      p.{pageNum}
                                      {hasAnchor && <span className="ml-1 text-yellow-600">•</span>}
                                    </button>
                                  );
                                })}
                                {!expandedCode !== codeInfo.code && codeInfo.pages?.length > 10 && (
                                  <span className="text-xs text-gray-400 px-1">+{codeInfo.pages.length - 10}</span>
                                )}
                              </div>
                            )}
                            {/* Expanded: show anchors/citations */}
                            {expandedCode === codeInfo.code && (
                              <div className="px-3 py-2 bg-gray-50 border-t space-y-2">
                                {codeInfo.contexts?.[0] && (
                                  <p className="text-xs text-gray-500 italic">
                                    {codeInfo.contexts[0]}
                                  </p>
                                )}
                                {codeInfo.anchors?.length > 0 && (
                                  <div className="space-y-1">
                                    <p className="text-xs font-medium text-gray-600">Citations:</p>
                                    {codeInfo.anchors.map((anchor, idx) => {
                                      // Support both old format (text) and new format (start/end)
                                      const anchorText = anchor.text || anchor.start;
                                      const displayText = anchor.text
                                        ? `"${anchor.text}"`
                                        : anchor.start && anchor.end
                                          ? `"${anchor.start}" ... "${anchor.end}"`
                                          : anchor.start
                                            ? `"${anchor.start}"`
                                            : '';
                                      // Build search text: use range format if we have start+end
                                      const searchValue = anchor.start && anchor.end
                                        ? `[RANGE]${anchor.start}|||${anchor.end}[/RANGE]`
                                        : anchorText;
                                      return (
                                        <button
                                          key={idx}
                                          onClick={() => {
                                            setSelectedPage(anchor.page);
                                            setSearchText(searchValue);
                                          }}
                                          className="block w-full text-left text-xs px-2 py-1 rounded bg-yellow-50 border border-yellow-200 hover:bg-yellow-100 transition-colors"
                                        >
                                          <span className="text-gray-400">p.{anchor.page}:</span>{' '}
                                          <span className="text-gray-700">{displayText}</span>
                                        </button>
                                      );
                                    })}
                                  </div>
                                )}
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    )}
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

                        return (
                          <div key={i} className="border rounded">
                            <button
                              onClick={() => setExpandedTopic(expandedTopic === topicName ? null : topicName)}
                              className="w-full px-3 py-2 flex items-center justify-between hover:bg-gray-50 text-left"
                            >
                              <span className="text-sm">{topicName}</span>
                              <div className="flex items-center gap-2">
                                <span className="text-xs text-gray-400">{topicPages.length} pages</span>
                                {topicAnchors.length > 0 && <span className="text-yellow-500 text-xs">•</span>}
                                <ChevronDown className={`w-4 h-4 text-gray-400 transition-transform ${expandedTopic === topicName ? 'rotate-180' : ''}`} />
                              </div>
                            </button>
                            {/* Page buttons */}
                            {topicPages.length > 0 && (
                              <div className="px-3 py-1.5 border-t bg-gray-50/50 flex flex-wrap gap-1">
                                {topicPages.map(pageNum => {
                                  const anchor = topicAnchors.find(a => a.page === pageNum);
                                  const anchorText = anchor?.text || anchor?.start;
                                  const hasAnchor = !!anchorText;
                                  // Build search text: use range format if we have start+end
                                  const searchValue = anchor?.start && anchor?.end
                                    ? `[RANGE]${anchor.start}|||${anchor.end}[/RANGE]`
                                    : anchorText;
                                  return (
                                    <button
                                      key={pageNum}
                                      onClick={() => {
                                        setSelectedPage(pageNum);
                                        if (searchValue) {
                                          setSearchText(searchValue);
                                        } else {
                                          setSearchText(topicName);
                                        }
                                      }}
                                      title={hasAnchor ? `📍 "${anchorText}"` : `Search for "${topicName}"`}
                                      className={`px-2 py-0.5 text-xs rounded transition-colors ${
                                        selectedPage === pageNum
                                          ? 'bg-blue-600 text-white'
                                          : hasAnchor
                                            ? 'bg-yellow-50 border border-yellow-300 hover:bg-yellow-100'
                                            : 'bg-white border hover:bg-blue-50'
                                      }`}
                                    >
                                      p.{pageNum}
                                      {hasAnchor && <span className="ml-1 text-yellow-600">•</span>}
                                    </button>
                                  );
                                })}
                              </div>
                            )}
                            {/* Expanded: show citations */}
                            {expandedTopic === topicName && topicAnchors.length > 0 && (
                              <div className="px-3 py-2 bg-blue-50 border-t space-y-1">
                                <p className="text-xs font-medium text-blue-600">Citations:</p>
                                {topicAnchors.map((anchor, idx) => {
                                  const anchorText = anchor.text || anchor.start;
                                  const displayText = anchor.start && anchor.end
                                    ? `"${anchor.start}" ... "${anchor.end}"`
                                    : anchor.start
                                      ? `"${anchor.start}"`
                                      : anchor.text
                                        ? `"${anchor.text}"`
                                        : '';
                                  // Build search text: use range format if we have start+end
                                  const searchValue = anchor.start && anchor.end
                                    ? `[RANGE]${anchor.start}|||${anchor.end}[/RANGE]`
                                    : anchorText;
                                  return (
                                    <button
                                      key={idx}
                                      onClick={() => {
                                        setSelectedPage(anchor.page);
                                        setSearchText(searchValue);
                                      }}
                                      className="block w-full text-left text-xs px-2 py-1 rounded bg-white border border-blue-200 hover:bg-blue-100 transition-colors"
                                    >
                                      <span className="text-gray-400">p.{anchor.page}:</span>{' '}
                                      <span className="text-gray-700">{displayText}</span>
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

                        return (
                          <div key={i} className="border rounded">
                            <button
                              onClick={() => setExpandedMed(expandedMed === medName ? null : medName)}
                              className="w-full px-3 py-2 flex items-center justify-between hover:bg-gray-50 text-left"
                            >
                              <span className="text-sm font-medium text-emerald-700">{medName}</span>
                              <div className="flex items-center gap-2">
                                <span className="text-xs text-gray-400">{medPages.length} pages</span>
                                {medAnchors.length > 0 && <span className="text-yellow-500 text-xs">•</span>}
                                <ChevronDown className={`w-4 h-4 text-gray-400 transition-transform ${expandedMed === medName ? 'rotate-180' : ''}`} />
                              </div>
                            </button>
                            {/* Page buttons */}
                            {medPages.length > 0 && (
                              <div className="px-3 py-1.5 border-t bg-gray-50/50 flex flex-wrap gap-1">
                                {medPages.map(pageNum => {
                                  const anchor = medAnchors.find(a => a.page === pageNum);
                                  const anchorText = anchor?.text || anchor?.start;
                                  const hasAnchor = !!anchorText;
                                  // Build search text: use range format if we have start+end
                                  const searchValue = anchor?.start && anchor?.end
                                    ? `[RANGE]${anchor.start}|||${anchor.end}[/RANGE]`
                                    : anchorText;
                                  return (
                                    <button
                                      key={pageNum}
                                      onClick={() => {
                                        setSelectedPage(pageNum);
                                        if (searchValue) {
                                          setSearchText(searchValue);
                                        } else {
                                          setSearchText(medName);
                                        }
                                      }}
                                      title={hasAnchor ? `📍 "${anchorText}"` : `Search for "${medName}"`}
                                      className={`px-2 py-0.5 text-xs rounded transition-colors ${
                                        selectedPage === pageNum
                                          ? 'bg-blue-600 text-white'
                                          : hasAnchor
                                            ? 'bg-yellow-50 border border-yellow-300 hover:bg-yellow-100'
                                            : 'bg-white border hover:bg-blue-50'
                                      }`}
                                    >
                                      p.{pageNum}
                                      {hasAnchor && <span className="ml-1 text-yellow-600">•</span>}
                                    </button>
                                  );
                                })}
                              </div>
                            )}
                            {/* Expanded: show citations */}
                            {expandedMed === medName && medAnchors.length > 0 && (
                              <div className="px-3 py-2 bg-emerald-50 border-t space-y-1">
                                <p className="text-xs font-medium text-emerald-600">Citations:</p>
                                {medAnchors.map((anchor, idx) => {
                                  const anchorText = anchor.text || anchor.start;
                                  const displayText = anchor.start && anchor.end
                                    ? `"${anchor.start}" ... "${anchor.end}"`
                                    : anchor.start
                                      ? `"${anchor.start}"`
                                      : anchor.text
                                        ? `"${anchor.text}"`
                                        : '';
                                  // Build search text: use range format if we have start+end
                                  const searchValue = anchor.start && anchor.end
                                    ? `[RANGE]${anchor.start}|||${anchor.end}[/RANGE]`
                                    : anchorText;
                                  return (
                                    <button
                                      key={idx}
                                      onClick={() => {
                                        setSelectedPage(anchor.page);
                                        setSearchText(searchValue);
                                      }}
                                      className="block w-full text-left text-xs px-2 py-1 rounded bg-white border border-emerald-200 hover:bg-emerald-100 transition-colors"
                                    >
                                      <span className="text-gray-400">p.{anchor.page}:</span>{' '}
                                      <span className="text-gray-700">{displayText}</span>
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

const CodeIndexView = ({ onDocumentClick, onClose }) => {
  const { codes, loading } = useCodes();
  const [searchQuery, setSearchQuery] = useState('');
  const [filterType, setFilterType] = useState('all');
  const [selectedCode, setSelectedCode] = useState(null);
  const [codeDetails, setCodeDetails] = useState(null);

  useEffect(() => {
    if (selectedCode) {
      fetch(`${API_BASE}/codes/${encodeURIComponent(selectedCode)}`)
        .then(res => res.json())
        .then(setCodeDetails)
        .catch(() => setCodeDetails(null));
    }
  }, [selectedCode]);

  const filteredCodes = codes.filter(c => {
    // Null checks
    if (!c || !c.code) return false;

    if (filterType !== 'all' && c.type !== filterType) return false;
    if (searchQuery) {
      const query = searchQuery.toLowerCase();
      const code = (c.code || '').toLowerCase();
      if (!code.includes(query)) return false;
    }
    return true;
  });

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl w-full max-w-5xl h-[80vh] flex flex-col overflow-hidden shadow-2xl">
        {/* Header */}
        <div className="px-4 py-3 border-b flex items-center justify-between bg-slate-50">
          <div className="flex items-center gap-3">
            <Code className="w-5 h-5 text-blue-600" />
            <h2 className="font-semibold text-gray-900">Code Index</h2>
            <span className="text-sm text-gray-500">{codes.length} codes</span>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-gray-200 rounded-lg">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 flex overflow-hidden">
          {/* Left: List */}
          <div className="w-1/3 border-r flex flex-col">
            <div className="p-3 border-b space-y-2">
              <div className="relative">
                <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder="Search codes..."
                  className="w-full pl-9 pr-3 py-2 border rounded-lg text-sm"
                />
              </div>
              <div className="flex gap-1">
                {['all', 'ICD-10', 'HCPCS', 'CPT'].map(type => (
                  <button
                    key={type}
                    onClick={() => setFilterType(type)}
                    className={`px-2 py-1 rounded text-xs font-medium transition-colors ${
                      filterType === type ? 'bg-blue-600 text-white' : 'bg-gray-100 hover:bg-gray-200'
                    }`}
                  >
                    {type === 'all' ? 'All' : type}
                  </button>
                ))}
              </div>
            </div>

            <div className="flex-1 overflow-auto">
              {loading ? (
                <div className="p-4 text-center">
                  <Loader2 className="w-6 h-6 animate-spin mx-auto text-gray-400" />
                </div>
              ) : filteredCodes.length === 0 ? (
                <div className="p-4 text-center text-gray-400 text-sm">No codes found</div>
              ) : (
                filteredCodes.map((code, idx) => (
                  <button
                    key={`${code.code}-${idx}`}
                    onClick={() => setSelectedCode(code.code)}
                    className={`w-full px-4 py-3 border-b text-left hover:bg-gray-50 transition-colors ${
                      selectedCode === code.code ? 'bg-blue-50 border-l-4 border-l-blue-600' : ''
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-mono font-medium text-sm">{code.code}</span>
                      <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${
                        code.type === 'ICD-10' ? 'bg-gray-100 text-gray-600' :
                        code.type === 'HCPCS' ? 'bg-gray-100 text-gray-600' :
                        'bg-gray-100 text-gray-600'
                      }`}>
                        {code.type}
                      </span>
                    </div>
                    <div className="text-xs text-gray-500 mt-1">
                      {code.documents?.[0]?.count || code.documents?.length || 0} document{(code.documents?.[0]?.count || code.documents?.length || 0) !== 1 ? 's' : ''}
                    </div>
                  </button>
                ))
              )}
            </div>
          </div>

          {/* Right: Details */}
          <div className="flex-1 overflow-auto">
            {codeDetails ? (
              <div className="p-4">
                <div className="mb-6">
                  <div className="flex items-center gap-3 mb-2">
                    <h3 className="text-2xl font-mono font-bold">{codeDetails.code}</h3>
                    <span className={`px-2 py-1 rounded text-xs font-medium ${
                      codeDetails.type === 'ICD-10' ? 'bg-gray-100 text-gray-600' :
                      codeDetails.type === 'HCPCS' ? 'bg-gray-100 text-gray-600' :
                      'bg-gray-100 text-gray-600'
                    }`}>
                      {codeDetails.type}
                    </span>
                  </div>
                </div>

                <div className="space-y-4">
                  {codeDetails.documents?.map(doc => (
                    <div key={doc.id} className="border rounded-lg overflow-hidden">
                      <div className="p-3 bg-slate-50 flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <DocTypeIcon docType={doc.doc_type} />
                          <span className="font-medium text-sm">{doc.filename}</span>
                        </div>
                        <button
                          onClick={() => onDocumentClick(doc.id)}
                          className="text-xs text-blue-600 hover:underline flex items-center gap-1"
                        >
                          Open <ExternalLink className="w-3 h-3" />
                        </button>
                      </div>
                      <div className="p-3 space-y-2">
                        {doc.pages?.map((pageInfo, i) => {
                          // Build highlight text from anchor (prefer start/end format, fallback to text or context)
                          const anchor = pageInfo.anchor;
                          let highlightText = null;
                          if (anchor?.start && anchor?.end) {
                            highlightText = `[RANGE]${anchor.start}|||${anchor.end}[/RANGE]`;
                          } else if (anchor?.text) {
                            highlightText = anchor.text;
                          } else if (anchor?.start) {
                            highlightText = anchor.start;
                          } else if (pageInfo.context) {
                            highlightText = pageInfo.context;
                          }

                          return (
                            <button
                              key={i}
                              onClick={() => onDocumentClick(doc.id, pageInfo.page, highlightText)}
                              className="w-full text-left hover:bg-blue-50 p-2 rounded-lg transition-colors group"
                            >
                              <div className="flex items-center gap-2">
                                <span className="text-xs font-medium text-blue-600 group-hover:underline">
                                  Page {pageInfo.page}
                                </span>
                                {anchor && <span className="text-yellow-500 text-xs">•</span>}
                                <ChevronRight className="w-3 h-3 text-gray-300 group-hover:text-blue-400" />
                              </div>
                              {anchor?.start && anchor?.end ? (
                                <p className="text-xs italic text-gray-500 mt-1 truncate">
                                  "{anchor.start}" ... "{anchor.end}"
                                </p>
                              ) : anchor?.text ? (
                                <p className="text-xs italic text-gray-500 mt-1 truncate">
                                  "{anchor.text}"
                                </p>
                              ) : pageInfo.context ? (
                                <p className="text-xs italic text-gray-500 mt-1 truncate">
                                  "{pageInfo.context}"
                                </p>
                              ) : null}
                              {pageInfo.content_preview && (
                                <p className="text-xs text-gray-400 mt-1 truncate">{pageInfo.content_preview}</p>
                              )}
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ) : (
              <div className="h-full flex items-center justify-center text-gray-400">
                <div className="text-center">
                  <Hash className="w-12 h-12 mx-auto mb-2 opacity-50" />
                  <p>Select a code to view details</p>
                </div>
              </div>
            )}
          </div>
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

  const handleDocumentClick = (docId, page = null, highlightCode = null) => {
    setSelectedDocId(docId);
    setInitialPage(page);
    setHighlightCode(highlightCode);
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
          onClose={() => {
            setSelectedDocId(null);
            setInitialPage(null);
            setHighlightCode(null);
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