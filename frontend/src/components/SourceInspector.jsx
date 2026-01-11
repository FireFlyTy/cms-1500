import React, { useState, useEffect, useMemo } from 'react';
import { FileText, Eye, Code, ChevronUp, ChevronDown, Minus, Maximize2, X, ExternalLink, Loader2, ChevronRight } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

// ✅ Разбивает текст на страницы по "## Page N"
const splitTextByPages = (text) => {
    if (!text) return [];
    const parts = text.split(/(?=## Page \d+)/);
    return parts.filter(p => p.trim());
};

const SourceInspector = ({ fileUrl, parsedText, isLoading = false, onClose }) => {
  const [activeTab, setActiveTab] = useState('split');
  const [pdfSrc, setPdfSrc] = useState(null);
  const [isCollapsed, setIsCollapsed] = useState(false);

  // ✅ Пагинация текста
  const [visiblePagesCount, setVisiblePagesCount] = useState(10); // Показываем первые 10 страниц
  const PAGES_INCREMENT = 20; // Подгружаем по 20 страниц

  // ✅ Разбиваем текст на страницы
  const textPages = useMemo(() => splitTextByPages(parsedText), [parsedText]);

  // ✅ Видимый текст (только первые N страниц)
  const visibleText = useMemo(() => {
      return textPages.slice(0, visiblePagesCount).join('\n\n');
  }, [textPages, visiblePagesCount]);

  const hasMorePages = textPages.length > visiblePagesCount;

  useEffect(() => {
    if (!fileUrl) {
      setActiveTab('text');
      setPdfSrc(null);
    } else {
      if (activeTab === 'text' && !parsedText) setActiveTab('split');
      setPdfSrc(`${fileUrl}#page=1`);
    }
  }, [fileUrl]);

  // ✅ Сброс пагинации при новом тексте
  useEffect(() => {
      setVisiblePagesCount(10);
  }, [parsedText]);

  const jumpToPage = (pageNumber) => {
    if (!fileUrl) return;
    const newSrc = `${fileUrl}#page=${pageNumber}`;
    if (newSrc === pdfSrc) return;
    setPdfSrc(newSrc);
    if (activeTab === 'text') setActiveTab('split');
  };

  const loadMorePages = () => {
      setVisiblePagesCount(prev => prev + PAGES_INCREMENT);
  };

  const extractText = (node) => {
    if (typeof node === 'string' || typeof node === 'number') return node;
    if (Array.isArray(node)) return node.map(extractText).join('');
    if (typeof node === 'object' && node?.props?.children) {
      return extractText(node.props.children);
    }
    return '';
  };

  const MarkdownComponents = {
    h2: ({ node, ...props }) => {
      const fullText = extractText(props.children);
      const match = fullText.match(/Page\s*:?\s*(\d+)/i);

      if (match && fileUrl) {
        const pageNum = match[1];
        return (
          <div onClick={() => jumpToPage(pageNum)} className="group flex items-center gap-3 mt-8 mb-4 p-3 bg-slate-50 border border-slate-200 rounded-xl cursor-pointer hover:bg-teal-50 hover:border-teal-300 transition-all shadow-sm hover:shadow-md">
            <div className="flex items-center justify-center w-8 h-8 bg-white rounded-lg border border-slate-200 text-slate-500 group-hover:text-teal-600 group-hover:border-teal-200"><FileText size={16} /></div>
            <div className="flex flex-col">
                <h2 className="text-sm font-bold text-slate-700 m-0 group-hover:text-teal-800">Page {pageNum}</h2>
                <span className="text-[10px] font-medium text-slate-400 group-hover:text-teal-600 flex items-center gap-1">Click to sync PDF <ExternalLink size={10} /></span>
            </div>
          </div>
        );
      }
      return <h2 className="text-xl font-bold mt-6 mb-3 text-slate-800 border-b border-slate-100 pb-2" {...props} />;
    }
  };

  if (!fileUrl && !parsedText && !isLoading) return null;

  return (
    <div className={`bg-white border border-slate-200 rounded-xl shadow-sm mb-8 overflow-hidden transition-all duration-300 ease-in-out flex flex-col ${isCollapsed ? 'h-[60px]' : 'h-[600px] animate-in fade-in slide-in-from-top-4'}`}>

      {/* HEADER */}
      <div
        className="flex items-center justify-between px-4 py-3 bg-slate-50 border-b border-slate-200 shrink-0 cursor-pointer hover:bg-slate-100 transition-colors"
        onClick={() => setIsCollapsed(!isCollapsed)}
      >
        <div className="flex items-center gap-3" onClick={(e) => e.stopPropagation()}>
          <span className="text-sm font-bold text-slate-600 uppercase tracking-wider flex items-center gap-2">
            <Eye size={16} className="text-teal-600"/>
            Source Inspector
          </span>

          {/* TABS */}
          {!isCollapsed && fileUrl && (
            <div className="flex gap-1 bg-slate-200/50 p-1 rounded-lg ml-4">
              {['split', 'pdf', 'text'].map((tab) => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  className={`px-3 py-1 text-[10px] font-bold uppercase tracking-wider rounded-md flex items-center gap-2 transition-all ${activeTab === tab ? 'bg-white text-teal-700 shadow-sm' : 'text-slate-500 hover:text-slate-700'}`}
                >
                  {tab === 'split' ? "Split" : tab === 'pdf' ? "PDF" : "Text"}
                </button>
              ))}
            </div>
          )}
          {!isCollapsed && !fileUrl && (
             <span className="ml-4 px-2 py-1 bg-slate-200 rounded text-[10px] font-bold text-slate-500 uppercase">Text Mode</span>
          )}

          {/* ✅ Показываем счётчик страниц */}
          {!isCollapsed && textPages.length > 0 && (
              <span className="ml-2 px-2 py-1 bg-teal-50 text-teal-700 rounded text-[10px] font-bold">
                  {visiblePagesCount >= textPages.length ? textPages.length : `${visiblePagesCount}/${textPages.length}`} pages
              </span>
          )}
        </div>

        <div className="flex items-center gap-2" onClick={(e) => e.stopPropagation()}>
            <button
                onClick={() => setIsCollapsed(!isCollapsed)}
                className="text-slate-400 hover:text-teal-600 p-1.5 hover:bg-slate-200 rounded-lg transition-colors"
                title={isCollapsed ? "Expand" : "Collapse"}
            >
                {isCollapsed ? <Maximize2 size={16} /> : <Minus size={16} />}
            </button>

            {onClose && (
                <button
                    onClick={onClose}
                    className="text-slate-400 hover:text-red-500 p-1.5 hover:bg-red-50 rounded-lg transition-colors ml-1"
                    title="Close Inspector"
                >
                    <X size={18} />
                </button>
            )}
        </div>
      </div>

      {/* BODY */}
      {!isCollapsed && (
        <div className="flex-1 flex overflow-hidden relative">

          {/* PDF Area */}
          {fileUrl && (activeTab === 'split' || activeTab === 'pdf') && (
            <div className={`h-full bg-slate-200 relative ${activeTab === 'split' ? 'w-1/2 border-r border-slate-200' : 'w-full'}`}>
              <iframe key={pdfSrc} src={pdfSrc} className="w-full h-full block" title="PDF Preview" />
            </div>
          )}

          {/* Text Area */}
          {(activeTab === 'split' || activeTab === 'text') && (
            <div className={`h-full bg-white overflow-y-auto ${(!fileUrl || activeTab === 'text') ? 'w-full' : 'w-1/2'}`}>
              <div className="p-8 prose prose-sm max-w-none font-mono text-sm text-slate-700 leading-relaxed">

                  {/* ✅ Loading State */}
                  {isLoading && (
                      <div className="flex flex-col items-center justify-center h-full text-teal-600 mt-20">
                          <Loader2 size={48} className="mb-4 animate-spin" />
                          <p className="text-slate-500">Loading document text...</p>
                      </div>
                  )}

                  {/* ✅ Content (пагинированный) */}
                  {!isLoading && visibleText && (
                      <>
                          <ReactMarkdown remarkPlugins={[remarkGfm]} components={MarkdownComponents}>
                              {visibleText}
                          </ReactMarkdown>

                          {/* ✅ Load More Button */}
                          {hasMorePages && (
                              <div className="mt-8 mb-4 flex justify-center">
                                  <button
                                      onClick={loadMorePages}
                                      className="px-6 py-3 bg-teal-50 hover:bg-teal-100 text-teal-700 font-bold rounded-xl border border-teal-200 transition-all flex items-center gap-2"
                                  >
                                      <ChevronDown size={16} />
                                      Load More ({textPages.length - visiblePagesCount} pages remaining)
                                  </button>
                              </div>
                          )}

                          {/* ✅ End marker */}
                          {!hasMorePages && textPages.length > 10 && (
                              <div className="mt-8 mb-4 text-center text-slate-400 text-xs uppercase tracking-wider">
                                  — End of Document ({textPages.length} pages) —
                              </div>
                          )}
                      </>
                  )}

                  {/* ✅ Empty State */}
                  {!isLoading && !visibleText && (
                      <div className="flex flex-col items-center justify-center h-full text-slate-400 mt-20">
                          <FileText size={48} className="mb-4 opacity-20" />
                          <p>Waiting for parsed text...</p>
                      </div>
                  )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default SourceInspector;