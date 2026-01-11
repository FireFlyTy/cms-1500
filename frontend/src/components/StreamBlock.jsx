import React, { useState, useEffect, useMemo } from 'react';
import {
  ChevronDown, ChevronRight, Loader2, BrainCircuit,
  FileText, Search, ShieldCheck, ListTodo, Gavel,
  BookOpen, CircleDashed, ClipboardList
} from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

const analyzeVerdict = (text, title) => {
  if (!text) return null;
  const upperText = text.toUpperCase();

  const getCount = (keyword) => {
      const regex = new RegExp(`${keyword}[:\\s]*(\\d+)`, 'i');
      const match = text.match(regex);
      return match ? parseInt(match[1], 10) : 0;
  };

  if (title && (title.includes("Arbitrator") || title.includes("Final"))) {
      let count = getCount("CORRECTION_COUNT");
      if (!count) count = getCount("Corrections Applied");
      if (!count) count = getCount("Citations Resolved");

      if (count > 0) return `${count} CORRECTION${count !== 1 ? 'S' : ''} APPLIED`;

      if (upperText.includes("STATUS: VERIFIED")) return "PROTOCOL VERIFIED";
      if (upperText.includes("STATUS: SECURE") || upperText.includes("STATUS: SAFE")) return "PROTOCOL APPROVED";
  }

  const risks = getCount("RISK_COUNT");
  if (risks > 0) return `${risks} RISK${risks !== 1 ? 'S' : ''} FOUND`;

  const suggestions = getCount("SUGGESTION_COUNT");
  if (suggestions > 0) return `${suggestions} SUGGESTION${suggestions !== 1 ? 'S' : ''}`;

  if (upperText.includes("STATUS: SAFE") || upperText.includes("STATUS: COMPLIANT")) return "NO ISSUES";

  return null;
};

/**
 * Splits content into main content and appendix in real-time
 * Triggers: "# APPENDIX", "## APPENDIX", "## A. TRACEABILITY", "TRACEABILITY LOG"
 */
const splitContentAndAppendix = (content) => {
  if (!content) return { main: '', appendix: '', isStreaming: false };

  // Patterns to detect appendix start (case-insensitive, multiline)
  const appendixPatterns = [
    /(?:^|\n)(#{1,2}\s*APPENDIX)/i,
    /(?:^|\n)(#{1,2}\s*A\.\s*TRACEABILITY)/i,
    /(?:^|\n)(#{1,2}\s*TRACEABILITY\s*LOG)/i,
  ];

  for (const pattern of appendixPatterns) {
    const match = content.match(pattern);
    if (match) {
      const splitIndex = content.indexOf(match[1]);
      return {
        main: content.substring(0, splitIndex).trim(),
        appendix: content.substring(splitIndex).trim(),
        isStreaming: true
      };
    }
  }

  return { main: content, appendix: '', isStreaming: false };
};

const StreamBlock = ({ title, status, thoughts, content, isParallel = false, shouldCollapse = false, variant = 'gray', onJump, onValidate }) => {
  const [showThoughts, setShowThoughts] = useState(false);
  const [isCollapsed, setIsCollapsed] = useState(false);
  const [showAppendix, setShowAppendix] = useState(false);

  const verdictLabel = useMemo(() => analyzeVerdict(content, title), [content, title]);

  // Split content into main and appendix - works during streaming too
  const { main: mainContent, appendix: appendixContent, isStreaming: hasAppendixStarted } = useMemo(
    () => splitContentAndAppendix(content),
    [content]
  );

  // Auto-expand appendix when it starts streaming (optional - can remove if annoying)
  useEffect(() => {
    if (hasAppendixStarted && status === 'loading' && !showAppendix) {
      // Uncomment to auto-show appendix during streaming:
      // setShowAppendix(true);
    }
  }, [hasAppendixStarted, status]);

  useEffect(() => { if (status === 'loading') setIsCollapsed(false); }, [status]);
  useEffect(() => { if (shouldCollapse && status === 'done') setIsCollapsed(true); }, [shouldCollapse, status]);

  const themes = {
      gray: { border: 'border-slate-200', bgHeader: 'bg-slate-50', textHeader: 'text-slate-700', icon: <FileText className="text-slate-400 w-5 h-5" />, badgeBg: 'bg-slate-200 text-slate-600' },
      blue: { border: 'border-blue-200', bgHeader: 'bg-blue-50/80 hover:bg-blue-100/50', textHeader: 'text-blue-900', icon: <Search className="text-blue-600 w-5 h-5" />, badgeBg: 'bg-blue-100 text-blue-700' },
      green: { border: 'border-emerald-200', bgHeader: 'bg-emerald-50/80 hover:bg-emerald-50', textHeader: 'text-emerald-900', icon: <ShieldCheck className="text-emerald-600 w-5 h-5" />, badgeBg: 'bg-emerald-100 text-emerald-800' }
  };

  const currentTheme = themes[variant] || themes.gray;

  let displayIcon = currentTheme.icon;
  if (status === 'loading') displayIcon = <Loader2 className="animate-spin text-teal-600 w-5 h-5" />;
  if (status === 'idle') displayIcon = <CircleDashed className="text-slate-300 w-5 h-5" />;

  const hasItems = verdictLabel && (
      verdictLabel.includes('RISK') ||
      verdictLabel.includes('SUGGESTION') ||
      verdictLabel.includes('CORRECTION')
  );

  // --- SAFE TEXT RENDERER ---
  const processStringWithLinks = (text) => {
      if (!text) return null;

      // Preprocessing: split merged citations [[...], [...]] into separate [[...]] [[...]]
      let processedText = text.replace(
          /\[\[Page:\s*(\d+)\s*\|\s*"([^"]+)"\s*\],\s*\[Page:\s*(\d+)\s*\|\s*"([^"]+)"\s*\]\]/g,
          '[[Page: $1 | "$2"]] [[Page: $3 | "$4"]]'
      );

      // Handle 3+ merged citations recursively
      while (processedText.includes('], [Page:')) {
          processedText = processedText.replace(
              /\[\[Page:\s*(\d+)\s*\|\s*"([^"]+)"\s*\],\s*\[Page:/g,
              '[[Page: $1 | "$2"]] [[Page:'
          );
      }

      const parts = processedText.split(/(\[\[Pages?:[^\]]+\]\])/g);

      return parts.map((part, i) => {
          // Match both [[Page: N | "..."]] and [[Pages: N-M | "..."]]
          const match = part.match(/\[\[Pages?:\s*([0-9,\s\-]+)(?:\s*[|,]\s*"(.*?)")?\]\]/);
          if (match) {
              const rawPages = match[1];
              const quote = match[2];

              // Parse pages: "95" or "95-96" or "95, 96"
              let pages = [];
              if (rawPages.includes('-')) {
                  const [start, end] = rawPages.split('-').map(p => p.trim());
                  pages = [start, end];
              } else {
                  pages = rawPages.split(',').map(p => p.trim()).filter(p => p);
              }

              return (
                  <span key={i} className="inline-flex flex-wrap gap-1 mx-1 align-middle">
                      {pages.map((pageNum, idx) => (
                          <button
                              key={`${i}-${idx}`}
                              onClick={(e) => {
                                  e.stopPropagation();
                                  if (onJump) onJump(pageNum, quote);
                              }}
                              className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-teal-50 text-teal-700 text-[10px] font-bold uppercase tracking-wider border border-teal-200 hover:bg-teal-100 hover:text-teal-800 transition-colors cursor-pointer"
                              title={quote ? `Page ${pageNum}: "${quote}"` : `Page ${pageNum}`}
                          >
                              <BookOpen size={10} /> p.{pageNum}
                          </button>
                      ))}
                  </span>
              );
          }
          return part;
      });
  };

  const renderContent = (children) => {
      return React.Children.map(children, (child) => {
          if (typeof child === 'string') return processStringWithLinks(child);
          if (React.isValidElement(child) && child.props.children) {
              return React.cloneElement(child, { ...child.props, children: renderContent(child.props.children) });
          }
          return child;
      });
  };

  const MarkdownComponents = {
      p: ({children}) => <p className="mb-2 last:mb-0 leading-relaxed text-slate-800">{renderContent(children)}</p>,
      li: ({children}) => <li className="mb-1 leading-relaxed text-slate-800 list-disc ml-4">{renderContent(children)}</li>,
      strong: ({children}) => <strong className="font-bold text-slate-900">{renderContent(children)}</strong>,
      em: ({children}) => <em className="italic text-slate-700">{renderContent(children)}</em>,
      h1: ({children}) => <h1 className="text-xl font-bold mt-4 mb-2">{renderContent(children)}</h1>,
      h2: ({children}) => <h2 className="text-lg font-bold mt-3 mb-2">{renderContent(children)}</h2>,
      h3: ({children}) => <h3 className="text-base font-bold mt-2 mb-1">{renderContent(children)}</h3>,
  };

  // Smaller markdown components for appendix
  const AppendixMarkdownComponents = {
      ...MarkdownComponents,
      p: ({children}) => <p className="mb-1.5 last:mb-0 leading-relaxed text-slate-600 text-sm">{renderContent(children)}</p>,
      li: ({children}) => <li className="mb-0.5 leading-relaxed text-slate-600 text-sm list-disc ml-4">{renderContent(children)}</li>,
      h1: ({children}) => <h1 className="text-base font-bold mt-3 mb-1.5 text-slate-700">{renderContent(children)}</h1>,
      h2: ({children}) => <h2 className="text-sm font-bold mt-2 mb-1 text-slate-700">{renderContent(children)}</h2>,
      h3: ({children}) => <h3 className="text-sm font-semibold mt-1.5 mb-1 text-slate-600">{renderContent(children)}</h3>,
      table: ({children}) => <table className="text-xs border-collapse w-full my-2">{children}</table>,
      th: ({children}) => <th className="border border-slate-200 bg-slate-50 px-2 py-1 text-left font-semibold text-slate-600">{children}</th>,
      td: ({children}) => <td className="border border-slate-200 px-2 py-1 text-slate-600">{children}</td>,
  };

  return (
    <div className={`border ${currentTheme.border} rounded-xl mb-4 shadow-sm transition-all overflow-hidden ${isParallel ? 'h-full flex flex-col' : ''} bg-white`}>
      <div
        className={`p-4 flex items-center justify-between cursor-pointer transition-colors ${currentTheme.bgHeader}`}
        onClick={() => setIsCollapsed(!isCollapsed)}
      >
        <div className="flex items-center gap-3">
          {displayIcon}
          <div className="flex flex-col">
              <span className={`font-semibold text-sm md:text-base ${status === 'idle' ? 'text-slate-400' : currentTheme.textHeader}`}>
                {title}
              </span>

              {isCollapsed && verdictLabel && status === 'done' && (
                  <div className="flex items-center mt-1">
                      <span className={`text-[10px] font-bold uppercase px-2 py-0.5 rounded-full tracking-wide flex items-center gap-1 ${currentTheme.badgeBg}`}>
                          {hasItems && <ListTodo size={10} />}
                          {verdictLabel}
                      </span>
                  </div>
              )}
          </div>
        </div>

        <div className="flex items-center gap-3">
            {onValidate && status === 'done' && (
                <button
                    onClick={(e) => { e.stopPropagation(); onValidate(); }}
                    className="hidden md:flex items-center gap-2 px-3 py-1.5 bg-white border border-teal-200 text-teal-700 text-xs font-bold uppercase tracking-wider rounded-lg hover:bg-teal-50 hover:border-teal-300 transition-all shadow-sm"
                >
                    <ShieldCheck size={14} /> Validate
                </button>
            )}
            <div className="text-slate-400">
                {isCollapsed ? <ChevronRight size={20} /> : <ChevronDown size={20} />}
            </div>
        </div>
      </div>

      {!isCollapsed && (
        <div className="flex-1 flex flex-col">

          {thoughts && status !== 'idle' && (
            <div className="border-b border-gray-100 bg-slate-50">
              <button
                onClick={(e) => { e.stopPropagation(); setShowThoughts(!showThoughts); }}
                className="w-full text-left px-4 py-2 text-xs font-bold text-slate-500 flex items-center gap-2 hover:text-slate-700 transition-colors uppercase tracking-wider"
              >
                <BrainCircuit size={14} />
                {showThoughts ? "Hide Reasoning" : "Show Reasoning"}
                <span className="ml-auto font-normal normal-case opacity-50">{thoughts.length} chars</span>
              </button>
              {showThoughts && (
                <div className="p-4 pt-0 border-t border-slate-100 animate-in slide-in-from-top-2 max-h-96 overflow-y-auto bg-slate-50">
                  <div className="prose prose-xs max-w-none font-mono text-slate-600">
                      <ReactMarkdown remarkPlugins={[remarkGfm]}>{thoughts}</ReactMarkdown>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Main Content */}
          <div className="p-5 bg-white min-h-[100px] flex-1">
            {status === 'idle' ? (
                <div className="flex items-center gap-2 text-slate-300 italic font-mono text-sm">
                    <CircleDashed size={16} /> Ready to start...
                </div>
            ) : mainContent ? (
                <div className="prose prose-sm max-w-none text-slate-800">
                    <ReactMarkdown
                        remarkPlugins={[remarkGfm]}
                        components={MarkdownComponents}
                    >
                        {mainContent}
                    </ReactMarkdown>
                </div>
            ) : content ? (
                <div className="prose prose-sm max-w-none text-slate-800">
                    <ReactMarkdown
                        remarkPlugins={[remarkGfm]}
                        components={MarkdownComponents}
                    >
                        {content}
                    </ReactMarkdown>
                </div>
            ) : (
                <div className="flex items-center gap-2 text-teal-600 animate-pulse font-mono text-sm">
                    <Loader2 size={16} className="animate-spin" /> Generating...
                </div>
            )}
          </div>

          {/* Appendix Section (collapsible) - NOW SHOWS DURING STREAMING */}
          {appendixContent && (
            <div className="border-t border-slate-200 bg-slate-50/50">
              <button
                onClick={(e) => { e.stopPropagation(); setShowAppendix(!showAppendix); }}
                className="w-full text-left px-4 py-2.5 text-xs font-bold text-slate-500 flex items-center gap-2 hover:text-slate-700 hover:bg-slate-100 transition-colors uppercase tracking-wider"
              >
                <ClipboardList size={14} />
                {showAppendix ? "Hide Technical Details" : "Show Technical Details"}
                <span className="ml-auto text-[10px] font-normal normal-case text-slate-400 flex items-center gap-2">
                  Traceability & Validation
                  {status === 'loading' && (
                    <Loader2 size={12} className="animate-spin text-teal-500" />
                  )}
                </span>
                {showAppendix ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
              </button>

              {showAppendix && (
                <div className="px-5 py-4 border-t border-slate-200 bg-slate-50 animate-in slide-in-from-top-2 max-h-[400px] overflow-y-auto">
                  <div className="prose prose-xs max-w-none text-slate-600">
                    <ReactMarkdown
                      remarkPlugins={[remarkGfm]}
                      components={AppendixMarkdownComponents}
                    >
                      {appendixContent}
                    </ReactMarkdown>
                    {status === 'loading' && (
                      <div className="flex items-center gap-2 text-teal-500 text-xs mt-2">
                        <Loader2 size={12} className="animate-spin" /> Generating...
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          )}

          {status === 'done' && verdictLabel && (
            <div className={`p-2 px-5 text-xs font-bold uppercase tracking-widest text-right border-t border-slate-100 text-slate-400`}>
                Analysis Result: <span className="text-slate-700">{verdictLabel}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default StreamBlock;