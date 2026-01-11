import React, { useState, useEffect } from 'react';
import { X, ShieldCheck, BookOpen, Copy } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import PdfViewer from './PdfViewer'; // <--- Импортируем новый компонент

const ValidationModal = ({ pdfUrl, content, onClose }) => {
  const [currentPage, setCurrentPage] = useState(1);
  const [searchTerm, setSearchTerm] = useState("");
  const [toastMsg, setToastMsg] = useState(null);

  // Сбрасываем при открытии
  useEffect(() => {
    setCurrentPage(1);
    setSearchTerm("");
    document.body.style.overflow = 'hidden';
    return () => { document.body.style.overflow = 'unset'; };
  }, [pdfUrl]);

  const showToast = (msg) => {
      setToastMsg(msg);
      setTimeout(() => setToastMsg(null), 3000);
  };

  const jumpToPage = (pageNumber, quote = "") => {
    // Обновляем страницу
    setCurrentPage(parseInt(pageNumber));

    // Обновляем текст для подсветки
    if (quote) {
        const cleanQuote = quote.replace(/["“”]/g, "").trim();
        setSearchTerm(cleanQuote);

        // Копируем в буфер на всякий случай
        navigator.clipboard.writeText(cleanQuote);
        showToast("Keywords applied & copied");
    } else {
        setSearchTerm("");
    }
  };

  const processTextWithLinks = (text) => {
      if (typeof text !== 'string') return text;

      // ✅ Предобработка: разбиваем склеенные цитаты [[...], [...]] на отдельные [[...]] [[...]]
      let processedText = text.replace(
          /\[\[Page:\s*(\d+)\s*\|\s*"([^"]+)"\s*\],\s*\[Page:\s*(\d+)\s*\|\s*"([^"]+)"\s*\]\]/g,
          '[[Page: $1 | "$2"]] [[Page: $3 | "$4"]]'
      );

      // Обрабатываем случай с 3+ склеенными цитатами (рекурсивно)
      while (processedText.includes('], [Page:')) {
          processedText = processedText.replace(
              /\[\[Page:\s*(\d+)\s*\|\s*"([^"]+)"\s*\],\s*\[Page:/g,
              '[[Page: $1 | "$2"]] [[Page:'
          );
      }

      const regex = /(\[\[Page:\s*([0-9,\s]+)(?:\|\s*"(.*?)")?\]\])/gi;

      let result = [];
      let lastIndex = 0;
      let match;

      while ((match = regex.exec(processedText)) !== null) {
          if (match.index > lastIndex) result.push(processedText.substring(lastIndex, match.index));

          const rawPages = match[2];
          const quote = match[3];
          const pages = rawPages.split(',').map(p => p.trim()).filter(p => p);

          pages.forEach((pageNum, idx) => {
              result.push(
                  <button
                      key={`${match.index}-${idx}`}
                      onClick={(e) => { e.stopPropagation(); jumpToPage(pageNum, quote); }}
                      className="inline-flex items-center gap-1 mx-1 px-1.5 py-0.5 rounded-md bg-teal-100 text-teal-800 text-[10px] font-bold uppercase tracking-wider border border-teal-300 hover:bg-teal-200 hover:text-teal-900 transition-colors cursor-pointer align-middle transform -translate-y-[1px]"
                      title={quote ? `Page ${pageNum}: "${quote}"` : `Page ${pageNum}`}
                  >
                      <BookOpen size={10} /> p.{pageNum}
                  </button>
              );
          });
          lastIndex = regex.lastIndex;
      }
      if (lastIndex < processedText.length) result.push(processedText.substring(lastIndex));
      return result.length > 0 ? result : text;
  };

  const MarkdownComponents = {
      p: ({children}) => <p className="mb-4 last:mb-0 leading-relaxed text-slate-700">{React.Children.map(children, processTextWithLinks)}</p>,
      li: ({children}) => <li className="mb-2 pl-1">{React.Children.map(children, processTextWithLinks)}</li>,
      h1: ({children}) => <h1 className="text-2xl font-bold text-slate-900 mb-4 pb-2 border-b border-slate-200">{children}</h1>,
      h2: ({children}) => <h2 className="text-xl font-bold text-slate-800 mt-6 mb-3">{children}</h2>,
      h3: ({children}) => <h3 className="text-lg font-bold text-slate-800 mt-4 mb-2">{children}</h3>,
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 backdrop-blur-sm p-4 animate-in fade-in duration-200">
      <div className="bg-white w-full h-full max-w-[95vw] max-h-[95vh] rounded-2xl shadow-2xl flex flex-col overflow-hidden border border-slate-200 relative">

        {/* TOAST */}
        {toastMsg && (
            <div className="absolute top-20 left-1/2 -translate-x-1/2 z-50 bg-slate-800 text-white px-4 py-2 rounded-full text-xs font-bold shadow-lg animate-in fade-in slide-in-from-top-2 flex items-center gap-2">
                <Copy size={12} /> {toastMsg}
            </div>
        )}

        {/* HEADER */}
        <div className="px-6 py-4 border-b border-slate-200 flex justify-between items-center bg-slate-50 shrink-0">
            <div>
                <h2 className="text-lg font-bold text-slate-800 flex items-center gap-2">
                    <ShieldCheck size={20} className="text-teal-600"/>
                    Protocol Validation Mode
                </h2>
                <p className="text-xs text-slate-500">Review protocol. Highlighting is automatic.</p>
            </div>
            <button onClick={onClose} className="p-2 hover:bg-slate-200 rounded-full text-slate-500 transition-colors"><X size={24} /></button>
        </div>

        {/* BODY */}
        <div className="flex-1 flex overflow-hidden">
            {/* LEFT: PDF VIEWER (REACT-PDF) */}
            <div className="w-1/2 bg-slate-100 border-r border-slate-200 relative overflow-hidden">
                {pdfUrl ? (
                    <PdfViewer
                        url={pdfUrl}
                        pageNumber={currentPage}
                        searchText={searchTerm} // Передаем текст для подсветки
                    />
                ) : (
                    <div className="flex items-center justify-center h-full text-slate-400 font-bold">PDF Not Available</div>
                )}
            </div>

            {/* RIGHT: TEXT */}
            <div className="w-1/2 bg-white overflow-y-auto p-8 lg:p-12">
                <div className="prose prose-slate max-w-none">
                    <ReactMarkdown remarkPlugins={[remarkGfm]} components={MarkdownComponents}>
                        {content}
                    </ReactMarkdown>
                </div>
            </div>
        </div>
      </div>
    </div>
  );
};

export default ValidationModal;