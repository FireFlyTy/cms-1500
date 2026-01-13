import React, { useEffect, useMemo, useCallback, useRef, useState } from "react";
import { Document, Page, pdfjs } from "react-pdf";
import "react-pdf/dist/Page/TextLayer.css";
import "react-pdf/dist/Page/AnnotationLayer.css";
import { Loader2, ZoomIn, ZoomOut } from "lucide-react";

pdfjs.GlobalWorkerOptions.workerSrc = `//unpkg.com/pdfjs-dist@${pdfjs.version}/build/pdf.worker.min.mjs`;

const PAGE_WIDTH_PT = 612;

function escapeRegExp(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Нормализация текста для сравнения
function normalizeText(text) {
  return text
    .toLowerCase()
    .replace(/["""]/g, '"')
    .replace(/\s+/g, ' ')
    .trim();
}

// Очистка поискового запроса от служебных символов
function cleanSearchText(text, currentPage) {
  if (!text) return [];

  const pagePattern = /\],?\s*\[page:\s*(\d+)\s*\|/gi;
  const allParts = [];
  let lastIndex = 0;
  let lastPage = currentPage;
  let match;

  const startMatch = text.match(/^\s*\[?page:\s*(\d+)\s*\|/i);
  if (startMatch) {
    lastPage = parseInt(startMatch[1], 10);
    lastIndex = startMatch[0].length;
  }

  while ((match = pagePattern.exec(text)) !== null) {
    if (lastIndex < match.index) {
      const partText = text.substring(lastIndex, match.index).trim();
      if (partText) {
        allParts.push({ page: lastPage, text: partText });
      }
    }
    lastPage = parseInt(match[1], 10);
    lastIndex = match.index + match[0].length;
  }

  if (lastIndex < text.length) {
    const partText = text.substring(lastIndex).trim();
    if (partText) {
      allParts.push({ page: lastPage, text: partText });
    }
  }

  if (allParts.length === 0) {
    allParts.push({ page: currentPage, text: text });
  }

  const currentPageParts = allParts
    .filter(p => p.page === currentPage)
    .map(p => {
      let cleaned = p.text;
      cleaned = cleaned.replace(/[\[\]|]/g, ' ');
      cleaned = cleaned.replace(/["]/g, '');
      cleaned = cleaned.replace(/\s+/g, ' ').trim();
      return cleaned;
    })
    .filter(t => t.length >= 3);

  return currentPageParts;
}

/**
 * Находит границы параграфа на основе вертикальных gaps между spans
 * Параграф = группа spans с небольшими вертикальными расстояниями между ними
 */
function findParagraphBounds(spanMap, matchStart, matchEnd) {
  // Найти spans которые содержат match
  const matchingIndices = [];
  spanMap.forEach((item, i) => {
    if (item.start < matchEnd && item.end > matchStart) {
      matchingIndices.push(i);
    }
  });

  if (matchingIndices.length === 0) return { start: matchStart, end: matchEnd };

  const firstMatchIdx = matchingIndices[0];
  const lastMatchIdx = matchingIndices[matchingIndices.length - 1];

  // Расширяем назад (ищем начало параграфа)
  let paragraphStartIdx = firstMatchIdx;
  for (let i = firstMatchIdx - 1; i >= 0; i--) {
    const currentSpan = spanMap[i + 1].span;
    const prevSpan = spanMap[i].span;
    
    const currentRect = currentSpan.getBoundingClientRect();
    const prevRect = prevSpan.getBoundingClientRect();
    
    // Вертикальный gap между spans
    const verticalGap = currentRect.top - prevRect.bottom;
    
    // Проверяем: большой gap (>15px) = новый параграф
    // Или: большой indent (начало строки сильно правее) = новый параграф
    const isNewParagraph = verticalGap > 15 || 
                           (verticalGap > 5 && prevRect.left > currentRect.left + 30);
    
    if (isNewParagraph) {
      break;
    }
    paragraphStartIdx = i;
  }

  // Расширяем вперёд (ищем конец параграфа)
  let paragraphEndIdx = lastMatchIdx;
  for (let i = lastMatchIdx + 1; i < spanMap.length; i++) {
    const currentSpan = spanMap[i].span;
    const prevSpan = spanMap[i - 1].span;
    
    const currentRect = currentSpan.getBoundingClientRect();
    const prevRect = prevSpan.getBoundingClientRect();
    
    const verticalGap = currentRect.top - prevRect.bottom;
    const isNewParagraph = verticalGap > 15 || 
                           (verticalGap > 5 && currentRect.left > prevRect.left + 30);
    
    if (isNewParagraph) {
      break;
    }
    paragraphEndIdx = i;
  }

  return {
    startIdx: paragraphStartIdx,
    endIdx: paragraphEndIdx,
    start: spanMap[paragraphStartIdx].start,
    end: spanMap[paragraphEndIdx].end
  };
}

function PdfViewer({ url, pageNumber, searchText }) {
  const containerRef = useRef(null);
  const [zoom, setZoom] = useState(1.0);
  const [containerWidth, setContainerWidth] = useState(700);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(([entry]) => {
      setContainerWidth(entry.contentRect.width);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const effectiveScale = useMemo(() => {
    const fitScale = (containerWidth - 32) / PAGE_WIDTH_PT;
    return Math.max(0.5, Math.min(fitScale * zoom, 3.0));
  }, [containerWidth, zoom]);

  const pageNum = parseInt(pageNumber, 10) || 1;

  const highlightPattern = useCallback(() => {
    if (!searchText?.trim()) return;

    const textLayer = document.querySelector(".react-pdf__Page__textContent");
    if (!textLayer) return;

    const spans = textLayer.querySelectorAll("span");
    if (!spans.length) return;

    // Очистка предыдущих подсветок
    spans.forEach(span => {
      span.classList.remove("pdf-hl", "pdf-hl-paragraph");
      span.style.backgroundColor = '';
      span.style.boxShadow = '';
      span.style.borderLeft = '';
    });

    // Собираем текст и позиции
    let fullText = "";
    const spanMap = [];

    spans.forEach((span, idx) => {
      const text = span.textContent || "";
      const rect = span.getBoundingClientRect();
      const isTiny = rect.width < 10 && rect.height < 10;

      if (isTiny) return;

      const start = fullText.length;
      fullText += text;
      const end = fullText.length;

      spanMap.push({ start, end, span, text, idx, rect });

      if (text && !text.endsWith(' ')) {
        fullText += ' ';
      }
    });

    // Получаем поисковые фразы
    const searchParts = cleanSearchText(searchText, pageNum);
    if (searchParts.length === 0) return;

    const lowerFullText = fullText.toLowerCase();
    let foundMatch = false;

    // Ищем каждую часть
    for (const searchPart of searchParts) {
      const lowerSearch = searchPart.toLowerCase()
        .replace(/["""]/g, '"')
        .replace(/\s+/g, ' ')
        .trim();

      let matchStart = lowerFullText.indexOf(lowerSearch);

      if (matchStart !== -1) {
        const matchEnd = matchStart + lowerSearch.length;
        foundMatch = true;

        console.log(`✅ Найдено: "${lowerSearch.substring(0, 50)}..."`);

        // Подсвечиваем ТОЛЬКО точное совпадение (яркий фон)
        let firstMatchedSpan = null;
        spanMap.forEach(({ start, end, span, text }) => {
          const hasOverlap = (start < matchEnd && end > matchStart);
          if (!hasOverlap) return;

          const overlapStart = Math.max(start, matchStart);
          const overlapEnd = Math.min(end, matchEnd);
          const overlapText = lowerFullText.substring(overlapStart, overlapEnd).trim();

          if (overlapText.length > 2) {
            span.classList.add("pdf-hl");
            span.style.backgroundColor = 'rgba(250, 204, 21, 0.6)'; // Yellow
            span.style.boxShadow = '0 0 0 2px rgba(250, 204, 21, 0.8)';
            if (!firstMatchedSpan) firstMatchedSpan = span;
          }
        });

        // Скроллим к первому совпадению
        if (firstMatchedSpan) {
          setTimeout(() => {
            firstMatchedSpan.scrollIntoView({ behavior: 'smooth', block: 'center' });
          }, 100);
        }

        break; // Нашли - выходим
      }
    }

    // Fallback: если точное совпадение не найдено, ищем по словам
    if (!foundMatch) {
      for (const searchPart of searchParts) {
        const searchWords = searchPart.toLowerCase().split(/\s+/).filter(w => w.length >= 3);
        if (searchWords.length < 2) continue;

        console.log(`🔍 Fallback: ищем по словам:`, searchWords);

        let firstMatchedSpan = null;

        spanMap.forEach(({ span, text, idx }) => {
          const lowerText = text.toLowerCase();
          const matchCount = searchWords.filter(word => lowerText.includes(word)).length;

          if (matchCount >= Math.min(2, searchWords.length)) {
            span.classList.add("pdf-hl");
            span.style.backgroundColor = 'rgba(250, 204, 21, 0.6)';
            span.style.boxShadow = '0 0 0 2px rgba(250, 204, 21, 0.8)';
            
            if (!firstMatchedSpan) firstMatchedSpan = span;
          }
        });

        if (firstMatchedSpan) {
          setTimeout(() => {
            firstMatchedSpan.scrollIntoView({ behavior: 'smooth', block: 'center' });
          }, 100);
          break;
        }
      }
    }
  }, [searchText, pageNumber, effectiveScale]);

  useEffect(() => {
    const t = setTimeout(() => highlightPattern(), 100);
    return () => clearTimeout(t);
  }, [highlightPattern, searchText, pageNumber, effectiveScale, url]);

  return (
    <div ref={containerRef} className="flex flex-col h-full bg-slate-100 relative">
      {/* Toolbar */}
      <div className="absolute top-4 left-1/2 -translate-x-1/2 z-10 flex gap-2 bg-white/90 backdrop-blur shadow-md px-3 py-1.5 rounded-full border border-slate-200 select-none">
        <button
          onClick={() => setZoom((z) => Math.max(0.5, +(z - 0.15).toFixed(2)))}
          className="p-1 hover:bg-slate-100 rounded text-slate-600"
          title="Zoom out"
        >
          <ZoomOut size={16} />
        </button>
        <span className="text-xs font-mono font-bold flex items-center text-slate-500 min-w-[3rem] justify-center">
          {Math.round(zoom * 100)}%
        </span>
        <button
          onClick={() => setZoom((z) => Math.min(3.0, +(z + 0.15).toFixed(2)))}
          className="p-1 hover:bg-slate-100 rounded text-slate-600"
          title="Zoom in"
        >
          <ZoomIn size={16} />
        </button>
      </div>

      {/* PDF */}
      <div className="flex-1 overflow-auto p-4 flex justify-center">
        <Document
          file={url}
          loading={
            <div className="flex items-center gap-2 mt-20 text-slate-400">
              <Loader2 className="animate-spin" /> Loading PDF...
            </div>
          }
          error={<div className="mt-20 text-red-400">Failed to load PDF</div>}
        >
          <Page
            pageNumber={parseInt(pageNumber, 10) || 1}
            scale={effectiveScale}
            renderTextLayer={true}
            renderAnnotationLayer={false}
            className="pdf-page"
            style={{ "--scale-factor": effectiveScale }}
            onRenderTextLayerSuccess={highlightPattern}
          />
        </Document>
      </div>

      {/* CSS для react-pdf и подсветки */}
      <style>{`
        .react-pdf__Page {
          position: relative;
        }
        .react-pdf__Page__canvas {
          display: block;
        }
        .react-pdf__Page__textContent {
          position: absolute !important;
          top: 0 !important;
          left: 0 !important;
          transform-origin: 0 0;
        }
        .react-pdf__Page__textContent span {
          color: transparent !important;
          position: absolute;
          white-space: pre;
          pointer-events: all;
          transform-origin: 0% 0%;
        }
        .react-pdf__Page__textContent span.pdf-hl {
          color: transparent !important;
          background-color: rgba(250, 204, 21, 0.5) !important;
          border-radius: 2px;
        }
        .pdf-hl {
          transition: all 0.2s ease;
        }
      `}</style>
    </div>
  );
}

export default PdfViewer;