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

// Нормализация текста для сравнения (включая лигатуры)
function normalizeText(text) {
  return text
    .toLowerCase()
    // Лигатуры → обычные буквы
    .replace(/ﬁ/g, 'fi')
    .replace(/ﬂ/g, 'fl')
    .replace(/ﬀ/g, 'ff')
    .replace(/ﬃ/g, 'ffi')
    .replace(/ﬄ/g, 'ffl')
    .replace(/ﬆ/g, 'st')
    .replace(/Ꜳ/g, 'aa')
    .replace(/ꜳ/g, 'aa')
    .replace(/["""]/g, '"')
    .replace(/['']/g, "'")
    .replace(/[–—]/g, '-')
    .replace(/\s+/g, ' ')
    .trim();
}

// Parse range format: [RANGE]start|||end[/RANGE]
function parseRangeSearch(text) {
  if (!text) return null;
  const rangeMatch = text.match(/\[RANGE\](.*?)\|\|\|(.*?)\[\/RANGE\]/);
  if (rangeMatch) {
    return {
      type: 'range',
      start: rangeMatch[1].trim(),
      end: rangeMatch[2].trim()
    };
  }
  return null;
}

// Очистка поискового запроса от служебных символов
function cleanSearchText(text, currentPage) {
  if (!text) return [];

  // Check for range format first
  const rangeSearch = parseRangeSearch(text);
  if (rangeSearch) {
    return []; // Range searches are handled separately
  }

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
      span.classList.remove("pdf-hl", "pdf-hl-paragraph", "pdf-hl-range");
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

    const lowerFullText = fullText.toLowerCase();

    // Check for range search format
    const rangeSearch = parseRangeSearch(searchText);
    if (rangeSearch) {
      const lowerStart = rangeSearch.start.toLowerCase().replace(/\s+/g, ' ').trim();
      const lowerEnd = rangeSearch.end.toLowerCase().replace(/\s+/g, ' ').trim();

      console.log(`🔍 Range search: "${lowerStart.substring(0, 30)}..." to "${lowerEnd.substring(0, 30)}..."`);

      // Helper: make regex pattern that handles ligatures
      const makeLigaturePattern = (word) => {
        return word
          .replace(/[-.*+?^${}()|[\]\\]/g, '\\$&')
          .replace(/fi/gi, '(fi|ﬁ)')
          .replace(/fl/gi, '(fl|ﬂ)')
          .replace(/ff/gi, '(ff|ﬀ)')
          .replace(/ffi/gi, '(ffi|ﬃ)')
          .replace(/ffl/gi, '(ffl|ﬄ)');
      };

      // Helper: fuzzy find position using first N significant words
      const fuzzyFind = (text, searchStr, afterPos = 0) => {
        const lowerText = text.toLowerCase();
        const lowerSearch = searchStr.toLowerCase().replace(/\s+/g, ' ').trim();

        // First try exact match
        let pos = lowerText.indexOf(lowerSearch, afterPos);
        if (pos !== -1) return { pos, len: lowerSearch.length, exact: true };

        // Try with ligature-aware regex for exact phrase
        try {
          const exactPattern = makeLigaturePattern(lowerSearch);
          const exactRegex = new RegExp(exactPattern, 'i');
          const searchArea = lowerText.substring(afterPos);
          const exactMatch = searchArea.match(exactRegex);
          if (exactMatch) {
            return { pos: afterPos + exactMatch.index, len: exactMatch[0].length, exact: true };
          }
        } catch (e) {}

        // Try with just first 25 chars
        const shortSearch = lowerSearch.substring(0, 25).trim();
        try {
          const shortPattern = makeLigaturePattern(shortSearch);
          const shortRegex = new RegExp(shortPattern, 'i');
          const searchArea = lowerText.substring(afterPos);
          const shortMatch = searchArea.match(shortRegex);
          if (shortMatch) {
            return { pos: afterPos + shortMatch.index, len: shortMatch[0].length, exact: true };
          }
        } catch (e) {}

        // Fuzzy: use first 3-4 significant words only
        const words = searchStr.split(/\s+/).filter(w => w.length >= 4).slice(0, 4);
        if (words.length < 2) {
          const shortWords = searchStr.split(/\s+/).filter(w => w.length >= 3).slice(0, 3);
          if (shortWords.length >= 2) {
            words.length = 0;
            words.push(...shortWords);
          }
        }
        if (words.length < 2) return null;

        console.log(`  Fuzzy words: [${words.join(', ')}]`);

        // Build regex pattern with ligature support
        const pattern = words.map(w => makeLigaturePattern(w.toLowerCase())).join('[\\s\\S]{0,100}');
        try {
          const regex = new RegExp(pattern, 'i');
          const searchArea = lowerText.substring(afterPos);
          const match = searchArea.match(regex);

          if (match) {
            return { pos: afterPos + match.index, len: match[0].length, exact: false };
          }
        } catch (e) {
          console.log('Regex error:', e);
        }
        return null;
      };

      // Helper: normalize text by replacing ligatures with normal letters
      const normalizeLigatures = (text) => {
        return text
          .replace(/ﬁ/g, 'fi')
          .replace(/ﬂ/g, 'fl')
          .replace(/ﬀ/g, 'ff')
          .replace(/ﬃ/g, 'ffi')
          .replace(/ﬄ/g, 'ffl')
          .replace(/ﬆ/g, 'st');
      };

      // Helper: find ALL occurrences of a pattern
      const findAllOccurrences = (text, searchStr, debug = false) => {
        const results = [];
        const lowerSearch = searchStr.toLowerCase().replace(/\s+/g, ' ').trim();
        // Normalize the text to replace ligatures
        const normalizedText = normalizeLigatures(text);

        if (debug) {
          console.log(`  findAll: searching for "${lowerSearch.substring(0, 40)}..."`);
          // Find ALL occurrences of "modi" to see where modification appears
          if (lowerSearch.includes('modif')) {
            let idx = 0;
            let count = 0;
            while ((idx = normalizedText.indexOf('modi', idx)) !== -1 && count < 5) {
              console.log(`    "modi" #${count+1} at ${idx}: "${normalizedText.substring(idx, idx + 40)}"`);
              idx += 4;
              count++;
            }
            // Also show where "cvd" is
            const cvdIdx = normalizedText.indexOf('cvd');
            if (cvdIdx !== -1) {
              console.log(`    "cvd" at ${cvdIdx}: "${normalizedText.substring(cvdIdx - 20, cvdIdx + 30)}"`);
            }
          }
        }

        // Search in NORMALIZED text (ligatures replaced with normal chars)
        // PDF may split words across spans, so we need flexible matching

        // Helper: create regex that allows optional spaces within words
        const makeFlexiblePattern = (str) => {
          // First escape special regex characters
          const escaped = str.replace(/[-.*+?^${}()|[\]\\]/g, '\\$&');
          // Insert \s* between each character to handle word splits
          // But keep real spaces as \s+
          return escaped.split(' ').map(word =>
            word.split('').join('\\s*')
          ).join('\\s+');
        };

        // Try exact matches first (fast path)
        let pos = 0;
        while ((pos = normalizedText.indexOf(lowerSearch, pos)) !== -1) {
          results.push({ pos, len: lowerSearch.length, exact: true });
          pos += 1;
        }
        if (debug && results.length) console.log(`    exact: ${results.length} matches`);

        // Try flexible pattern (allows spaces within words)
        if (results.length === 0) {
          try {
            const flexPattern = makeFlexiblePattern(lowerSearch);
            if (debug) console.log(`    flexible pattern: ${flexPattern.substring(0, 60)}...`);
            const regex = new RegExp(flexPattern, 'gi');
            let match;
            while ((match = regex.exec(normalizedText)) !== null) {
              if (!results.some(r => Math.abs(r.pos - match.index) < 5)) {
                results.push({ pos: match.index, len: match[0].length, exact: true });
              }
            }
            if (debug) console.log(`    flexible: ${results.length} matches`);
          } catch (e) { if (debug) console.log(`    flexible error: ${e.message}`); }
        }

        // Try first 15 chars with flexible pattern
        const shortSearch = lowerSearch.substring(0, 15).trim();
        if (shortSearch.length >= 8 && results.length === 0) {
          try {
            const flexPattern = makeFlexiblePattern(shortSearch);
            const regex = new RegExp(flexPattern, 'gi');
            let match;
            while ((match = regex.exec(normalizedText)) !== null) {
              if (!results.some(r => Math.abs(r.pos - match.index) < 5)) {
                results.push({ pos: match.index, len: match[0].length, exact: false });
              }
            }
            if (debug) console.log(`    short flexible "${shortSearch}": ${results.length} matches`);
          } catch (e) {}
        }

        // Fuzzy: handle broken words (like "modi cation" for "modification")
        // Search for word fragments that may have spaces or missing ligatures
        if (results.length === 0) {
          // Clean punctuation from words
          const words = searchStr.split(/\s+/)
            .map(w => w.replace(/[^a-zA-Z0-9-]/g, ''))
            .filter(w => w.length >= 3)
            .slice(0, 3);
          if (debug) console.log(`    fuzzy words: [${words.join(', ')}]`);

          // Make pattern that handles broken ligatures: "modification" -> "modi.?\\s*.?cation"
          const makeRobustPattern = (word) => {
            const w = word.toLowerCase().replace(/[-.*+?^${}()|[\]\\]/g, '\\$&');
            // Handle common ligature breaks: fi, fl, ff -> allow space and optional missing char
            return w
              .replace(/fi/g, 'f?i?\\s*')  // fi might become "f i", " i", "fi", etc
              .replace(/fl/g, 'f?l?\\s*')
              .replace(/ff/g, 'f?f?\\s*');
          };

          if (words.length >= 2) {
            const pattern = words.map(w => makeRobustPattern(w)).join('[\\s\\S]{0,100}');
            if (debug) console.log(`    robust pattern: ${pattern.substring(0, 80)}...`);
            try {
              const regex = new RegExp(pattern, 'gi');
              let match;
              while ((match = regex.exec(normalizedText)) !== null) {
                if (!results.some(r => Math.abs(r.pos - match.index) < 10)) {
                  results.push({ pos: match.index, len: match[0].length, exact: false });
                }
              }
              if (debug) console.log(`    robust matches: ${results.length}`);
            } catch (e) { if (debug) console.log(`    robust regex error:`, e.message); }
          }
        }

        // Last resort: find distinctive word fragment
        if (results.length === 0) {
          // For "modification", search for "modi" followed by "cation"
          // Clean word from punctuation first
          const firstWord = searchStr.split(/\s+/)
            .map(w => w.replace(/[^a-zA-Z0-9]/g, ''))
            .find(w => w.length >= 6);
          if (firstWord) {
            const word = firstWord.toLowerCase();
            // Split at potential ligature points and search
            const fragments = [word.substring(0, 4)]; // First 4 chars
            if (word.length > 6) fragments.push(word.substring(word.length - 5)); // Last 5 chars

            if (debug) console.log(`    fragment search: [${fragments.join(', ')}]`);

            const pattern = fragments.map(f => f.replace(/[-.*+?^${}()|[\]\\]/g, '\\$&')).join('[\\s\\S]{0,10}');
            try {
              const regex = new RegExp(pattern, 'gi');
              let match;
              while ((match = regex.exec(normalizedText)) !== null) {
                if (!results.some(r => Math.abs(r.pos - match.index) < 10)) {
                  results.push({ pos: match.index, len: match[0].length, exact: false });
                }
              }
              if (debug) console.log(`    fragment matches: ${results.length}`);
            } catch (e) { if (debug) console.log(`    fragment error: ${e.message}`); }
          }
        }

        if (debug) console.log(`    final: ${results.length} matches`);

        return results;
      };

      // Find all start and end anchor occurrences
      const startMatches = findAllOccurrences(lowerFullText, lowerStart, true);
      const endMatches = findAllOccurrences(lowerFullText, lowerEnd, true);

      console.log(`  Found ${startMatches.length} start matches, ${endMatches.length} end matches`);

      // Find best pair: end after start position, minimal distance
      let bestPair = null;
      let bestDistance = Infinity;
      const MAX_DISTANCE = 10000;

      for (const start of startMatches) {
        for (const end of endMatches) {
          // End must START after start STARTS (allow overlap)
          const distance = end.pos - start.pos;
          if (distance > 0 && distance <= MAX_DISTANCE && distance < bestDistance) {
            bestDistance = distance;
            bestPair = { start, end };
          }
        }
      }

      // Fallback: if no valid pair, try with even more relaxed constraints
      if (!bestPair && startMatches.length > 0 && endMatches.length > 0) {
        for (const start of startMatches) {
          for (const end of endMatches) {
            // Just require end.pos + end.len > start.pos (any overlap is ok)
            const rangeLen = (end.pos + end.len) - start.pos;
            if (rangeLen > 10 && rangeLen < bestDistance) {
              bestDistance = rangeLen;
              bestPair = { start, end };
            }
          }
        }
      }

      const startMatch = bestPair?.start || startMatches[0] || null;
      const endMatch = bestPair?.end || null;

      if (bestPair) {
        console.log(`  Best pair: distance=${bestDistance} chars`);
      }

      if (startMatch && endMatch) {
        const rangeStart = startMatch.pos;
        const rangeEnd = endMatch.pos + endMatch.len;

        console.log(`✅ Range found: positions ${rangeStart} to ${rangeEnd} (exact: ${startMatch.exact}/${endMatch.exact})`);

        // Highlight all spans in the range
        let firstMatchedSpan = null;
        spanMap.forEach(({ start, end, span }) => {
          const hasOverlap = (start < rangeEnd && end > rangeStart);
          if (!hasOverlap) return;

          span.classList.add("pdf-hl", "pdf-hl-range");
          span.style.backgroundColor = 'rgba(250, 180, 0, 0.75)';
          span.style.boxShadow = '0 0 0 2px rgba(250, 150, 0, 1)';

          if (!firstMatchedSpan) firstMatchedSpan = span;
        });

        // Scroll to first match
        if (firstMatchedSpan) {
          setTimeout(() => {
            firstMatchedSpan.scrollIntoView({ behavior: 'smooth', block: 'center' });
          }, 100);
        }
        return;
      } else if (startMatch) {
        // Fallback: highlight from start to end of paragraph
        console.log(`⚠️ End anchor not found, highlighting from start`);
        const rangeStart = startMatch.pos;

        // Find paragraph bounds using existing function
        const bounds = findParagraphBounds(spanMap, rangeStart, rangeStart + startMatch.len);

        let firstMatchedSpan = null;
        spanMap.forEach(({ start, end, span }, idx) => {
          const inParagraph = idx >= bounds.startIdx && idx <= bounds.endIdx;
          if (!inParagraph) return;

          span.classList.add("pdf-hl", "pdf-hl-range");
          span.style.backgroundColor = 'rgba(250, 180, 0, 0.75)';
          span.style.boxShadow = '0 0 0 2px rgba(250, 150, 0, 1)';
          if (!firstMatchedSpan) firstMatchedSpan = span;
        });

        if (firstMatchedSpan) {
          setTimeout(() => {
            firstMatchedSpan.scrollIntoView({ behavior: 'smooth', block: 'center' });
          }, 100);
        }
        return;
      } else {
        console.log(`❌ Start anchor not found, trying word-based search`);
        // Will fall through to regular search
      }
    }

    // Regular search (non-range)
    const searchParts = cleanSearchText(searchText, pageNum);
    if (searchParts.length === 0) return;

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
            span.style.backgroundColor = 'rgba(250, 180, 0, 0.85)'; // Yellow
            span.style.boxShadow = '0 0 0 3px rgba(250, 150, 0, 1)';
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
        // Clean punctuation from words for better matching
        const searchWords = searchPart.toLowerCase()
          .split(/\s+/)
          .map(w => w.replace(/[^a-z0-9-]/g, ''))  // Remove punctuation
          .filter(w => w.length >= 3);
        if (searchWords.length < 2) continue;

        console.log(`🔍 Fallback: ищем по словам:`, searchWords);

        let firstMatchedSpan = null;

        spanMap.forEach(({ span, text, idx }) => {
          const lowerText = text.toLowerCase();
          const matchCount = searchWords.filter(word => lowerText.includes(word)).length;

          if (matchCount >= Math.min(2, searchWords.length)) {
            span.classList.add("pdf-hl");
            span.style.backgroundColor = 'rgba(250, 180, 0, 0.85)';
            span.style.boxShadow = '0 0 0 3px rgba(250, 150, 0, 1)';

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
          background-color: rgba(250, 204, 21, 0.85) !important;
          border-radius: 2px;
          box-shadow: 0 0 0 2px rgba(250, 204, 21, 1) !important;
        }
        .react-pdf__Page__textContent span.pdf-hl-range {
          color: transparent !important;
          background-color: rgba(250, 180, 0, 0.7) !important;
          border-radius: 0;
          box-shadow: 0 0 0 1px rgba(250, 180, 0, 0.9) !important;
        }
        .pdf-hl, .pdf-hl-range {
          transition: all 0.2s ease;
        }
      `}</style>
    </div>
  );
}

export default PdfViewer;