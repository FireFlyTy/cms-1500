import React, { useState, useEffect, useRef } from 'react';
import { Check, ChevronsUpDown, Search } from 'lucide-react';

const CodeSelector = ({ selectedCode, onSelect }) => {
  const [codes, setCodes] = useState([]);
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(false);

  const wrapperRef = useRef(null);

  useEffect(() => {
    const fetchCodes = async () => {
      setLoading(true);
      try {
        const res = await fetch('http://127.0.0.1:8000/api/codes');
        if (res.ok) {
          const data = await res.json();
          setCodes(data);
        }
      } catch (e) {
        console.error("Failed to fetch codes", e);
      } finally {
        setLoading(false);
      }
    };
    fetchCodes();
  }, []);

  useEffect(() => {
    function handleClickOutside(event) {
      if (wrapperRef.current && !wrapperRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [wrapperRef]);

  const filteredCodes = codes.filter(item =>
    item.label.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className="relative" ref={wrapperRef}>
      <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">
        Target Code
      </label>

      <div
        className="w-full h-[46px] border-2 border-slate-300 rounded-xl px-4 flex items-center justify-between cursor-pointer bg-slate-50 hover:border-teal-400 transition-colors"
        onClick={() => setIsOpen(!isOpen)}
      >
        <span className={selectedCode ? "text-slate-900 font-mono font-bold text-lg" : "text-slate-400 text-sm font-medium"}>
          {selectedCode || "Select code..."}
        </span>
        <ChevronsUpDown size={16} className="text-slate-400" />
      </div>

      {isOpen && (
        <div className="absolute z-50 w-full mt-2 bg-white border border-slate-200 rounded-xl shadow-xl max-h-60 overflow-hidden flex flex-col animate-in fade-in zoom-in-95 duration-100">

          <div className="p-2 border-b border-slate-100 bg-slate-50 flex items-center gap-2">
            <Search size={14} className="text-slate-400" />
            <input
              type="text"
              className="w-full bg-transparent outline-none text-sm text-slate-700 placeholder:text-slate-400"
              placeholder="Search code or description..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              autoFocus
            />
          </div>

          <div className="overflow-y-auto flex-1">
            {loading ? (
                <div className="p-4 text-center text-slate-400 text-xs">Loading codes...</div>
            ) : filteredCodes.length === 0 ? (
                <div className="p-4 text-center text-slate-400 text-xs">No code found.</div>
            ) : (
                filteredCodes.map((item) => (
                  <div
                    key={item.value}
                    className={`px-3 py-2 text-sm cursor-pointer flex items-center justify-between hover:bg-teal-50 transition-colors
                        ${selectedCode === item.value ? 'bg-teal-50 text-teal-900' : 'text-slate-700'}
                    `}
                    onClick={() => {
                      onSelect(item); // Передаем весь объект {value, description, label}
                      setIsOpen(false);
                      setSearch("");
                    }}
                  >
                    <span className="font-mono">{item.label}</span>
                    {selectedCode === item.value && <Check size={14} className="text-teal-600" />}
                  </div>
                ))
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default CodeSelector;