import React, { useState, useEffect, useCallback } from 'react';
import {
  ChevronRight, ChevronDown, CheckCircle, Clock, AlertCircle,
  Loader2, Hash, FileText, Search, RefreshCw, Zap, X, Trash2
} from 'lucide-react';

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
                {/* Color dot instead of icon */}
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
// CODE ITEM (reusable)
// =============================================================================

const CodeItem = ({ code, isSelected, canSelect, generating, onToggleCode, onDeleteRule }) => {
  const hasRule = code.rule_status?.has_rule;
  const isMock = code.rule_status?.is_mock;
  const [deleting, setDeleting] = useState(false);
  
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
  
  return (
    <div
      className={`p-3 rounded-lg border transition-all ${
        isSelected 
          ? 'bg-blue-50 border-blue-300' 
          : hasRule && !isMock
            ? 'bg-green-50 border-green-200' 
            : hasRule && isMock
              ? 'bg-yellow-50 border-yellow-200'
              : 'bg-white border-gray-200 hover:border-gray-300'
      }`}
    >
      <div className="flex items-center gap-3">
        {canSelect && (
          <input
            type="checkbox"
            checked={isSelected}
            onChange={() => onToggleCode(code.code)}
            disabled={generating}
            className="w-4 h-4 text-blue-600 rounded"
          />
        )}
        
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-mono font-medium text-sm">{code.code}</span>
            <span className="text-xs px-1.5 py-0.5 bg-gray-100 rounded text-gray-500">
              {code.type || 'ICD-10'}
            </span>
          </div>
          
          <div className="mt-1 flex items-center gap-3 text-xs text-gray-500">
            <span className="flex items-center gap-1">
              <FileText className="w-3 h-3" />
              {code.documents?.length || 0} docs
            </span>
            <span>{code.total_pages || 0} pages</span>
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
                <div className="flex items-center gap-1 text-green-600">
                  <CheckCircle className="w-4 h-4" />
                  <span className="text-xs">v{code.rule_status.version}</span>
                </div>
                {/* Delete button for real rules */}
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
  generating 
}) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [showMockOnly, setShowMockOnly] = useState(false);
  const [showDiagnoses, setShowDiagnoses] = useState(true);
  const [showProcedures, setShowProcedures] = useState(true);
  
  // Filter function
  const filterCodes = (codes) => codes.filter(c => {
    const matchesSearch = c.code.toLowerCase().includes(searchQuery.toLowerCase());
    const matchesMockFilter = !showMockOnly || (c.rule_status?.has_rule && c.rule_status?.is_mock);
    return matchesSearch && matchesMockFilter;
  });
  
  const filteredDiagnoses = filterCodes(diagnoses);
  const filteredProcedures = filterCodes(procedures);
  const allFiltered = [...filteredDiagnoses, ...filteredProcedures];

  // Codes that can be selected: no rule OR mock rule (for regeneration)
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
          
          {/* Mock filter */}
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

      {/* Code lists - split into Diagnoses and Procedures */}
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
// GENERATION PROGRESS MODAL
// =============================================================================

const GenerationProgress = ({ isOpen, codes, progress, onClose }) => {
  if (!isOpen) return null;

  const steps = ['draft', 'validation', 'arbitration', 'final'];
  const completedCount = Object.values(progress).filter(p => p.status === 'complete').length;
  
  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl w-full max-w-2xl max-h-[80vh] overflow-hidden shadow-2xl">
        <div className="p-4 border-b flex items-center justify-between">
          <h3 className="font-semibold">Generating Rules</h3>
          <button 
            onClick={onClose}
            className="p-1 hover:bg-gray-100 rounded"
          >
            <X className="w-5 h-5" />
          </button>
        </div>
        
        <div className="p-4 overflow-auto max-h-[60vh]">
          {codes.map((code) => {
            const codeProgress = progress[code] || {};
            const currentStep = codeProgress.currentStep || null;
            const status = codeProgress.status || 'pending';
            
            return (
              <div key={code} className="mb-4 last:mb-0">
                <div className="flex items-center gap-2 mb-2">
                  {status === 'complete' ? (
                    <CheckCircle className="w-5 h-5 text-green-500" />
                  ) : status === 'error' ? (
                    <AlertCircle className="w-5 h-5 text-red-500" />
                  ) : status === 'generating' ? (
                    <Loader2 className="w-5 h-5 animate-spin text-blue-500" />
                  ) : (
                    <Clock className="w-5 h-5 text-gray-300" />
                  )}
                  <span className="font-mono font-medium">{code}</span>
                </div>
                
                {/* Steps progress */}
                <div className="ml-7 flex items-center gap-1">
                  {steps.map((step, i) => {
                    const stepIdx = steps.indexOf(currentStep);
                    const isComplete = status === 'complete' || (currentStep && i < stepIdx);
                    const isCurrent = step === currentStep;
                    
                    return (
                      <React.Fragment key={step}>
                        <div 
                          className={`px-2 py-1 rounded text-xs ${
                            isComplete 
                              ? 'bg-green-100 text-green-700' 
                              : isCurrent 
                                ? 'bg-blue-100 text-blue-700' 
                                : 'bg-gray-100 text-gray-400'
                          }`}
                        >
                          {step}
                        </div>
                        {i < steps.length - 1 && (
                          <div className={`w-4 h-0.5 ${isComplete ? 'bg-green-300' : 'bg-gray-200'}`} />
                        )}
                      </React.Fragment>
                    );
                  })}
                </div>
                
                {codeProgress.message && (
                  <p className="ml-7 mt-1 text-xs text-gray-500">{codeProgress.message}</p>
                )}
              </div>
            );
          })}
        </div>
        
        <div className="p-4 border-t bg-gray-50">
          <div className="text-sm text-gray-600">
            {completedCount} / {codes.length} complete
          </div>
        </div>
      </div>
    </div>
  );
};

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

  // Get color for selected category
  const selectedCategoryInfo = categories.find(c => c.name === selectedCategory);
  const categoryColor = selectedCategoryInfo?.color || '#6B7280';

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
        // Deselect all
        const next = new Set(prev);
        codesToSelect.forEach(c => next.delete(c));
        return next;
      } else {
        // Select all
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
        // Refresh data
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
    
    // Initialize progress
    const initialProgress = {};
    codesToGenerate.forEach(code => {
      initialProgress[code] = { status: 'pending', currentStep: null, message: '' };
    });
    setGenerationProgress(initialProgress);
    
    // Generate sequentially
    for (const code of codesToGenerate) {
      setGenerationProgress(prev => ({
        ...prev,
        [code]: { status: 'generating', currentStep: 'draft', message: 'Starting...' }
      }));
      
      try {
        const response = await fetch(`${API_BASE}/generate/${code}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ code, code_type: 'ICD-10' })
        });
        
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          
          const text = decoder.decode(value);
          const lines = text.split('\n').filter(l => l.startsWith('data: '));
          
          for (const line of lines) {
            try {
              const data = JSON.parse(line.slice(6));
              
              if (data.step === 'done') {
                setGenerationProgress(prev => ({
                  ...prev,
                  [code]: { status: 'complete', currentStep: 'final', message: 'Complete' }
                }));
              } else {
                setGenerationProgress(prev => ({
                  ...prev,
                  [code]: { 
                    status: 'generating', 
                    currentStep: data.step, 
                    message: data.message 
                  }
                }));
              }
            } catch (e) {
              console.error('Parse error:', e);
            }
          }
        }
      } catch (err) {
        console.error(`Failed to generate rule for ${code}:`, err);
        setGenerationProgress(prev => ({
          ...prev,
          [code]: { status: 'error', message: err.message }
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
      />
    </div>
  );
}