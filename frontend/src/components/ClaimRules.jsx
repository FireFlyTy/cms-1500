import React, { useState, useEffect, useCallback } from 'react';
import {
  ChevronRight, ChevronDown, CheckCircle, Clock, AlertCircle,
  Loader2, Hash, FileText, Search, RefreshCw, Zap, X, Eye,
  BookOpen, Shield, Database, ArrowRight, GitBranch
} from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

const API_BASE = 'http://localhost:8001/api/rules';

// =============================================================================
// HOOKS
// =============================================================================

function useCategories() {
  const [data, setData] = useState({ categories: [], total_codes: 0, total_with_rules: 0 });
  const [loading, setLoading] = useState(true);

  const fetch_ = useCallback(async () => {
    setLoading(true);
    try {
      // Use rule_type=cms to count CMS rules (not guideline rules)
      const res = await fetch(`${API_BASE}/categories?rule_type=cms`);
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

function useCategoryCodesWithCMS(categoryName) {
  const [data, setData] = useState({
    diagnoses: [],
    procedures: [],
    total: 0
  });
  const [loading, setLoading] = useState(false);

  const fetchCodes = useCallback(async () => {
    if (!categoryName) return;

    setLoading(true);
    try {
      // Fetch codes from category - already includes rule_status with has_cms1500
      const res = await fetch(`${API_BASE}/categories/${encodeURIComponent(categoryName)}/codes`);
      const json = await res.json();

      // Map rule_status to cms_rule/guideline_rule format expected by components
      const mapCode = (code) => ({
        ...code,
        cms_rule: {
          has_rule: code.rule_status?.has_cms1500 || false,
          rule_id: code.rule_status?.cms1500_rule_id,
          version: code.rule_status?.cms_version
        },
        guideline_rule: {
          has_rule: code.rule_status?.has_rule || false,
          rule_id: code.rule_status?.rule_id,
          version: code.rule_status?.guideline_version
        }
      });

      setData({
        diagnoses: (json.diagnoses || []).map(mapCode),
        procedures: (json.procedures || []).map(mapCode),
        total: (json.diagnoses?.length || 0) + (json.procedures?.length || 0)
      });
    } catch (err) {
      console.error('Failed to fetch codes:', err);
    } finally {
      setLoading(false);
    }
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
        <Loader2 className="w-6 h-6 animate-spin text-teal-600" />
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
    <div className="space-y-1.5">
      {categories.map(cat => {
        const isSelected = selectedCategory === cat.name;
        const codesWithRules = cat.codes_with_rules || 0;
        const totalCodes = cat.total_codes || 0;
        const coveragePercent = totalCodes > 0 ? Math.round((codesWithRules / totalCodes) * 100) : 0;

        return (
          <button
            key={cat.name}
            onClick={() => onSelectCategory(cat.name)}
            className="w-full text-left p-3 rounded-lg transition-all"
            style={{
              background: isSelected ? '#eef4fa' : 'white',
              border: '1px solid',
              borderColor: isSelected ? '#5d8bb8' : '#e5e7eb',
              borderLeft: isSelected ? '4px solid #5d8bb8' : '4px solid transparent'
            }}
            onMouseEnter={(e) => {
              if (!isSelected) {
                e.currentTarget.style.background = '#f9fafb';
                e.currentTarget.style.borderLeftColor = '#9cb8d4';
              }
            }}
            onMouseLeave={(e) => {
              if (!isSelected) {
                e.currentTarget.style.background = 'white';
                e.currentTarget.style.borderLeftColor = 'transparent';
              }
            }}
          >
            <div className="flex items-center justify-between">
              <span className="font-medium text-xs uppercase tracking-wide" style={{ color: '#1a1a1a' }}>{cat.name}</span>
              <ChevronRight
                className="w-4 h-4 transition-transform"
                style={{
                  color: '#9ca3af',
                  transform: isSelected ? 'rotate(90deg)' : 'rotate(0deg)'
                }}
              />
            </div>

            {/* Coverage progress bar */}
            <div className="mt-2 flex items-center gap-2">
              <div
                className="flex-1 h-1.5 rounded-full overflow-hidden"
                style={{ background: '#e5e7eb' }}
              >
                <div
                  className="h-full rounded-full transition-all"
                  style={{
                    width: `${coveragePercent}%`,
                    background: coveragePercent === 100 ? '#4878a8' : coveragePercent > 0 ? '#5d8bb8' : '#e5e7eb'
                  }}
                />
              </div>
              <span
                className="text-xs font-mono"
                style={{ color: '#6b7280', minWidth: '45px', textAlign: 'right' }}
              >
                {codesWithRules}/{totalCodes}
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

const CodeItem = ({ code, isSelected, generating, onToggleCode, onGenerate, onViewRule }) => {
  const hasCmsRule = code.cms_rule?.has_rule;
  const hasGuidelineRule = code.guideline_rule?.has_rule || code.rule_status?.has_rule;
  const cmsVersion = code.cms_rule?.version;

  // Determine code type for NCCI availability
  const codeType = code.type || 'ICD-10';
  const hasNcci = codeType === 'CPT' || codeType === 'HCPCS';
  const hasSources = hasGuidelineRule || hasNcci;

  // Can select: has sources (for generation or regeneration)
  const canSelect = hasSources;

  // Unified card: white background, left border indicates status
  const borderLeftColor = hasCmsRule ? '#059669' : hasSources ? '#0090DA' : '#d1d5db';

  return (
    <div
      className="rounded-lg transition-all"
      style={{
        background: isSelected ? '#f0fdfa' : 'white',
        border: '1px solid #e5e7eb',
        borderLeft: `4px solid ${borderLeftColor}`
      }}
    >
      <div className="flex items-center gap-3 p-3">
        {/* Checkbox for ALL codes with sources (including generated - for regeneration) */}
        {canSelect ? (
          <input
            type="checkbox"
            checked={isSelected}
            onChange={() => onToggleCode(code.code)}
            disabled={generating}
            className="w-4 h-4 text-teal-600 rounded"
            onClick={(e) => e.stopPropagation()}
          />
        ) : (
          // Placeholder for alignment when no checkbox
          <div className="w-4 h-4" />
        )}

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-mono font-medium text-sm" style={{ color: '#1a1a1a' }}>{code.code}</span>
            <span
              className="text-xs px-1.5 py-0.5 rounded"
              style={{ background: '#f3f4f6', color: '#6b7280' }}
            >
              {codeType}
            </span>
          </div>

          {/* Source indicators */}
          <div className="mt-1.5 flex items-center gap-3 text-xs">
            {/* Guideline source */}
            <span className="flex items-center gap-1" style={{ color: hasGuidelineRule ? '#0090DA' : '#9ca3af' }}>
              <BookOpen className="w-3 h-3" />
              Guideline
              {hasGuidelineRule && <CheckCircle className="w-3 h-3" style={{ color: '#059669' }} />}
            </span>

            {/* NCCI source */}
            <span className="flex items-center gap-1" style={{ color: hasNcci ? '#7c3aed' : '#9ca3af' }}>
              <Database className="w-3 h-3" />
              NCCI
              {hasNcci && <CheckCircle className="w-3 h-3" style={{ color: '#059669' }} />}
            </span>
          </div>
        </div>

        {/* Action zone - right side */}
        <div className="flex items-center gap-2 shrink-0">
          {hasCmsRule ? (
            <>
              <button
                onClick={() => onViewRule(code.code)}
                className="px-3 py-1.5 text-xs rounded flex items-center gap-1.5 transition-colors"
                style={{
                  color: '#059669',
                  background: 'transparent',
                  border: '1px solid #059669'
                }}
                onMouseEnter={(e) => e.currentTarget.style.background = '#f0fdf4'}
                onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                title="View CMS rule"
              >
                <Eye className="w-3.5 h-3.5" />
                View
              </button>
              <span className="text-xs font-medium px-2 py-1 rounded" style={{ background: '#f0fdf4', color: '#059669' }}>
                v{cmsVersion}
              </span>
              {/* Regenerate button for existing rules */}
              <button
                onClick={() => onGenerate(code.code, true)}
                disabled={generating}
                className="px-2 py-1.5 text-xs rounded flex items-center gap-1 transition-colors"
                style={{
                  color: generating ? '#9ca3af' : '#6b7280',
                  background: 'transparent',
                  border: generating ? '1px solid #d1d5db' : '1px solid #d1d5db',
                  cursor: generating ? 'not-allowed' : 'pointer'
                }}
                onMouseEnter={(e) => !generating && (e.currentTarget.style.background = '#f3f4f6')}
                onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                title="Regenerate CMS rule"
              >
                <RefreshCw className="w-3 h-3" />
              </button>
            </>
          ) : hasSources ? (
            <button
              onClick={() => onGenerate(code.code)}
              disabled={generating}
              className="px-3 py-1.5 text-xs rounded flex items-center gap-1.5 transition-colors"
              style={{
                color: generating ? '#9ca3af' : '#0090DA',
                background: 'transparent',
                border: generating ? '1px solid #d1d5db' : '1px solid #0090DA',
                cursor: generating ? 'not-allowed' : 'pointer'
              }}
              onMouseEnter={(e) => !generating && (e.currentTarget.style.background = '#eff6ff')}
              onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
              title="Generate CMS rule"
            >
              <Zap className="w-3.5 h-3.5" />
              Generate
            </button>
          ) : (
            <span className="text-xs" style={{ color: '#9ca3af' }}>No sources</span>
          )}
        </div>
      </div>
    </div>
  );
};

// =============================================================================
// CMS RULE VIEWER
// =============================================================================

// Field name mapping - technical to friendly (CMS-1500 form fields)
const FIELD_LABELS = {
  // Diagnosis codes (Box 21)
  'diagnosisCodes[].code': 'Any Diagnosis',
  'diagnosisCodes[0].code': 'Primary Diagnosis',
  'diagnosisCodes[1].code': '2nd Diagnosis',
  'diagnosisCodes[2].code': '3rd Diagnosis',
  'diagnosisCodes[3].code': '4th Diagnosis',
  'diagnosisCodes': 'Diagnosis Codes',

  // Procedure/Service lines (Box 24)
  'procedureCodes[].code': 'Any Procedure',
  'procedureCodes[0].code': 'Primary Procedure',
  'procedureCodes[1].code': '2nd Procedure',
  'serviceLines[].procedureCode': 'Any Procedure',
  'serviceLines[].modifier': 'Any Modifier',
  'serviceLines[].modifiers': 'Modifiers',
  'serviceLines[].units': 'Service Units',
  'serviceLines[].charges': 'Line Charges',
  'serviceLines[].placeOfService': 'Place of Service',
  'serviceLines[].dateOfService': 'Service Date',
  'serviceLines[].diagnosisPointer': 'Diagnosis Pointer',

  // Patient info (Boxes 2, 3, 5)
  'patientAge': 'Patient Age',
  'patientDOB': 'Patient Date of Birth',
  'patientGender': 'Patient Gender',
  'patientSex': 'Patient Sex',
  'patientName': 'Patient Name',
  'patientAddress': 'Patient Address',

  // Provider info (Boxes 17, 24J, 31, 32, 33)
  'renderingProvider': 'Rendering Provider',
  'renderingProviderNPI': 'Rendering Provider NPI',
  'referringProvider': 'Referring Provider',
  'referringProviderNPI': 'Referring Provider NPI',
  'billingProvider': 'Billing Provider',
  'billingProviderNPI': 'Billing Provider NPI',
  'facilityName': 'Facility Name',
  'facilityNPI': 'Facility NPI',

  // Insurance info (Boxes 1, 4, 9, 11)
  'payerType': 'Payer Type',
  'payerId': 'Payer ID',
  'insuranceType': 'Insurance Type',
  'insuredName': 'Insured Name',
  'insuredId': 'Insured ID',
  'groupNumber': 'Group Number',

  // Dates (Boxes 14, 15, 16, 18)
  'dateOfIllness': 'Date of Illness',
  'dateOfSimilarIllness': 'Similar Illness Date',
  'hospitalizationDates': 'Hospitalization Dates',
  'admissionDate': 'Admission Date',
  'dischargeDate': 'Discharge Date',

  // Other common fields
  'placeOfService': 'Place of Service',
  'typeOfService': 'Type of Service',
  'modifier': 'Modifier',
  'modifiers': 'Modifiers',
  'units': 'Units',
  'totalCharges': 'Total Charges',
  'amountPaid': 'Amount Paid',
  'priorAuthNumber': 'Prior Auth Number',
  'claimFrequencyCode': 'Claim Frequency',
  'acceptAssignment': 'Accept Assignment',
};

// Operator mapping - technical to friendly
const OP_LABELS = {
  'contains': 'includes',
  'contains_pattern': 'matches',
  'not_contains': 'excludes',
  'not_contains_pattern': 'does not match',
  'equals': 'is',
  'not_equals': 'is not',
  'greater_than': '>',
  'less_than': '<',
  'greater_than_or_equal': '≥',
  'less_than_or_equal': '≤',
  'in': 'is one of',
  'not_in': 'is not one of',
};

// Clean up pattern values for display (E08\..*  → E08.*)
const formatValue = (value, op) => {
  if (!value) return value;
  // Handle arrays - join with comma
  if (Array.isArray(value)) {
    return value.join(', ');
  }
  if (typeof value !== 'string') return String(value);

  // Clean regex escapes for display
  let display = value
    .replace(/\\\./g, '.')  // \. → .
    .replace(/\.\*/g, '*')  // .* → *
    .replace(/\.\+/g, '+'); // .+ → +

  // If it ends with % (SQL-like wildcard), keep it
  // If it ends with * (cleaned regex), that's good too
  return display;
};

// Single check renderer - professional style
const CheckItem = ({ check, depth = 0 }) => {
  // If check has nested operator (like NOT, OR, AND), render recursively
  if (check.operator && check.checks) {
    const opConfig = {
      'AND': { label: 'All of:', color: '#1E40AF', bg: '#EFF6FF' },
      'OR': { label: 'Any of:', color: '#92400E', bg: '#FFFBEB' },
      'NOT': { label: 'None of:', color: '#991B1B', bg: '#FEF2F2' },
    };
    const config = opConfig[check.operator] || { label: check.operator, color: '#374151', bg: '#F3F4F6' };

    return (
      <div className={`${depth > 0 ? 'ml-4 pl-3' : ''}`} style={{ borderLeft: depth > 0 ? '2px solid #E5E7EB' : 'none' }}>
        <div
          className="text-xs font-semibold px-2 py-0.5 rounded inline-block mb-1"
          style={{ background: config.bg, color: config.color }}
        >
          {config.label}
        </div>
        <div className="space-y-1">
          {check.checks.map((nestedCheck, idx) => (
            <CheckItem key={idx} check={nestedCheck} depth={depth + 1} />
          ))}
        </div>
      </div>
    );
  }

  // Regular check with field, op, value
  if (check.field) {
    // Get friendly label or create one from the field name
    let fieldLabel = FIELD_LABELS[check.field];
    if (!fieldLabel) {
      fieldLabel = check.field
        .replace(/\[\]\.?/g, ' ')
        .replace(/\[(\d+)\]\.?/g, ' #$1 ')
        .replace(/([a-z])([A-Z])/g, '$1 $2')
        .replace(/\./g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
        .replace(/^./, c => c.toUpperCase());
    }

    const opLabel = OP_LABELS[check.op] || check.op?.replace(/_/g, ' ');
    const displayValue = formatValue(check.value, check.op);

    return (
      <div className="flex items-center gap-2 text-xs flex-wrap py-0.5">
        <span
          className="font-medium px-2 py-0.5 rounded"
          style={{ background: '#F3F4F6', color: '#374151' }}
        >
          {fieldLabel}
        </span>
        <span style={{ color: '#6B7280', fontStyle: 'italic' }}>
          {opLabel}
        </span>
        {displayValue !== undefined && (
          <span
            className="font-mono px-2 py-0.5 rounded"
            style={{ background: '#EDE9FE', color: '#5B21B6', border: '1px solid #DDD6FE' }}
          >
            {displayValue}
          </span>
        )}
      </div>
    );
  }

  // Fallback for unknown structure
  return (
    <pre className="text-xs p-2 rounded overflow-x-auto" style={{ background: '#F3F4F6', color: '#374151' }}>
      {JSON.stringify(check, null, 2)}
    </pre>
  );
};

// Condition renderer - converts JSON condition to readable format
const ConditionDisplay = ({ condition }) => {
  if (!condition) return null;

  if (typeof condition === 'string') {
    return (
      <div className="p-3 rounded-lg" style={{ background: '#F9FAFB', border: '1px solid #E5E7EB' }}>
        <code className="text-xs" style={{ color: '#374151' }}>{condition}</code>
      </div>
    );
  }

  // Render the condition starting from the top-level
  return (
    <div>
      <div className="text-xs font-medium mb-2 flex items-center gap-1.5" style={{ color: '#6B7280' }}>
        Rule triggers when:
      </div>
      <CheckItem check={condition} depth={0} />
    </div>
  );
};

// Generate natural language description from condition
const generateConditionSentence = (condition, ruleType) => {
  if (!condition) return null;

  // Simple recursive extraction - track if we're inside a NOT
  const extractAllCodes = (cond, isNegated = false) => {
    const codes = { positive: [], negative: [] };

    if (!cond) return codes;

    // NOT flips the negation for everything inside
    if (cond.operator === 'NOT' && cond.checks) {
      cond.checks.forEach(check => {
        const nested = extractAllCodes(check, true);
        // Everything inside NOT becomes negative
        codes.negative.push(...nested.positive, ...nested.negative);
      });
      return codes;
    }

    // AND/OR just recurse into children
    if ((cond.operator === 'AND' || cond.operator === 'OR') && cond.checks) {
      cond.checks.forEach(check => {
        const nested = extractAllCodes(check, isNegated);
        codes.positive.push(...nested.positive);
        codes.negative.push(...nested.negative);
      });
      return codes;
    }

    // Leaf node - actual field check
    if (cond.field && cond.value) {
      let value = formatValue(cond.value, cond.op);

      // Add field context for special fields
      const fieldLower = cond.field.toLowerCase();
      if (fieldLower.includes('placeofservice') || fieldLower.includes('pos')) {
        value = `POS ${value}`;
      } else if (fieldLower.includes('modifier')) {
        value = `Modifier ${value}`;
      }

      const isNegativeOp = cond.op?.includes('not_');

      if (isNegated || isNegativeOp) {
        codes.negative.push(value);
      } else {
        codes.positive.push(value);
      }
    }

    return codes;
  };

  const codes = extractAllCodes(condition);

  // Remove duplicates
  const positive = [...new Set(codes.positive)];
  const negative = [...new Set(codes.negative)];

  // Build sentence based on rule type

  // Bundling rules - special handling for nested arrays
  if (ruleType === 'bundling') {
    // Extract bundled code groups from condition
    const bundleCheck = condition?.checks?.find(c => c.op === 'same_dos');
    if (bundleCheck && Array.isArray(bundleCheck.value)) {
      const groups = bundleCheck.value;
      if (groups.length >= 2) {
        return {
          type: 'bundling',
          codeGroup1: groups[0] || [],
          codeGroup2: groups[1] || [],
          verb: 'cannot be billed same day as'
        };
      }
    }
  }

  // Unit limit rules
  if (ruleType === 'unit_limit') {
    const unitCheck = condition?.checks?.find(c => c.op === 'greater_than' || c.op === 'greater_than_or_equal');
    if (unitCheck) {
      // Extract code from field like "serviceLines[procedureCode.code=82948].units.sum_by_dos"
      const codeMatch = unitCheck.field?.match(/code[=:](\w+)/);
      const code = codeMatch ? codeMatch[1] : null;
      return {
        type: 'unit_limit',
        code: code,
        maxUnits: unitCheck.value,
        verb: 'total units cannot exceed'
      };
    }
  }

  if (ruleType === 'diagnosis_conflict') {
    if (positive.length > 0 && negative.length > 0) {
      return {
        type: 'conflict',
        has: positive.slice(0, 1),
        conflicts: negative,
        verb: 'cannot appear with'
      };
    }
    if (positive.length >= 2) {
      return {
        type: 'mutual_conflict',
        codes: positive,
        verb: 'cannot appear together'
      };
    }
  }

  if (ruleType === 'sequencing') {
    if (positive.length > 0 && negative.length > 0) {
      // Filter out codes that appear in both lists
      const otherCodes = positive.filter(c => !negative.includes(c));
      return {
        type: 'sequencing',
        mustBePrimary: negative,
        whenPresent: otherCodes,
        verb: 'must be primary when'
      };
    }
  }

  if (ruleType === 'expected_code') {
    if (positive.length > 0 && negative.length > 0) {
      return {
        type: 'expected',
        has: positive,
        expected: negative,
        verb: 'should include one of'
      };
    }
  }

  // Generic fallback
  if (positive.length > 0 && negative.length > 0) {
    return {
      type: 'generic',
      has: positive,
      conflicts: negative,
      verb: 'should not have'
    };
  }

  if (positive.length >= 2) {
    return {
      type: 'mutual_conflict',
      codes: positive,
      verb: 'found together'
    };
  }

  return null;
};

// Code badge component - all chips identical neutral gray
const CodeBadge = ({ code }) => {
  return (
    <span
      className="inline-flex font-mono text-sm px-2.5 py-1 rounded"
      style={{ background: '#f3f4f6', border: '1px solid #e5e7eb', color: '#374151' }}
    >
      {code}
    </span>
  );
};

// Render display field from new template format
const RuleDisplayFromTemplate = ({ display }) => {
  if (!display) return null;

  const textStyle = { color: '#374151', fontSize: '14px', lineHeight: '1.8' };

  // Helper to render code list
  const renderCodeList = (codes) => {
    if (!codes) return null;
    const arr = Array.isArray(codes) ? codes : [codes];
    return arr.map((code, i) => <CodeBadge key={i} code={code} />);
  };

  // Helper to render formatted text with **bold** markdown
  const renderFormatted = (text) => {
    if (!text) return null;
    // Split by **bold** markers and render
    const parts = text.split(/\*\*([^*]+)\*\*/g);
    return parts.map((part, i) => {
      if (i % 2 === 1) {
        // Odd indices are bold content
        return <CodeBadge key={i} code={part} />;
      }
      return part ? <span key={i}>{part}</span> : null;
    });
  };

  const { template, subject, verb, object, qualifier, value, unit, formatted } = display;

  // Known templates with structured rendering
  if (template === 'sequencing' && subject && verb && object) {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(subject)}
        <span>{verb}</span>
        {renderCodeList(object)}
        {qualifier && <span>{qualifier}</span>}
      </div>
    );
  }

  if (template === 'expected' && subject && verb && object) {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(subject)}
        <span>{verb}</span>
        {renderCodeList(object)}
        {qualifier && <span className="text-gray-500 italic">{qualifier}</span>}
      </div>
    );
  }

  if (template === 'conflict' && subject && object) {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(subject)}
        <span>{verb || 'cannot appear with'}</span>
        {renderCodeList(object)}
      </div>
    );
  }

  if (template === 'unit_limit' && subject && value != null) {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(subject)}
        <span>{verb || 'units cannot exceed'}</span>
        <CodeBadge code={String(value)} />
        {unit && <span>{unit}</span>}
      </div>
    );
  }

  if (template === 'bundling' && subject && object) {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(subject)}
        <span>{verb || 'cannot be billed same day as'}</span>
        {renderCodeList(object)}
      </div>
    );
  }

  if (template === 'usage' && subject && value != null) {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(subject)}
        <span>{verb || 'count exceeding'}</span>
        <CodeBadge code={String(value)} />
        {unit && <span>{unit}</span>}
        {qualifier && <span>{qualifier}</span>}
      </div>
    );
  }

  if (template === 'pos' && subject && object) {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(subject)}
        <span>{verb || 'requires place of service'}</span>
        {renderCodeList(object)}
        {qualifier && <span className="text-gray-500">({qualifier})</span>}
      </div>
    );
  }

  if (template === 'modifier' && subject && object) {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(subject)}
        <span>{verb || 'requires modifier'}</span>
        {renderCodeList(object)}
        {qualifier && <span className="text-gray-500 italic">{qualifier}</span>}
      </div>
    );
  }

  // Fallback: use formatted text (handles 'unknown' template and any other)
  if (formatted) {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderFormatted(formatted)}
      </div>
    );
  }

  return null;
};

// Natural language condition display - all text neutral
const NaturalConditionDisplay = ({ sentence }) => {
  if (!sentence) return null;

  const textStyle = { color: '#374151', fontSize: '14px', lineHeight: '1.8' };

  // Helper to render code list (safely handles undefined/null/non-array)
  const renderCodeList = (codes) => {
    if (!codes) return null;
    const arr = Array.isArray(codes) ? codes : [codes];
    return arr.map((code, i) => <CodeBadge key={i} code={code} />);
  };

  // Type: bundling - "82948 or 82962 cannot be billed same day as 78811, 78812..."
  if (sentence.type === 'bundling') {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(sentence.codeGroup1)}
        <span>{sentence.verb}</span>
        {renderCodeList(sentence.codeGroup2)}
      </div>
    );
  }

  // Type: unit_limit - "82948 total units cannot exceed 2"
  if (sentence.type === 'unit_limit') {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {sentence.code && <CodeBadge code={sentence.code} />}
        <span>{sentence.verb}</span>
        <CodeBadge code={String(sentence.maxUnits)} />
        <span>per date of service</span>
      </div>
    );
  }

  // Type: conflict
  if (sentence.type === 'conflict') {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(sentence.has)}
        <span>cannot appear with any of</span>
        {renderCodeList(sentence.conflicts)}
      </div>
    );
  }

  // Type: mutual_conflict
  if (sentence.type === 'mutual_conflict') {
    const [first, ...rest] = sentence.codes;
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        <CodeBadge code={first} />
        <span>cannot appear with</span>
        {renderCodeList(rest)}
      </div>
    );
  }

  // Type: sequencing
  if (sentence.type === 'sequencing') {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(sentence.mustBePrimary)}
        <span>must be primary when</span>
        {sentence.whenPresent.length > 0 ? renderCodeList(sentence.whenPresent) : null}
        <span>is present</span>
      </div>
    );
  }

  // Type: expected
  if (sentence.type === 'expected') {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(sentence.has)}
        <span>should include one of</span>
        {renderCodeList(sentence.expected)}
      </div>
    );
  }

  // Type: generic fallback
  if (sentence.type === 'generic') {
    return (
      <div className="flex items-center gap-2 flex-wrap" style={textStyle}>
        {renderCodeList(sentence.has)}
        <span>should not have</span>
        {renderCodeList(sentence.conflicts)}
      </div>
    );
  }

  return null;
};

// Rule card component - calmed version with left border accent
const RuleCard = ({ rule, number }) => {
  const [showDetails, setShowDetails] = useState(false);
  const severity = rule.severity || 'info';

  // Left border color by severity
  const severityConfig = {
    error: { borderColor: '#dc2626', numberColor: '#dc2626' },
    warning: { borderColor: '#d97706', numberColor: '#d97706' },
    info: { borderColor: '#0090DA', numberColor: '#0090DA' }
  };

  const c = severityConfig[severity] || severityConfig.info;

  // Generate natural language sentence
  const sentence = generateConditionSentence(rule.condition, rule.type);

  // Type label formatting
  const typeLabel = rule.type ? rule.type.replace(/_/g, ' ') : null;

  const hasDetails = rule.message || rule.source?.citation;

  return (
    <div
      className="bg-white rounded-lg overflow-hidden"
      style={{ border: '1px solid #e5e7eb', borderLeft: `4px solid ${c.borderColor}` }}
    >
      {/* Header: white background, [Number. Title · Type] [Rule ID] */}
      <div className="px-4 py-3 flex items-center gap-3">
        {/* Title and type */}
        <div className="flex-1 min-w-0">
          <span className="font-medium text-sm" style={{ color: '#1a1a1a' }}>
            {number && <span style={{ color: c.numberColor }}>{number}. </span>}
            {rule.title || rule.description}
          </span>
          {typeLabel && (
            <span className="text-xs ml-2" style={{ color: '#9ca3af' }}>
              · {typeLabel}
            </span>
          )}
        </div>

        {/* Rule ID */}
        <span
          className="font-mono shrink-0"
          style={{ color: '#9ca3af', fontSize: '11px' }}
        >
          {rule.id || rule.rule_id}
        </span>
      </div>

      {/* Statement: gray background box */}
      <div
        className="mx-4 mb-3 p-3 rounded-lg"
        style={{ background: '#f9fafb' }}
      >
        {/* Priority: 1) rule.display (new format), 2) parsed sentence (legacy), 3) raw condition */}
        {rule.display ? (
          <RuleDisplayFromTemplate display={rule.display} />
        ) : sentence ? (
          <NaturalConditionDisplay sentence={sentence} severity={severity} />
        ) : (
          <ConditionDisplay condition={rule.condition} />
        )}
      </div>

      {/* Details: collapsible */}
      {hasDetails && (
        <div style={{ borderTop: '1px solid #f3f4f6' }}>
          <button
            onClick={() => setShowDetails(!showDetails)}
            className="w-full px-4 py-2 flex items-center gap-2 text-xs font-medium transition-colors"
            style={{ color: '#6b7280', background: showDetails ? '#f9fafb' : 'transparent' }}
            onMouseEnter={(e) => e.currentTarget.style.background = '#f9fafb'}
            onMouseLeave={(e) => e.currentTarget.style.background = showDetails ? '#f9fafb' : 'transparent'}
          >
            <ChevronRight
              className="w-3.5 h-3.5 transition-transform"
              style={{ transform: showDetails ? 'rotate(90deg)' : 'rotate(0deg)' }}
            />
            {showDetails ? 'Hide details' : 'Show details'}
          </button>

          {showDetails && (
            <div className="px-4 pb-3" style={{ background: '#f9fafb' }}>
              {/* Message/Explanation */}
              {rule.message && (
                <p className="text-sm leading-relaxed mb-2" style={{ color: '#4b5563' }}>
                  {rule.message}
                </p>
              )}

              {/* Source citation */}
              {rule.source?.citation && (
                <div className="flex items-start gap-2 text-xs" style={{ color: '#9ca3af' }}>
                  <BookOpen className="w-3.5 h-3.5 shrink-0 mt-0.5" />
                  <span className="italic">{rule.source.citation.replace(/\[\[|\]\]/g, '')}</span>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

// Removed rule row
const RemovedRuleRow = ({ rule, index }) => (
  <tr className="hover:bg-gray-50">
    <td className="px-3 py-2 text-xs text-gray-400 font-mono">{index + 1}</td>
    <td className="px-3 py-2 text-sm text-gray-700">{rule.original_text}</td>
    <td className="px-3 py-2 text-xs text-gray-500 font-mono">{rule.source_citation?.replace(/\[\[|\]\]/g, '')}</td>
    <td className="px-3 py-2 text-xs text-gray-600">{rule.reason}</td>
  </tr>
);

const CMSRuleViewer = ({ code, onClose }) => {
  const [loading, setLoading] = useState(true);
  const [ruleData, setRuleData] = useState(null);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState('rules');

  useEffect(() => {
    if (!code) return;

    const fetchRule = async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch(`${API_BASE}/codes/${encodeURIComponent(code)}/cms-rule`);
        if (res.ok) {
          const data = await res.json();
          setRuleData(data);
        } else {
          setError('Failed to load CMS rule');
        }
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchRule();
  }, [code]);

  if (!code) return null;

  const rule = ruleData?.rule;
  const sources = rule?.sources || {};

  // Merge validatable_rules and warning_rules (for backwards compatibility with old JSON format)
  const validatableRules = rule?.validatable_rules || [];
  const warningRulesLegacy = (rule?.warning_rules || []).map(r => ({
    ...r,
    severity: 'warning' // Force warning severity for legacy warning_rules array
  }));
  const allRules = [...validatableRules, ...warningRulesLegacy];

  // Calculate stats from merged rules array
  const stats = {
    error_count: allRules.filter(r => r.severity === 'error').length,
    warning_count: allRules.filter(r => r.severity === 'warning').length,
    info_count: allRules.filter(r => r.severity === 'info').length,
    removed_count: rule?.stats?.removed_count || rule?.removed_rules?.length || 0,
    ncci_ptp_count: rule?.stats?.ncci_ptp_count || rule?.ncci_edits?.ptp?.length || 0,
    ncci_mue_applied: rule?.stats?.ncci_mue_applied || !!rule?.ncci_edits?.mue
  };

  // Create a modified rule object with merged rules for display
  const displayRule = rule ? { ...rule, validatable_rules: allRules } : null;

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl w-full max-w-5xl max-h-[90vh] overflow-hidden shadow-2xl flex flex-col">
        {/* Header - Professional style */}
        <div className="px-5 py-4 flex items-center justify-between shrink-0" style={{ background: 'white', borderBottom: '1px solid #E5E7EB' }}>
          <div>
            <div className="flex items-center gap-3">
              <Shield className="w-5 h-5" style={{ color: '#0090DA' }} />
              <span
                className="font-mono text-sm px-2.5 py-1 rounded font-semibold"
                style={{ background: '#F3F4F6', color: '#1a1a1a' }}
              >
                {code}
              </span>
              <span className="text-sm font-medium" style={{ color: '#6B7280' }}>CMS-1500 Claim Rules</span>
            </div>
            {ruleData && (
              <p className="text-xs mt-1.5" style={{ color: '#9CA3AF' }}>
                Version {ruleData.version}
                {rule?.generated_at && (
                  <span className="ml-2">
                    • Generated {new Date(rule.generated_at).toLocaleString()}
                  </span>
                )}
              </p>
            )}
          </div>
          <button
            onClick={onClose}
            className="p-2 rounded-lg transition-colors"
            style={{ color: '#6B7280' }}
            onMouseEnter={(e) => e.currentTarget.style.background = '#F3F4F6'}
            onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-hidden flex flex-col">
          {loading ? (
            <div className="flex-1 flex items-center justify-center">
              <Loader2 className="w-8 h-8 animate-spin text-teal-500" />
            </div>
          ) : error ? (
            <div className="p-6">
              <div className="text-red-500 p-4 bg-red-50 rounded-lg">{error}</div>
            </div>
          ) : ruleData ? (
            <>
              {/* Stats Bar - Outline style */}
              <div className="px-5 py-4 shrink-0" style={{ background: 'white', borderBottom: '1px solid #E5E7EB' }}>
                <div className="flex items-center gap-6 flex-wrap">
                  {/* Sources */}
                  <div className="flex items-center gap-3">
                    <span className="text-xs uppercase tracking-wider font-semibold" style={{ color: '#6B7280' }}>Sources</span>
                    <span className={`flex items-center gap-1.5 text-sm ${sources.guideline?.used ? '' : 'opacity-40'}`} style={{ color: sources.guideline?.used ? '#0090DA' : '#9CA3AF' }}>
                      <BookOpen className="w-4 h-4" />
                      <span>Guideline</span>
                      {sources.guideline?.used && <CheckCircle className="w-3.5 h-3.5" style={{ color: '#059669' }} />}
                    </span>
                    <span className={`flex items-center gap-1.5 text-sm ${sources.ncci_ptp?.used || sources.ncci_mue?.used ? '' : 'opacity-40'}`} style={{ color: sources.ncci_ptp?.used || sources.ncci_mue?.used ? '#7C3AED' : '#9CA3AF' }}>
                      <Database className="w-4 h-4" />
                      <span>NCCI</span>
                      {(sources.ncci_ptp?.used || sources.ncci_mue?.used) && <CheckCircle className="w-3.5 h-3.5" style={{ color: '#059669' }} />}
                    </span>
                  </div>

                  <div className="w-px h-5" style={{ background: '#E5E7EB' }} />

                  {/* Rules - outline style */}
                  <div className="flex items-center gap-3">
                    <span className="text-xs uppercase tracking-wider font-semibold" style={{ color: '#6B7280' }}>Rules</span>
                    <span
                      className="text-xs font-medium px-2.5 py-1 rounded"
                      style={{ background: 'transparent', color: '#dc2626', border: '1px solid #dc2626' }}
                    >
                      {stats.error_count || 0} reject
                    </span>
                    <span
                      className="text-xs font-medium px-2.5 py-1 rounded"
                      style={{ background: 'transparent', color: '#d97706', border: '1px solid #d97706' }}
                    >
                      {stats.warning_count || 0} review
                    </span>
                    <span
                      className="text-xs font-medium px-2.5 py-1 rounded"
                      style={{ background: 'transparent', color: '#0090DA', border: '1px solid #0090DA' }}
                    >
                      {stats.info_count || 0} info
                    </span>
                    <span
                      className="text-xs font-medium px-2.5 py-1 rounded"
                      style={{ background: 'transparent', color: '#6B7280', border: '1px solid #d1d5db' }}
                    >
                      {stats.removed_count || 0} not validatable
                    </span>
                  </div>
                </div>
              </div>

              {/* Tabs - Professional style */}
              <div className="flex shrink-0 px-2 pt-2" style={{ background: '#F9FAFB', borderBottom: '1px solid #E5E7EB' }}>
                {[
                  { id: 'rules', label: 'Validatable Rules', count: (stats.error_count || 0) + (stats.warning_count || 0) + (stats.info_count || 0) },
                  { id: 'removed', label: 'Not Validatable', count: stats.removed_count },
                  { id: 'raw', label: 'Raw Output' }
                ].map(tab => (
                  <button
                    key={tab.id}
                    onClick={() => setActiveTab(tab.id)}
                    className="px-4 py-2.5 text-sm font-medium transition-all rounded-t-lg"
                    style={{
                      background: activeTab === tab.id ? 'white' : 'transparent',
                      color: activeTab === tab.id ? '#1a1a1a' : '#6B7280',
                      borderTop: activeTab === tab.id ? '1px solid #E5E7EB' : '1px solid transparent',
                      borderLeft: activeTab === tab.id ? '1px solid #E5E7EB' : '1px solid transparent',
                      borderRight: activeTab === tab.id ? '1px solid #E5E7EB' : '1px solid transparent',
                      marginBottom: activeTab === tab.id ? '-1px' : '0'
                    }}
                  >
                    {tab.label}
                    {tab.count !== undefined && (
                      <span
                        className="ml-2 px-2 py-0.5 rounded-full text-xs font-semibold"
                        style={{
                          background: activeTab === tab.id ? '#0090DA' : '#E5E7EB',
                          color: activeTab === tab.id ? 'white' : '#6B7280'
                        }}
                      >
                        {tab.count || 0}
                      </span>
                    )}
                  </button>
                ))}
              </div>

              {/* Tab Content */}
              <div className="flex-1 overflow-auto p-4">
                {activeTab === 'rules' && (
                  <div className="space-y-3">
                    {displayRule?.validatable_rules?.length > 0 ? (
                      displayRule.validatable_rules.map((r, idx) => (
                        <RuleCard key={idx} rule={r} number={idx + 1} />
                      ))
                    ) : (
                      <div className="text-center py-12" style={{ color: '#9CA3AF' }}>
                        <Shield className="w-12 h-12 mx-auto mb-3 opacity-30" />
                        <p className="font-medium">No validatable rules</p>
                        <p className="text-sm">All guideline rules require medical record review</p>
                      </div>
                    )}
                  </div>
                )}

                {activeTab === 'removed' && (
                  <div className="border rounded-lg overflow-hidden">
                    {rule?.removed_rules?.length > 0 ? (
                      <table className="w-full text-left">
                        <thead className="bg-gray-100 text-xs text-gray-600 uppercase">
                          <tr>
                            <th className="px-3 py-2 w-8">#</th>
                            <th className="px-3 py-2">Original Guideline Rule</th>
                            <th className="px-3 py-2 w-28">Source</th>
                            <th className="px-3 py-2 w-64">Reason Not Validatable</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y">
                          {rule.removed_rules.map((r, idx) => (
                            <RemovedRuleRow key={idx} rule={r} index={idx} />
                          ))}
                        </tbody>
                      </table>
                    ) : (
                      <div className="text-center py-12 text-gray-400">
                        <p>No removed rules</p>
                      </div>
                    )}
                  </div>
                )}

                {activeTab === 'raw' && (
                  <div className="prose prose-sm max-w-none">
                    <ReactMarkdown
                      remarkPlugins={[remarkGfm]}
                      components={{
                        h1: ({children}) => <h1 className="text-xl font-bold text-gray-900 mt-6 mb-3 pb-2 border-b">{children}</h1>,
                        h2: ({children}) => <h2 className="text-lg font-bold text-gray-800 mt-5 mb-2">{children}</h2>,
                        h3: ({children}) => <h3 className="text-base font-semibold text-gray-800 mt-4 mb-2">{children}</h3>,
                        p: ({children}) => <p className="mb-3 text-gray-700 leading-relaxed">{children}</p>,
                        ul: ({children}) => <ul className="list-disc pl-5 mb-3 space-y-1">{children}</ul>,
                        ol: ({children}) => <ol className="list-decimal pl-5 mb-3 space-y-1">{children}</ol>,
                        li: ({children}) => <li className="text-gray-700">{children}</li>,
                        code: ({children, className}) => {
                          const isBlock = className?.includes('language-');
                          return isBlock
                            ? <pre className="bg-slate-100 p-3 rounded-lg overflow-x-auto text-xs"><code>{children}</code></pre>
                            : <code className="bg-slate-100 px-1 py-0.5 rounded text-xs font-mono">{children}</code>;
                        },
                        pre: ({children}) => <>{children}</>,
                        table: ({children}) => <table className="w-full border-collapse border border-gray-300 my-4">{children}</table>,
                        th: ({children}) => <th className="border border-gray-300 bg-gray-100 px-3 py-2 text-left text-xs font-semibold">{children}</th>,
                        td: ({children}) => <td className="border border-gray-300 px-3 py-2 text-sm">{children}</td>,
                        strong: ({children}) => <strong className="font-semibold text-gray-900">{children}</strong>,
                      }}
                    >
                      {ruleData.markdown || 'No raw output available'}
                    </ReactMarkdown>
                  </div>
                )}
              </div>
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center text-gray-400">
              No rule data
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

// =============================================================================
// BATCH GENERATION PROGRESS MODAL
// =============================================================================

const BatchProgressModal = ({ isOpen, progress, onClose, generating }) => {
  const [expandedCode, setExpandedCode] = useState(null);
  const [expandedStep, setExpandedStep] = useState(null);

  if (!isOpen) return null;

  const codes = Object.keys(progress || {});
  const completed = codes.filter(c => progress[c]?.status === 'complete').length;
  const errors = codes.filter(c => progress[c]?.status === 'error').length;
  const inProgress = codes.filter(c => progress[c]?.status === 'generating').length;

  const STEP_LABELS = {
    transform: { label: 'Transform', desc: 'Guideline + NCCI → CMS Rules' },
    parse: { label: 'Parse', desc: 'Markdown → JSON' }
  };

  // Auto-expand currently generating code
  const currentGenerating = codes.find(c => progress[c]?.status === 'generating');

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl w-full max-w-5xl max-h-[90vh] overflow-hidden shadow-2xl flex flex-col">
        <div className="p-4 border-b flex items-center justify-between shrink-0">
          <h3 className="font-semibold flex items-center gap-2">
            <Shield className="w-5 h-5 text-blue-600" />
            Generating CMS Rules
          </h3>
          <div className="flex items-center gap-3">
            {/* Progress stats */}
            <div className="flex items-center gap-2 text-sm">
              {inProgress > 0 && (
                <span className="text-blue-600 flex items-center gap-1">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  {inProgress} running
                </span>
              )}
              <span className="text-green-600">{completed} done</span>
              {errors > 0 && <span className="text-red-600">{errors} failed</span>}
              <span className="text-gray-400">/ {codes.length}</span>
            </div>
            <button
              onClick={onClose}
              disabled={generating}
              className="p-2 hover:bg-gray-100 rounded-lg disabled:opacity-50"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>

        <div className="flex-1 overflow-auto p-4">
          {/* Overall progress bar */}
          <div className="mb-4">
            <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
              <div
                className="h-full bg-blue-500 transition-all"
                style={{ width: `${codes.length > 0 ? ((completed + errors) / codes.length) * 100 : 0}%` }}
              />
            </div>
          </div>

          {/* Code list */}
          <div className="space-y-3">
            {codes.map(code => {
              const codeProgress = progress[code] || {};
              const status = codeProgress.status || 'pending';
              const steps = codeProgress.steps || {};
              const isExpanded = expandedCode === code || (status === 'generating' && !expandedCode);

              return (
                <div key={code} className="border rounded-lg overflow-hidden">
                  {/* Code header */}
                  <button
                    onClick={() => setExpandedCode(isExpanded ? null : code)}
                    className="w-full p-3 flex items-center justify-between hover:bg-gray-50 transition-colors"
                  >
                    <div className="flex items-center gap-3">
                      {status === 'pending' && <Clock className="w-5 h-5 text-gray-300" />}
                      {status === 'generating' && <Loader2 className="w-5 h-5 animate-spin text-blue-500" />}
                      {status === 'complete' && <CheckCircle className="w-5 h-5 text-green-500" />}
                      {status === 'error' && <AlertCircle className="w-5 h-5 text-red-500" />}
                      <span className="font-mono font-medium text-lg">{code}</span>
                      {status === 'generating' && (
                        <span className="text-sm text-blue-600">Processing...</span>
                      )}
                      {status === 'complete' && (
                        <span className="text-sm text-green-600">Complete</span>
                      )}
                      {status === 'error' && (
                        <span className="text-sm text-red-600">Failed</span>
                      )}
                    </div>
                    <ChevronDown className={`w-5 h-5 text-gray-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                  </button>

                  {/* Expanded details - full step view like original */}
                  {isExpanded && status !== 'pending' && (
                    <div className="border-t p-4 bg-gray-50">
                      <div className="space-y-3">
                        {/* Cascade indicator */}
                        {codeProgress.cascade && codeProgress.cascade.patterns_to_generate && codeProgress.cascade.patterns_to_generate.length > 1 && (
                          <div className="border rounded-lg overflow-hidden bg-white">
                            <div className="px-4 py-2 bg-purple-50 border-b flex items-center gap-2">
                              <GitBranch className="w-4 h-4 text-purple-600" />
                              <span className="text-sm font-semibold text-purple-800">Cascade Generation</span>
                              <span className="text-xs text-purple-600">
                                ({codeProgress.cascade.current_index + 1}/{codeProgress.cascade.patterns_to_generate.length} levels)
                              </span>
                            </div>
                            <div className="p-3">
                              <div className="flex items-center gap-2 flex-wrap">
                                {codeProgress.cascade.patterns_to_generate.map((pattern, idx) => {
                                  const isComplete = idx < codeProgress.cascade.current_index;
                                  const isCurrent = idx === codeProgress.cascade.current_index && codeProgress.cascade.current_pattern === pattern;
                                  return (
                                    <React.Fragment key={pattern}>
                                      <div className={`px-3 py-1.5 rounded-lg text-sm font-mono flex items-center gap-1.5 ${
                                        isComplete
                                          ? 'bg-green-100 text-green-700 border border-green-200'
                                          : isCurrent
                                            ? 'bg-blue-100 text-blue-700 border border-blue-300 ring-2 ring-blue-200'
                                            : 'bg-gray-100 text-gray-500 border border-gray-200'
                                      }`}>
                                        {isComplete && <CheckCircle className="w-3.5 h-3.5" />}
                                        {isCurrent && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                                        {pattern}
                                      </div>
                                      {idx < codeProgress.cascade.patterns_to_generate.length - 1 && (
                                        <ArrowRight className={`w-4 h-4 ${
                                          isComplete ? 'text-green-400' : 'text-gray-300'
                                        }`} />
                                      )}
                                    </React.Fragment>
                                  );
                                })}
                              </div>
                              {codeProgress.cascade.parent_rule && (
                                <div className="mt-2 text-xs text-gray-500">
                                  Inheriting from: <span className="font-mono font-medium">{codeProgress.cascade.parent_rule}</span>
                                </div>
                              )}
                            </div>
                          </div>
                        )}

                        {['transform', 'parse'].map(stepName => {
                          const step = steps[stepName];
                          const stepStatus = step?.status || 'idle';
                          const stepInfo = STEP_LABELS[stepName] || { label: stepName, desc: '' };
                          const isStepExpanded = expandedStep === `${code}-${stepName}` || stepStatus === 'streaming';

                          return (
                            <div key={stepName} className="border rounded-lg overflow-hidden bg-white">
                              {/* Step header */}
                              <button
                                onClick={() => setExpandedStep(isStepExpanded ? null : `${code}-${stepName}`)}
                                className="w-full p-3 flex items-center justify-between hover:bg-gray-100 transition-colors"
                              >
                                <div className="flex items-center gap-3">
                                  {stepStatus === 'idle' && <Clock className="w-5 h-5 text-gray-300" />}
                                  {stepStatus === 'streaming' && <Loader2 className="w-5 h-5 animate-spin text-blue-500" />}
                                  {stepStatus === 'done' && <CheckCircle className="w-5 h-5 text-green-500" />}
                                  {stepStatus === 'error' && <AlertCircle className="w-5 h-5 text-red-500" />}
                                  <div className="text-left">
                                    <div className="font-medium">{stepInfo.label}</div>
                                    <div className="text-xs text-gray-500">{stepInfo.desc}</div>
                                  </div>
                                </div>
                                <div className="flex items-center gap-2">
                                  {step?.duration_ms && (
                                    <span className="text-xs text-gray-500 bg-gray-200 px-2 py-0.5 rounded">
                                      {(step.duration_ms / 1000).toFixed(1)}s
                                    </span>
                                  )}
                                  {(step?.thinking || step?.content) && (
                                    <ChevronDown className={`w-4 h-4 text-gray-400 transition-transform ${isStepExpanded ? 'rotate-180' : ''}`} />
                                  )}
                                </div>
                              </button>

                              {/* Expanded step content */}
                              {isStepExpanded && (step?.thinking || step?.content || step?.think_preview || step?.preview) && (
                                <div className="border-t">
                                  {/* Thinking section */}
                                  {(step?.thinking || step?.think_preview) && (
                                    <div className="border-b">
                                      <div className="px-3 py-2 bg-amber-50 text-xs font-medium text-amber-700 flex items-center gap-1">
                                        <span>💭</span> Reasoning
                                        {step?.thinking && (
                                          <span className="text-amber-500 ml-1">
                                            ({Math.round((step.thinking.length) / 1000)}k chars)
                                          </span>
                                        )}
                                      </div>
                                      <div className="p-3 bg-amber-50/30 max-h-48 overflow-y-auto">
                                        <pre className="text-xs text-gray-700 whitespace-pre-wrap font-mono leading-relaxed">
                                          {step?.thinking || step?.think_preview || ''}
                                        </pre>
                                      </div>
                                    </div>
                                  )}

                                  {/* Content/Output section */}
                                  {(step?.content || step?.preview || step?.full_text) && (
                                    <div>
                                      <div className="px-3 py-2 bg-blue-50 text-xs font-medium text-blue-700 flex items-center gap-1">
                                        <FileText className="w-3 h-3" /> Output
                                        {step?.content && (
                                          <span className="text-blue-500 ml-1">
                                            ({Math.round((step.content.length) / 1000)}k chars)
                                          </span>
                                        )}
                                      </div>
                                      <div className="p-3 bg-blue-50/30 max-h-64 overflow-y-auto">
                                        <pre className="text-xs text-gray-700 whitespace-pre-wrap font-mono leading-relaxed">
                                          {step?.content || step?.full_text || step?.preview || ''}
                                        </pre>
                                      </div>
                                    </div>
                                  )}
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>

                      {/* Error message */}
                      {codeProgress.error && (
                        <div className="mt-3 p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
                          <div className="font-medium mb-1">Error</div>
                          {codeProgress.error}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
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
  onGenerateSingle,
  onViewRule,
  generating
}) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [expandedSections, setExpandedSections] = useState({
    generated: true,
    ready: true,
    noSources: false
  });
  const [expandedSubSections, setExpandedSubSections] = useState({});

  const filterCodes = (codes) => codes.filter(c =>
    c.code.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const allCodes = [...filterCodes(diagnoses), ...filterCodes(procedures)];

  // Categorize codes into three groups
  const hasCmsRule = (code) => code.cms_rule?.has_rule;
  const hasSources = (code) => {
    const hasGuideline = code.guideline_rule?.has_rule || code.rule_status?.has_rule;
    const hasNcci = code.type === 'CPT' || code.type === 'HCPCS';
    return hasGuideline || hasNcci;
  };

  const generatedCodes = allCodes.filter(hasCmsRule);
  const readyToGenerate = allCodes.filter(c => !hasCmsRule(c) && hasSources(c));
  const noSourcesCodes = allCodes.filter(c => !hasCmsRule(c) && !hasSources(c));

  // Further split by type
  const splitByType = (codes) => ({
    diagnoses: codes.filter(c => c.type === 'ICD-10' || !c.type),
    procedures: codes.filter(c => c.type === 'CPT' || c.type === 'HCPCS')
  });

  const generated = splitByType(generatedCodes);
  const ready = splitByType(readyToGenerate);
  const noSources = splitByType(noSourcesCodes);

  // Selectable codes = all codes with sources (generated + ready, for regeneration)
  const selectableCodes = [...generatedCodes, ...readyToGenerate];
  const allSelected = selectableCodes.length > 0 &&
    selectableCodes.every(c => selectedCodes.has(c.code));

  const toggleSection = (section) => {
    setExpandedSections(prev => ({ ...prev, [section]: !prev[section] }));
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-6 h-6 animate-spin text-teal-600" />
      </div>
    );
  }

  // Section component (simplified - no Generate All button)
  const Section = ({ id, icon, title, count, color, children }) => {
    const isExpanded = expandedSections[id];
    if (count === 0) return null;

    return (
      <div className="border rounded-lg overflow-hidden" style={{ borderColor: '#e5e7eb' }}>
        <button
          onClick={() => toggleSection(id)}
          className="w-full flex items-center gap-3 px-4 py-3 transition-colors hover:bg-gray-50"
          style={{ background: 'white' }}
        >
          <span style={{ color }}>{icon}</span>
          <span className="font-medium text-sm" style={{ color: '#1a1a1a' }}>{title}</span>
          <span
            className="text-xs font-semibold px-2 py-0.5 rounded-full"
            style={{ background: color, color: 'white' }}
          >
            {count}
          </span>
          <div className="flex-1" />
          <ChevronRight
            className="w-4 h-4 transition-transform"
            style={{ color: '#9ca3af', transform: isExpanded ? 'rotate(90deg)' : 'rotate(0deg)' }}
          />
        </button>
        {isExpanded && <div className="border-t" style={{ borderColor: '#e5e7eb' }}>{children}</div>}
      </div>
    );
  };

  // Toggle subsection (diagnoses/procedures within a section)
  const toggleSubSection = (sectionId, subLabel) => {
    const key = `${sectionId}-${subLabel}`;
    setExpandedSubSections(prev => ({ ...prev, [key]: !prev[key] }));
  };

  // Sub-section for diagnoses/procedures (collapsed by default)
  const SubSection = ({ sectionId, label, codes }) => {
    if (codes.length === 0) return null;
    const key = `${sectionId}-${label}`;
    const isExpanded = expandedSubSections[key] ?? false; // collapsed by default

    return (
      <div className="px-3 py-2">
        <button
          onClick={() => toggleSubSection(sectionId, label)}
          className="w-full flex items-center gap-2 text-xs font-medium uppercase tracking-wider mb-2 hover:text-gray-600 transition-colors"
          style={{ color: '#9ca3af' }}
        >
          <ChevronRight
            className="w-3 h-3 transition-transform"
            style={{ transform: isExpanded ? 'rotate(90deg)' : 'rotate(0deg)' }}
          />
          {label} ({codes.length})
        </button>
        {isExpanded && (
          <div className="space-y-1">
            {codes.map(code => (
              <CodeItem
                key={code.code}
                code={code}
                isSelected={selectedCodes.has(code.code)}
                generating={generating}
                onToggleCode={onToggleCode}
                onGenerate={onGenerateSingle}
                onViewRule={onViewRule}
              />
            ))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="p-4 border-b" style={{ background: '#f9fafb' }}>
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <h3 className="font-semibold" style={{ color: '#1a1a1a' }}>{categoryName}</h3>
          </div>
          <div className="flex items-center gap-2">
            {selectedCodes.size > 0 && (
              <button
                onClick={onGenerate}
                disabled={generating}
                className="px-4 py-2 text-sm rounded-lg flex items-center gap-1.5 font-medium transition-colors"
                style={{
                  color: generating ? '#9ca3af' : '#0090DA',
                  background: generating ? '#f9fafb' : 'transparent',
                  border: generating ? '1px solid #d1d5db' : '2px solid #0090DA'
                }}
                onMouseEnter={(e) => !generating && (e.currentTarget.style.background = '#eff6ff')}
                onMouseLeave={(e) => e.currentTarget.style.background = generating ? '#f9fafb' : 'transparent'}
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
              style={{ borderColor: '#e5e7eb' }}
            />
          </div>

          {selectableCodes.length > 0 && (
            <button
              onClick={() => onSelectAll(selectableCodes.map(c => c.code))}
              className="px-3 py-2 text-sm rounded-lg whitespace-nowrap"
              style={{ color: '#0090DA', background: 'transparent' }}
            >
              {allSelected ? 'Deselect all' : 'Select all'} ({selectableCodes.length})
            </button>
          )}
        </div>
      </div>

      {/* Code sections */}
      <div className="flex-1 overflow-auto p-3 space-y-3">
        {allCodes.length === 0 ? (
          <div className="text-center py-12" style={{ color: '#9ca3af' }}>
            No codes found
          </div>
        ) : (
          <>
            {/* Generated Rules */}
            <Section
              id="generated"
              icon="✓"
              title="Generated Rules"
              count={generatedCodes.length}
              color="#059669"
            >
              <SubSection sectionId="generated" label="Diagnoses" codes={generated.diagnoses} />
              <SubSection sectionId="generated" label="Procedures" codes={generated.procedures} />
            </Section>

            {/* Ready to Generate */}
            <Section
              id="ready"
              icon="⚡"
              title="Ready to Generate"
              count={readyToGenerate.length}
              color="#0090DA"
            >
              <SubSection sectionId="ready" label="Diagnoses" codes={ready.diagnoses} />
              <SubSection sectionId="ready" label="Procedures" codes={ready.procedures} />
            </Section>

            {/* No Sources */}
            <Section
              id="noSources"
              icon="○"
              title="No Sources"
              count={noSourcesCodes.length}
              color="#9ca3af"
            >
              <SubSection sectionId="noSources" label="Diagnoses" codes={noSources.diagnoses} />
              <SubSection sectionId="noSources" label="Procedures" codes={noSources.procedures} />
            </Section>
          </>
        )}
      </div>
    </div>
  );
};

// =============================================================================
// SSE PARSER
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

export default function ClaimRules() {
  const { categories, total_codes, loading, refetch } = useCategories();
  const [selectedCategory, setSelectedCategory] = useState(null);
  const { diagnoses, procedures, loading: codesLoading, refetch: refetchCodes } = useCategoryCodesWithCMS(selectedCategory);

  const [selectedCodes, setSelectedCodes] = useState(new Set());
  const [generating, setGenerating] = useState(false);

  // Auto-select first category when categories load
  useEffect(() => {
    if (!selectedCategory && categories.length > 0) {
      setSelectedCategory(categories[0].name);
    }
  }, [categories, selectedCategory]);

  // Batch progress tracking (code -> progress)
  const [batchProgress, setBatchProgress] = useState({});
  const [showBatchProgress, setShowBatchProgress] = useState(false);
  const [viewingCode, setViewingCode] = useState(null);

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
        const next = new Set(prev);
        codesToSelect.forEach(c => next.delete(c));
        return next;
      } else {
        return new Set([...prev, ...codesToSelect]);
      }
    });
  };

  // Generate a single CMS rule with progress tracking
  const generateSingleCMSRule = async (code, forceRegenerate = false) => {
    console.log(`[CMS] Starting generation for: ${code}`, forceRegenerate ? '(force)' : '');

    // Mark as generating (preserve any existing data)
    setBatchProgress(prev => ({
      ...prev,
      [code]: {
        ...(prev[code] || {}),
        status: 'generating',
        steps: {}
      }
    }));

    try {
      const response = await fetch(`${API_BASE}/generate-cms/${encodeURIComponent(code)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ force_regenerate: forceRegenerate })
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
        buffer = lines.pop() || '';

        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed || !trimmed.startsWith('data: ')) continue;

          const data = parseSSELine(trimmed);
          if (!data) continue;

          const { step, type } = data;
          console.log(`[CMS ${code}] SSE event:`, { step, type });

          if (type === 'done' && step === 'pipeline') {
            console.log(`[CMS ${code}] Pipeline DONE - marking complete`);
            setBatchProgress(prev => {
              console.log(`[CMS ${code}] Before complete:`, prev[code]?.status);
              return {
                ...prev,
                [code]: { ...(prev[code] || {}), status: 'complete' }
              };
            });
            continue;
          }

          if (type === 'error') {
            console.log(`[CMS ${code}] ERROR:`, data.content);
            setBatchProgress(prev => ({
              ...prev,
              [code]: { ...(prev[code] || {}), status: 'error', error: data.content || 'Unknown error' }
            }));
            continue;
          }

          // Update step progress
          setBatchProgress(prev => {
            const prevCode = prev[code] || { status: 'generating', steps: {}, cascade: null };
            const prevStep = prevCode.steps?.[step] || { status: 'idle', thinking: '', content: '' };
            let updatedStep = { ...prevStep };
            let updatedCascade = prevCode.cascade;

            switch (type) {
              // Cascade events (from HierarchyRuleGenerator)
              case 'plan':
                updatedCascade = {
                  patterns_to_generate: data.patterns_to_generate || [],
                  existing_patterns: data.existing_patterns || [],
                  current_index: 0,
                  current_pattern: null
                };
                updatedStep.status = 'planning';
                updatedStep.message = data.content;
                break;
              case 'generating':
                if (updatedCascade) {
                  const idx = updatedCascade.patterns_to_generate.indexOf(data.pattern);
                  updatedCascade = {
                    ...updatedCascade,
                    current_index: idx >= 0 ? idx : updatedCascade.current_index,
                    current_pattern: data.pattern,
                    parent_rule: data.parent_rule
                  };
                }
                // Reset all step outputs for new code in cascade
                prevCode.steps = {
                  transform: { status: 'idle', thinking: '', content: '' },
                  parse: { status: 'idle', thinking: '', content: '' }
                };
                updatedStep.status = 'streaming';
                updatedStep.message = data.content;
                break;
              case 'exists':
              case 'skip':
                updatedStep.status = 'done';
                updatedStep.message = data.content;
                break;
              case 'complete':
                updatedStep.status = 'done';
                updatedStep.message = data.content;
                if (updatedCascade) {
                  updatedCascade.current_index = updatedCascade.patterns_to_generate.length;
                }
                break;
              // Standard pipeline events
              case 'status':
                updatedStep.status = 'streaming';
                if (data.content) updatedStep.message = data.content;
                break;

              case 'thought':
                updatedStep.status = 'streaming';
                updatedStep.thinking = (updatedStep.thinking || '') + (data.content || '');
                if (data.think_preview) {
                  updatedStep.think_preview = data.think_preview;
                }
                break;

              case 'content':
                updatedStep.status = 'streaming';
                updatedStep.content = (updatedStep.content || '') + (data.content || '');
                if (data.preview) {
                  updatedStep.preview = data.preview;
                }
                break;

              case 'done':
                updatedStep.status = 'done';
                if (data.full_text) updatedStep.content = data.full_text;
                if (data.thinking) updatedStep.thinking = data.thinking;
                if (data.duration_ms) updatedStep.duration_ms = data.duration_ms;
                break;
            }

            return {
              ...prev,
              [code]: {
                ...prevCode,
                cascade: updatedCascade,
                steps: { ...prevCode.steps, [step]: updatedStep }
              }
            };
          });
        }
      }

      // Process any remaining buffer
      if (buffer.trim()) {
        console.log(`[CMS ${code}] Processing remaining buffer:`, buffer);
        const data = parseSSELine(buffer.trim());
        if (data && data.type === 'done' && data.step === 'pipeline') {
          console.log(`[CMS ${code}] Found pipeline done in buffer`);
          setBatchProgress(prev => ({
            ...prev,
            [code]: { ...(prev[code] || {}), status: 'complete' }
          }));
        }
      }

      // Ensure marked complete
      console.log(`[CMS ${code}] Stream ended - ensuring complete status`);
      setBatchProgress(prev => {
        const currentStatus = prev[code]?.status;
        console.log(`[CMS ${code}] Final status check:`, currentStatus);
        if (currentStatus === 'generating' || currentStatus === 'pending') {
          return { ...prev, [code]: { ...(prev[code] || {}), status: 'complete' } };
        }
        return prev;
      });

    } catch (err) {
      console.error(`[CMS] Failed to generate rule for ${code}:`, err);
      setBatchProgress(prev => ({
        ...prev,
        [code]: { ...(prev[code] || {}), status: 'error', error: err.message }
      }));
    }
  };

  // Generate CMS rules with parallel processing (concurrency limit)
  const generateCMSRules = async (codes, concurrency = 5, forceRegenerate = false) => {
    if (codes.length === 0) return;

    setGenerating(true);
    setShowBatchProgress(true);

    // Initialize batch progress
    const initialProgress = {};
    codes.forEach(code => {
      initialProgress[code] = { status: 'pending', steps: {}, error: null };
    });
    setBatchProgress(initialProgress);

    // Wait a tick for React to apply initial state
    await new Promise(resolve => setTimeout(resolve, 0));

    // Process codes in parallel with concurrency limit using index-based approach
    let currentIndex = 0;

    const processNext = async () => {
      while (currentIndex < codes.length) {
        const index = currentIndex++;
        const code = codes[index];
        if (code) {
          await generateSingleCMSRule(code, forceRegenerate);
        }
      }
    };

    // Start up to 'concurrency' parallel workers
    const workers = [];
    for (let i = 0; i < Math.min(concurrency, codes.length); i++) {
      workers.push(processNext());
    }

    // Wait for all workers to complete
    await Promise.all(workers);

    setGenerating(false);
    setSelectedCodes(new Set());
    refetchCodes();
  };

  const handleGenerateSingle = async (code, forceRegenerate = false) => {
    await generateCMSRules([code], 5, forceRegenerate);
  };

  const handleGenerateBatch = async () => {
    if (selectedCodes.size === 0) return;
    await generateCMSRules(Array.from(selectedCodes));
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="p-4 border-b bg-white">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="font-semibold text-lg flex items-center gap-2">
              <Shield className="w-5 h-5 text-teal-600" />
              Claim Rules
            </h2>
            <p className="text-sm text-gray-500">
              CMS-1500 validation rules from Guidelines + NCCI
            </p>
          </div>
          <button
            onClick={() => { refetch(); if (selectedCategory) refetchCodes(); }}
            className="p-2 hover:bg-gray-100 rounded-lg"
            title="Refresh data"
          >
            <RefreshCw className="w-4 h-4" />
          </button>
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
              onGenerate={handleGenerateBatch}
              onGenerateSingle={handleGenerateSingle}
              onViewRule={setViewingCode}
              generating={generating}
            />
          ) : (
            <div className="h-full flex items-center justify-center text-gray-400">
              <div className="text-center">
                <Shield className="w-16 h-16 mx-auto mb-4 opacity-30" />
                <p className="text-lg font-medium">Select a category</p>
                <p className="text-sm">Choose a category to generate CMS rules</p>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Batch Generation Progress Modal */}
      <BatchProgressModal
        isOpen={showBatchProgress}
        progress={batchProgress}
        onClose={() => !generating && setShowBatchProgress(false)}
        generating={generating}
      />

      {/* CMS Rule Viewer */}
      <CMSRuleViewer
        code={viewingCode}
        onClose={() => setViewingCode(null)}
      />
    </div>
  );
}
