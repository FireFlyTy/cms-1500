import React, { useState } from 'react';
import {
    AlertTriangle, CheckCircle, XCircle, ChevronDown, ChevronRight,
    BookOpen, ArrowRight, HelpCircle, Wrench, ClipboardList, BarChart3
} from 'lucide-react';

/**
 * DiffView - показывает различия между original и repaired текстом
 */
const DiffView = ({ original, repaired }) => {
    if (!original || !repaired) return null;

    return (
        <div className="mt-2 p-2 bg-slate-50 rounded border border-slate-200 text-xs font-mono">
            <div className="flex items-start gap-2 mb-1">
                <span className="text-rose-600 font-bold shrink-0">−</span>
                <span className="text-rose-700 line-through break-all">{original}</span>
            </div>
            <div className="flex items-start gap-2">
                <span className="text-emerald-600 font-bold shrink-0">+</span>
                <span className="text-emerald-700 break-all">{repaired}</span>
            </div>
        </div>
    );
};

/**
 * ConfidenceBadge - показывает уровень уверенности
 */
const ConfidenceBadge = ({ confidence, matchType }) => {
    if (confidence === undefined || confidence === null) return null;

    const pct = Math.round(confidence * 100);
    let color = 'bg-slate-100 text-slate-600';
    if (pct >= 90) color = 'bg-emerald-100 text-emerald-700';
    else if (pct >= 70) color = 'bg-amber-100 text-amber-700';
    else if (pct >= 50) color = 'bg-orange-100 text-orange-700';
    else color = 'bg-rose-100 text-rose-700';

    return (
        <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded ${color}`}>
            {pct}%
        </span>
    );
};

export default function VerificationPanel({ verification, onJumpToPage }) {
    const [isExpanded, setIsExpanded] = useState(true);
    const [showAuditLog, setShowAuditLog] = useState(false);
    const [showRepairs, setShowRepairs] = useState(true);

    if (!verification) return null;

    const {
        status,
        success_rate,
        total,
        verified_count,
        failed_count = 0,
        wrong_page_count = 0,
        overflow_count = 0,
        ambiguous_count = 0,
        invalid_format_count = 0,
        repaired_count = 0,
        failed = [],
        wrong_page = [],
        overflow = [],
        ambiguous = [],
        invalid_format = [],
        repaired = [],
        audit_summary = {}
    } = verification;

    const actualRepairedCount = repaired_count || repaired.length;
    const hasIssues = failed_count > 0 || wrong_page_count > 0 || overflow_count > 0 ||
                      ambiguous_count > 0 || invalid_format_count > 0 || actualRepairedCount > 0;
    const hasCriticalIssues = failed_count > 0 || invalid_format_count > 0;
    const hasRepairs = actualRepairedCount > 0;
    const isSuccess = status === "VERIFIED" || status === "VERIFIED_WITH_OVERFLOW" || status === "VERIFIED_WITH_REPAIRS";
    const isHighSuccess = success_rate >= 0.9;
    const needsReview = (ambiguous_count > 0 || overflow_count > 0) && !hasCriticalIssues;

    // Стили
    const bgColor = isSuccess || (isHighSuccess && !hasCriticalIssues) ? 'bg-emerald-50' :
                    hasRepairs && !hasCriticalIssues ? 'bg-blue-50' :
                    needsReview ? 'bg-amber-50' :
                    hasCriticalIssues ? 'bg-rose-50' : 'bg-slate-50';
    const borderColor = isSuccess || (isHighSuccess && !hasCriticalIssues) ? 'border-emerald-200' :
                        hasRepairs && !hasCriticalIssues ? 'border-blue-200' :
                        needsReview ? 'border-amber-200' :
                        hasCriticalIssues ? 'border-rose-200' : 'border-slate-200';
    const textColor = isSuccess || (isHighSuccess && !hasCriticalIssues) ? 'text-emerald-700' :
                      hasRepairs && !hasCriticalIssues ? 'text-blue-700' :
                      needsReview ? 'text-amber-700' :
                      hasCriticalIssues ? 'text-rose-700' : 'text-slate-700';
    const Icon = isSuccess ? CheckCircle :
                 hasRepairs && !hasCriticalIssues ? Wrench :
                 isHighSuccess && !hasCriticalIssues ? CheckCircle :
                 needsReview ? HelpCircle :
                 hasCriticalIssues ? XCircle : AlertTriangle;

    // Status label
    let statusLabel;
    if (isSuccess && !hasRepairs) {
        statusLabel = 'All Citations Verified';
    } else if (status === "VERIFIED_WITH_REPAIRS" || (hasRepairs && !hasCriticalIssues && failed_count === 0)) {
        statusLabel = `Verified (${actualRepairedCount} auto-repaired)`;
    } else if (isHighSuccess && !hasCriticalIssues) {
        statusLabel = ambiguous_count > 0 ?
            `Verified (${ambiguous_count} need review)` :
            `Verified (${Math.round(success_rate * 100)}%)`;
    } else if (failed_count === 0 && invalid_format_count === 0 && wrong_page_count === 0) {
        statusLabel = 'Needs Manual Review';
    } else if (success_rate >= 0.9) {
        statusLabel = `Mostly Verified (${Math.round(success_rate * 100)}%)`;
    } else if (success_rate >= 0.7) {
        statusLabel = `Partially Verified (${Math.round(success_rate * 100)}%)`;
    } else {
        statusLabel = 'Verification Failed';
    }

    return (
        <div className={`${bgColor} ${borderColor} border rounded-xl mt-4 overflow-hidden`}>
            {/* Header */}
            <div
                className="px-4 py-3 flex items-center justify-between cursor-pointer hover:opacity-80"
                onClick={() => setIsExpanded(!isExpanded)}
            >
                <div className="flex items-center gap-3">
                    <Icon className={textColor} size={20} />
                    <span className={`font-semibold ${textColor}`}>{statusLabel}</span>
                </div>
                <div className="flex items-center gap-3">
                    <span className={`text-sm font-mono ${textColor} opacity-70`}>
                        {verified_count + actualRepairedCount}/{total} verified ({Math.round(success_rate * 100)}%)
                    </span>
                    {hasIssues && (
                        isExpanded ? <ChevronDown size={18} className={textColor} /> : <ChevronRight size={18} className={textColor} />
                    )}
                </div>
            </div>

            {/* Details */}
            {hasIssues && isExpanded && (
                <div className="border-t border-inherit">

                    {/* Audit Log Toggle */}
                    <div className="border-b border-inherit">
                        <button
                            onClick={(e) => { e.stopPropagation(); setShowAuditLog(!showAuditLog); }}
                            className="w-full px-4 py-2 flex items-center gap-2 text-xs font-bold text-slate-500 hover:bg-slate-100 transition-colors uppercase tracking-wider"
                        >
                            <BarChart3 size={14} />
                            {showAuditLog ? "Hide Audit Log" : "Show Audit Log"}
                            <span className="ml-auto font-normal normal-case text-slate-400">
                                {audit_summary.exact_matches || 0} exact, {audit_summary.ngram_matches || 0} fuzzy
                            </span>
                            {showAuditLog ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                        </button>

                        {showAuditLog && (
                            <div className="px-4 py-3 bg-slate-50 border-t border-slate-200">
                                <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
                                    <div className="bg-white p-2 rounded border border-slate-200">
                                        <div className="text-slate-500">Total Citations</div>
                                        <div className="text-lg font-bold text-slate-700">{total}</div>
                                    </div>
                                    <div className="bg-white p-2 rounded border border-emerald-200">
                                        <div className="text-emerald-600">Exact Matches</div>
                                        <div className="text-lg font-bold text-emerald-700">{audit_summary.exact_matches || 0}</div>
                                    </div>
                                    <div className="bg-white p-2 rounded border border-blue-200">
                                        <div className="text-blue-600">Fuzzy Matches</div>
                                        <div className="text-lg font-bold text-blue-700">{audit_summary.ngram_matches || 0}</div>
                                    </div>
                                    <div className="bg-white p-2 rounded border border-purple-200">
                                        <div className="text-purple-600">Auto-Repaired</div>
                                        <div className="text-lg font-bold text-purple-700">{actualRepairedCount}</div>
                                    </div>
                                </div>
                                {audit_summary.average_confidence !== undefined && (
                                    <div className="mt-3 text-xs text-slate-500">
                                        Average Confidence: <span className="font-bold text-slate-700">{Math.round(audit_summary.average_confidence * 100)}%</span>
                                    </div>
                                )}
                            </div>
                        )}
                    </div>

                    <div className="px-4 py-3 space-y-4">

                        {/* Repaired Citations (with Diff) */}
                        {repaired.length > 0 && (
                            <div>
                                <button
                                    onClick={(e) => { e.stopPropagation(); setShowRepairs(!showRepairs); }}
                                    className="flex items-center gap-2 text-sm font-bold text-blue-800 mb-2 hover:text-blue-600"
                                >
                                    <Wrench size={14} />
                                    Auto-Repaired ({repaired.length})
                                    {showRepairs ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                                </button>
                                {showRepairs && (
                                    <div className="space-y-2">
                                        {repaired.map((item, idx) => (
                                            <div key={idx} className="bg-white rounded-lg p-3 border border-blue-200 text-sm">
                                                <div className="flex items-start justify-between gap-2">
                                                    <div className="flex-1">
                                                        <div className="flex items-center gap-2 text-slate-600 mb-1">
                                                            <span className="font-mono text-blue-600">Page {item.page}</span>
                                                            <span className="text-blue-500 text-xs">— {item.repair_reason || 'auto-repaired'}</span>
                                                            <ConfidenceBadge confidence={item.confidence} />
                                                        </div>
                                                        <DiffView
                                                            original={item.original_phrase}
                                                            repaired={item.repaired_phrase}
                                                        />
                                                    </div>
                                                    {onJumpToPage && (
                                                        <button
                                                            onClick={(e) => {
                                                                e.stopPropagation();
                                                                onJumpToPage(item.page, item.repaired_phrase || item.phrase);
                                                            }}
                                                            className="flex items-center gap-1 px-2 py-1 rounded bg-blue-100 text-blue-700 text-xs font-bold hover:bg-blue-200 transition-colors shrink-0"
                                                        >
                                                            <BookOpen size={12} /> p.{item.page}
                                                        </button>
                                                    )}
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>
                        )}

                        {/* Ambiguous Citations */}
                        {ambiguous.length > 0 && (
                            <div>
                                <h4 className="text-sm font-bold text-purple-800 mb-2 flex items-center gap-2">
                                    <HelpCircle size={14} />
                                    Ambiguous - Multiple Pages ({ambiguous.length})
                                </h4>
                                <div className="space-y-2">
                                    {ambiguous.map((item, idx) => (
                                        <div key={idx} className="bg-white rounded-lg p-3 border border-purple-200 text-sm">
                                            <div className="flex items-start justify-between gap-2">
                                                <div className="flex-1">
                                                    <div className="flex items-center gap-2 text-slate-600 mb-1">
                                                        <span className="font-mono text-purple-600">Page {item.page}</span>
                                                        <span className="text-purple-500 text-xs">
                                                            — found on: {(item.suggested_pages || []).join(', ')}
                                                        </span>
                                                        <ConfidenceBadge confidence={item.confidence} />
                                                    </div>
                                                    <div className="text-slate-500 text-xs italic truncate max-w-md" title={item.phrase}>
                                                        "{item.phrase}"
                                                    </div>
                                                    <div className="text-purple-600 text-xs mt-1 font-medium">
                                                        ⚠️ Verify which page context matches the statement
                                                    </div>
                                                </div>
                                                <div className="flex flex-col gap-1">
                                                    {(item.suggested_pages || []).slice(0, 3).map((pg) => (
                                                        <button
                                                            key={pg}
                                                            onClick={(e) => {
                                                                e.stopPropagation();
                                                                onJumpToPage && onJumpToPage(pg, item.phrase);
                                                            }}
                                                            className={`flex items-center gap-1 px-2 py-1 rounded text-xs font-bold transition-colors ${
                                                                pg === item.page 
                                                                    ? 'bg-purple-100 text-purple-700 hover:bg-purple-200' 
                                                                    : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                                                            }`}
                                                        >
                                                            <BookOpen size={10} /> p.{pg}
                                                        </button>
                                                    ))}
                                                </div>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Overflow Citations */}
                        {overflow.length > 0 && (
                            <div>
                                <h4 className="text-sm font-bold text-blue-800 mb-2 flex items-center gap-2">
                                    <ArrowRight size={14} />
                                    Cross-Page ({overflow.length})
                                </h4>
                                <div className="space-y-2">
                                    {overflow.map((item, idx) => (
                                        <div key={idx} className="bg-white rounded-lg p-3 border border-blue-200 text-sm">
                                            <div className="flex items-start justify-between gap-2">
                                                <div className="flex-1">
                                                    <div className="flex items-center gap-2 text-slate-600 mb-1">
                                                        <span className="font-mono text-blue-600">Page {item.page}</span>
                                                        <ArrowRight size={14} className="text-blue-400" />
                                                        <span className="font-mono text-blue-600">Page {item.overflow_to}</span>
                                                        <ConfidenceBadge confidence={item.confidence} />
                                                    </div>
                                                    <div className="text-slate-500 text-xs italic truncate max-w-md" title={item.phrase}>
                                                        "{item.phrase}"
                                                    </div>
                                                </div>
                                                <div className="flex gap-1">
                                                    {onJumpToPage && (
                                                        <>
                                                            <button
                                                                onClick={(e) => { e.stopPropagation(); onJumpToPage(item.page, item.phrase); }}
                                                                className="flex items-center gap-1 px-2 py-1 rounded bg-blue-100 text-blue-700 text-xs font-bold hover:bg-blue-200"
                                                            >
                                                                <BookOpen size={10} /> p.{item.page}
                                                            </button>
                                                            <button
                                                                onClick={(e) => { e.stopPropagation(); onJumpToPage(item.overflow_to, item.phrase); }}
                                                                className="flex items-center gap-1 px-2 py-1 rounded bg-blue-100 text-blue-700 text-xs font-bold hover:bg-blue-200"
                                                            >
                                                                <BookOpen size={10} /> p.{item.overflow_to}
                                                            </button>
                                                        </>
                                                    )}
                                                </div>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Invalid Format */}
                        {invalid_format.length > 0 && (
                            <div>
                                <h4 className="text-sm font-bold text-orange-800 mb-2 flex items-center gap-2">
                                    <XCircle size={14} />
                                    Invalid Format ({invalid_format.length})
                                </h4>
                                <div className="space-y-2">
                                    {invalid_format.map((item, idx) => (
                                        <div key={idx} className="bg-white rounded-lg p-3 border border-orange-200 text-sm">
                                            <div className="text-slate-600 mb-1">
                                                <span className="font-mono text-orange-600">Page {item.page}</span>
                                                <span className="text-orange-500 ml-2 text-xs">— NON-CONTINUOUS</span>
                                            </div>
                                            <div className="text-slate-500 text-xs italic truncate max-w-md" title={item.phrase}>
                                                "{item.phrase}"
                                            </div>
                                            <div className="text-orange-600 text-xs mt-1">
                                                ⚠️ Phrase contains "..." — must be continuous from source
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Wrong Page */}
                        {wrong_page.length > 0 && (
                            <div>
                                <h4 className="text-sm font-bold text-amber-800 mb-2 flex items-center gap-2">
                                    <AlertTriangle size={14} />
                                    Wrong Page ({wrong_page.length})
                                </h4>
                                <div className="space-y-2">
                                    {wrong_page.map((item, idx) => (
                                        <div key={idx} className="bg-white rounded-lg p-3 border border-amber-200 text-sm">
                                            <div className="flex items-start justify-between gap-2">
                                                <div className="flex-1">
                                                    <div className="flex items-center gap-2 text-slate-600 mb-1">
                                                        <span className="font-mono text-rose-600 line-through">Page {item.page}</span>
                                                        <ArrowRight size={14} className="text-slate-400" />
                                                        <span className="font-mono text-emerald-600 font-bold">Page {item.suggested_page}</span>
                                                        <ConfidenceBadge confidence={item.confidence} />
                                                    </div>
                                                    <div className="text-slate-500 text-xs italic truncate max-w-md" title={item.phrase}>
                                                        "{item.phrase}"
                                                    </div>
                                                </div>
                                                {onJumpToPage && item.suggested_page && (
                                                    <button
                                                        onClick={(e) => { e.stopPropagation(); onJumpToPage(item.suggested_page, item.phrase); }}
                                                        className="flex items-center gap-1 px-2 py-1 rounded bg-emerald-100 text-emerald-700 text-xs font-bold hover:bg-emerald-200"
                                                    >
                                                        <BookOpen size={12} /> Go to p.{item.suggested_page}
                                                    </button>
                                                )}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Not Found */}
                        {failed.length > 0 && (
                            <div>
                                <h4 className="text-sm font-bold text-rose-800 mb-2 flex items-center gap-2">
                                    <XCircle size={14} />
                                    Not Found ({failed.length})
                                </h4>
                                <div className="space-y-2">
                                    {failed.map((item, idx) => (
                                        <div key={idx} className="bg-white rounded-lg p-3 border border-rose-200 text-sm">
                                            <div className="flex items-start justify-between gap-2">
                                                <div className="flex-1">
                                                    <div className="text-slate-600 mb-1">
                                                        <span className="font-mono text-rose-600">Page {item.page}</span>
                                                        <span className="text-rose-500 ml-2 text-xs">— NOT FOUND</span>
                                                    </div>
                                                    <div className="text-slate-500 text-xs italic truncate max-w-md" title={item.phrase}>
                                                        "{item.phrase}"
                                                    </div>
                                                    <div className="text-rose-500 text-xs mt-1">{item.reason}</div>
                                                </div>
                                                {onJumpToPage && (
                                                    <button
                                                        onClick={(e) => { e.stopPropagation(); onJumpToPage(item.page, item.phrase); }}
                                                        className="flex items-center gap-1 px-2 py-1 rounded bg-slate-100 text-slate-600 text-xs font-bold hover:bg-slate-200"
                                                    >
                                                        <BookOpen size={12} /> Check p.{item.page}
                                                    </button>
                                                )}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Summary */}
                        <div className="text-xs text-slate-500 pt-2 border-t border-slate-200">
                            <strong>Summary:</strong> {verified_count} verified
                            {actualRepairedCount > 0 && `, ${actualRepairedCount} repaired`}
                            {overflow_count > 0 && `, ${overflow_count} overflow`}
                            {wrong_page_count > 0 && `, ${wrong_page_count} wrong page`}
                            {ambiguous_count > 0 && `, ${ambiguous_count} ambiguous`}
                            {invalid_format_count > 0 && `, ${invalid_format_count} invalid`}
                            {failed_count > 0 && `, ${failed_count} not found`}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}