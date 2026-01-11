import React, { useState, useEffect, useMemo } from 'react';
import {
  ChevronDown, ChevronRight, CheckCircle, Clock, AlertCircle,
  Loader2, X, FileText, ShieldCheck, Gavel, BookOpen, BrainCircuit,
  Users, Target, Swords
} from 'lucide-react';
import StreamBlock from './StreamBlock';

// =============================================================================
// STEP CONFIGURATION
// =============================================================================

const STEP_CONFIG = {
  draft: {
    title: 'Draft Generation',
    icon: FileText,
    variant: 'gray',
    description: 'Creating initial protocol from sources'
  },
  mentor: {
    title: 'Mentor Review',
    icon: Users,
    variant: 'blue',
    description: 'Checking usability and completeness'
  },
  redteam: {
    title: 'Red Team Analysis',
    icon: Swords,
    variant: 'blue',
    description: 'Finding vulnerabilities and edge cases'
  },
  arbitration: {
    title: 'Arbitration',
    icon: Gavel,
    variant: 'gray',
    description: 'Resolving conflicts between validators'
  },
  finalization: {
    title: 'Finalization',
    icon: ShieldCheck,
    variant: 'green',
    description: 'Applying corrections and verifying citations'
  }
};

const PIPELINE_ORDER = ['draft', 'mentor', 'redteam', 'arbitration', 'finalization'];

// =============================================================================
// SINGLE CODE PROGRESS
// =============================================================================

const CodeProgress = ({ code, codeProgress, isExpanded, onToggle }) => {
  const [expandedSteps, setExpandedSteps] = useState(new Set(['draft']));

  const status = codeProgress?.status || 'pending';
  const steps = codeProgress?.steps || {};

  // Determine current step
  const currentStep = useMemo(() => {
    for (const step of PIPELINE_ORDER) {
      const stepData = steps[step];
      if (stepData?.status === 'streaming' || stepData?.status === 'loading') {
        return step;
      }
    }
    // Find last completed step
    for (let i = PIPELINE_ORDER.length - 1; i >= 0; i--) {
      if (steps[PIPELINE_ORDER[i]]?.status === 'done') {
        return PIPELINE_ORDER[i];
      }
    }
    return null;
  }, [steps]);

  // Calculate total duration
  const totalDuration = useMemo(() => {
    let total = 0;
    Object.values(steps).forEach(step => {
      if (step.duration_ms) total += step.duration_ms;
    });
    return total;
  }, [steps]);

  // Group validators (mentor + redteam run in parallel)
  const validatorsParallel = steps.mentor || steps.redteam;

  const toggleStep = (step) => {
    setExpandedSteps(prev => {
      const next = new Set(prev);
      if (next.has(step)) {
        next.delete(step);
      } else {
        next.add(step);
      }
      return next;
    });
  };

  const getStepStatus = (stepName) => {
    const stepData = steps[stepName];
    if (!stepData) return 'idle';
    return stepData.status || 'idle';
  };

  const formatDuration = (ms) => {
    if (!ms) return '';
    return `${(ms / 1000).toFixed(1)}s`;
  };

  return (
    <div className="border rounded-xl overflow-hidden bg-white shadow-sm">
      {/* Header */}
      <button
        onClick={onToggle}
        className={`w-full px-4 py-3 flex items-center justify-between transition-colors ${
          status === 'complete' 
            ? 'bg-green-50 hover:bg-green-100' 
            : status === 'error'
              ? 'bg-red-50 hover:bg-red-100'
              : status === 'generating'
                ? 'bg-blue-50 hover:bg-blue-100'
                : 'bg-gray-50 hover:bg-gray-100'
        }`}
      >
        <div className="flex items-center gap-3">
          {status === 'complete' ? (
            <CheckCircle className="w-5 h-5 text-green-500" />
          ) : status === 'error' ? (
            <AlertCircle className="w-5 h-5 text-red-500" />
          ) : status === 'generating' ? (
            <Loader2 className="w-5 h-5 animate-spin text-blue-500" />
          ) : (
            <Clock className="w-5 h-5 text-gray-300" />
          )}
          <span className="font-mono font-bold text-lg">{code}</span>

          {/* Mini pipeline status */}
          <div className="flex items-center gap-1 ml-4">
            {PIPELINE_ORDER.map((step, i) => {
              // Skip redteam in mini view (shown with mentor)
              if (step === 'redteam') return null;

              const stepStatus = getStepStatus(step);
              const isValidator = step === 'mentor';

              return (
                <React.Fragment key={step}>
                  <div
                    className={`w-2 h-2 rounded-full transition-colors ${
                      stepStatus === 'done' 
                        ? 'bg-green-500' 
                        : stepStatus === 'streaming' || stepStatus === 'loading'
                          ? 'bg-blue-500 animate-pulse' 
                          : 'bg-gray-300'
                    }`}
                    title={STEP_CONFIG[step]?.title}
                  />
                  {i < PIPELINE_ORDER.length - 2 && step !== 'redteam' && (
                    <div className={`w-3 h-0.5 ${
                      stepStatus === 'done' ? 'bg-green-300' : 'bg-gray-200'
                    }`} />
                  )}
                </React.Fragment>
              );
            })}
          </div>
        </div>

        <div className="flex items-center gap-3">
          {totalDuration > 0 && (
            <span className="text-xs text-gray-500 font-mono">
              {formatDuration(totalDuration)}
            </span>
          )}
          {isExpanded ? (
            <ChevronDown className="w-5 h-5 text-gray-400" />
          ) : (
            <ChevronRight className="w-5 h-5 text-gray-400" />
          )}
        </div>
      </button>

      {/* Expanded content */}
      {isExpanded && (
        <div className="p-4 space-y-3 bg-gray-50/50">
          {/* Draft */}
          <StepBlock
            stepName="draft"
            stepData={steps.draft}
            isExpanded={expandedSteps.has('draft')}
            onToggle={() => toggleStep('draft')}
          />

          {/* Validators (parallel) */}
          {(steps.mentor || steps.redteam || currentStep === 'mentor' || currentStep === 'redteam') && (
            <div className="border rounded-lg overflow-hidden bg-white">
              <div className="px-4 py-2 bg-blue-50 border-b flex items-center gap-2">
                <Target className="w-4 h-4 text-blue-600" />
                <span className="text-sm font-semibold text-blue-800">Validation</span>
                <span className="text-xs text-blue-600">(parallel)</span>
                {(getStepStatus('mentor') === 'streaming' || getStepStatus('redteam') === 'streaming') && (
                  <Loader2 className="w-3 h-3 animate-spin text-blue-500 ml-auto" />
                )}
              </div>
              <div className="p-3 grid grid-cols-1 lg:grid-cols-2 gap-3">
                <StepBlock
                  stepName="mentor"
                  stepData={steps.mentor}
                  isExpanded={expandedSteps.has('mentor')}
                  onToggle={() => toggleStep('mentor')}
                  compact
                />
                <StepBlock
                  stepName="redteam"
                  stepData={steps.redteam}
                  isExpanded={expandedSteps.has('redteam')}
                  onToggle={() => toggleStep('redteam')}
                  compact
                />
              </div>
            </div>
          )}

          {/* Arbitration */}
          {(steps.arbitration || currentStep === 'arbitration') && (
            <StepBlock
              stepName="arbitration"
              stepData={steps.arbitration}
              isExpanded={expandedSteps.has('arbitration')}
              onToggle={() => toggleStep('arbitration')}
            />
          )}

          {/* Finalization */}
          {(steps.finalization || currentStep === 'finalization') && (
            <StepBlock
              stepName="finalization"
              stepData={steps.finalization}
              isExpanded={expandedSteps.has('finalization')}
              onToggle={() => toggleStep('finalization')}
            />
          )}

          {/* Error message */}
          {codeProgress?.error && (
            <div className="p-3 bg-red-50 border border-red-200 rounded-lg">
              <p className="text-sm text-red-700">{codeProgress.error}</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

// =============================================================================
// STEP BLOCK (wrapper for StreamBlock or compact view)
// =============================================================================

const StepBlock = ({ stepName, stepData, isExpanded, onToggle, compact = false }) => {
  const config = STEP_CONFIG[stepName];
  if (!config) return null;

  const status = stepData?.status || 'idle';
  const Icon = config.icon;

  const formatDuration = (ms) => {
    if (!ms) return '';
    return `${(ms / 1000).toFixed(1)}s`;
  };

  // Map our status to StreamBlock status
  const streamStatus = status === 'streaming' || status === 'loading' ? 'loading' : status;

  if (compact) {
    // Compact view for parallel validators
    return (
      <div className={`border rounded-lg overflow-hidden ${
        status === 'done' ? 'border-green-200' : 
        status === 'streaming' ? 'border-blue-200' : 
        'border-gray-200'
      }`}>
        <button
          onClick={onToggle}
          className={`w-full px-3 py-2 flex items-center justify-between text-left transition-colors ${
            status === 'done' ? 'bg-green-50 hover:bg-green-100' :
            status === 'streaming' ? 'bg-blue-50 hover:bg-blue-100' :
            'bg-gray-50 hover:bg-gray-100'
          }`}
        >
          <div className="flex items-center gap-2">
            {status === 'streaming' || status === 'loading' ? (
              <Loader2 className="w-4 h-4 animate-spin text-blue-500" />
            ) : status === 'done' ? (
              <CheckCircle className="w-4 h-4 text-green-500" />
            ) : (
              <Icon className="w-4 h-4 text-gray-400" />
            )}
            <span className="text-sm font-medium">{config.title}</span>
          </div>
          <div className="flex items-center gap-2">
            {stepData?.duration_ms && (
              <span className="text-xs text-gray-500">{formatDuration(stepData.duration_ms)}</span>
            )}
            {isExpanded ? (
              <ChevronDown className="w-4 h-4 text-gray-400" />
            ) : (
              <ChevronRight className="w-4 h-4 text-gray-400" />
            )}
          </div>
        </button>

        {isExpanded && (
          <div className="p-3 border-t bg-white max-h-80 overflow-auto">
            {/* Thinking */}
            {stepData?.thinking && (
              <div className="mb-3">
                <div className="flex items-center gap-1 text-xs text-gray-500 mb-1">
                  <BrainCircuit className="w-3 h-3" />
                  <span>Reasoning</span>
                </div>
                <div className="text-xs text-gray-600 bg-gray-50 p-2 rounded max-h-32 overflow-auto font-mono">
                  {stepData.thinking.slice(0, 500)}
                  {stepData.thinking.length > 500 && '...'}
                </div>
              </div>
            )}

            {/* Content preview */}
            {stepData?.content && (
              <div>
                <div className="flex items-center gap-1 text-xs text-gray-500 mb-1">
                  <FileText className="w-3 h-3" />
                  <span>Output</span>
                </div>
                <div className="text-xs text-gray-700 bg-gray-50 p-2 rounded max-h-40 overflow-auto">
                  {stepData.content.slice(0, 300)}
                  {stepData.content.length > 300 && '...'}
                </div>
              </div>
            )}

            {/* Corrections count for validators */}
            {stepData?.corrections_count !== undefined && (
              <div className="mt-2 text-xs">
                <span className={`px-2 py-1 rounded ${
                  stepData.corrections_count > 0 
                    ? 'bg-yellow-100 text-yellow-700' 
                    : 'bg-green-100 text-green-700'
                }`}>
                  {stepData.corrections_count} corrections
                </span>
              </div>
            )}

            {!stepData?.content && status === 'streaming' && (
              <div className="flex items-center gap-2 text-sm text-blue-600">
                <Loader2 className="w-4 h-4 animate-spin" />
                <span>Generating...</span>
              </div>
            )}
          </div>
        )}
      </div>
    );
  }

  // Full StreamBlock for main steps
  return (
    <StreamBlock
      title={config.title}
      status={streamStatus}
      thoughts={stepData?.thinking || ''}
      content={stepData?.content || ''}
      variant={config.variant}
      shouldCollapse={status === 'done'}
    />
  );
};

// =============================================================================
// MAIN COMPONENT
// =============================================================================

const GenerationProgress = ({ isOpen, codes = [], progress = {}, onClose, generating }) => {
  // ALL HOOKS MUST BE AT THE TOP, BEFORE ANY CONDITIONAL RETURNS
  const [expandedCodes, setExpandedCodes] = useState(new Set());

  // Compute derived values
  const completedCount = Object.values(progress).filter(p => p.status === 'complete').length;
  const errorCount = Object.values(progress).filter(p => p.status === 'error').length;

  // Auto-expand first code when codes change
  useEffect(() => {
    if (codes.length > 0 && expandedCodes.size === 0) {
      setExpandedCodes(new Set([codes[0]]));
    }
  }, [codes]);

  // Auto-expand currently generating code
  useEffect(() => {
    const generatingCode = codes.find(code => progress[code]?.status === 'generating');
    if (generatingCode && !expandedCodes.has(generatingCode)) {
      setExpandedCodes(prev => new Set([...prev, generatingCode]));
    }
  }, [progress, codes]);

  // Toggle function
  const toggleCode = (code) => {
    setExpandedCodes(prev => {
      const next = new Set(prev);
      if (next.has(code)) {
        next.delete(code);
      } else {
        next.add(code);
      }
      return next;
    });
  };

  // EARLY RETURN AFTER ALL HOOKS
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl w-full max-w-4xl max-h-[90vh] overflow-hidden shadow-2xl flex flex-col">
        {/* Header */}
        <div className="p-4 border-b flex items-center justify-between bg-gray-50">
          <div>
            <h3 className="font-semibold text-lg">Rule Generation Pipeline</h3>
            <p className="text-sm text-gray-500">
              {generating ? 'Processing...' : 'Complete'} • {completedCount}/{codes.length} done
              {errorCount > 0 && <span className="text-red-500 ml-2">• {errorCount} errors</span>}
            </p>
          </div>
          <button
            onClick={onClose}
            disabled={generating}
            className={`p-2 rounded-lg transition-colors ${
              generating 
                ? 'text-gray-300 cursor-not-allowed' 
                : 'hover:bg-gray-200 text-gray-500'
            }`}
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-auto p-4 space-y-3">
          {codes.map((code) => (
            <CodeProgress
              key={code}
              code={code}
              codeProgress={progress[code]}
              isExpanded={expandedCodes.has(code)}
              onToggle={() => toggleCode(code)}
            />
          ))}
        </div>

        {/* Footer */}
        <div className="p-4 border-t bg-gray-50 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 rounded-full bg-green-500" />
              <span className="text-sm text-gray-600">Complete</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 rounded-full bg-blue-500 animate-pulse" />
              <span className="text-sm text-gray-600">In progress</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 rounded-full bg-gray-300" />
              <span className="text-sm text-gray-600">Pending</span>
            </div>
          </div>

          {!generating && (
            <button
              onClick={onClose}
              className="px-4 py-2 bg-gray-900 text-white rounded-lg hover:bg-gray-800 transition-colors"
            >
              Close
            </button>
          )}
        </div>
      </div>
    </div>
  );
};

export default GenerationProgress;