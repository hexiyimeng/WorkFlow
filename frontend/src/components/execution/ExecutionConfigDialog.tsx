import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
} from 'react';
import { useFlow } from '../../hooks/useFlowContext';
import type {
  ExecutionMode,
  ExecutionPreflightResponse,
  RecoveryLocation,
  RecoverySummary,
  ResourceNodeRequirement,
  ResumeAction,
} from '../../types';
import {
  calculateWindowGridShape,
  estimateWindowCount,
  isAbsoluteServerPath,
  isValidMaxInFlightWindows,
  isValidWindowShape,
  preflightResourcesAllowExecution,
  preflightOutputShape,
  resolveRecoveryDirectory,
  sameServerPath,
} from '../../utils/executionConfig';
import { Button } from '../ui/Button';

const FOCUSABLE_SELECTOR = [
  'button:not([disabled])',
  'input:not([disabled])',
  '[href]',
  '[tabindex]:not([tabindex="-1"])',
].join(',');

const EMPTY_OUTPUT_SHAPE: number[] = [];

const plannedWorkerCount = (
  declaredWorkers: number | undefined,
  nodes: readonly ResourceNodeRequirement[],
  required: boolean,
): number => {
  if (Number.isSafeInteger(declaredWorkers) && Number(declaredWorkers) >= 0) {
    return Number(declaredWorkers);
  }
  const nodeWorkers = nodes.reduce((total, node) => (
    Number.isSafeInteger(node.workers) && Number(node.workers) > 0
      ? total + Number(node.workers)
      : total + 1
  ), 0);
  return required ? Math.max(1, nodeWorkers) : 0;
};

export default function ExecutionConfigDialog() {
  const { executionPreflight } = useFlow();
  if (!executionPreflight) return null;
  return <ExecutionConfigDialogContent executionPreflight={executionPreflight} />;
}

function ExecutionConfigDialogContent({
  executionPreflight,
}: {
  executionPreflight: ExecutionPreflightResponse;
}) {
  const {
    confirmExecution,
    cancelExecutionDialog,
    inspectRecoveryDirectory,
    openRecoveryBrowser,
    isConnected,
    isExecuting,
  } = useFlow();
  const outputShape = preflightOutputShape(executionPreflight) ?? EMPTY_OUTPUT_SHAPE;
  const outputs = executionPreflight.outputs ?? [];
  const [mode, setMode] = useState<ExecutionMode>('full_graph');
  const [windowInputs, setWindowInputs] = useState<string[]>(() => outputShape.map(() => '1'));
  const [maxInFlightInput, setMaxInFlightInput] = useState('');
  const [resumeAction, setResumeAction] = useState<ResumeAction>('new');
  const [recoveryMode, setRecoveryMode] = useState<'output_sidecar' | 'custom'>(
    outputs.length > 0 ? 'output_sidecar' : 'custom',
  );
  const [anchorNodeId, setAnchorNodeId] = useState(outputs[0]?.nodeId ?? '');
  const [customDirectory, setCustomDirectory] = useState('');
  const [inspection, setInspection] = useState<RecoverySummary | null>(null);
  const [inspectionError, setInspectionError] = useState<string | null>(null);
  const [isInspecting, setIsInspecting] = useState(false);
  const dialogRef = useRef<HTMLDivElement>(null);
  const openerRef = useRef<HTMLElement | null>(null);

  const windowAvailable = Boolean(
    executionPreflight.windowable
    && outputShape.length > 0
    && outputs.length > 0,
  );

  useEffect(() => {
    openerRef.current = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    const frame = requestAnimationFrame(() => {
      dialogRef.current?.querySelector<HTMLElement>('[data-autofocus]')?.focus();
    });
    return () => {
      cancelAnimationFrame(frame);
      openerRef.current?.focus();
    };
  }, []);

  const parsedWindowShape = useMemo(
    () => windowInputs.map(value => Number(value)),
    [windowInputs],
  );
  const windowShapeValid = isValidWindowShape(outputShape, parsedWindowShape);
  const parsedMaxInFlight = maxInFlightInput.trim() === ''
    ? null
    : Number(maxInFlightInput);
  const maxInFlightValid = parsedMaxInFlight === null
    || isValidMaxInFlightWindows(parsedMaxInFlight);
  const resourceRequirements = executionPreflight.requiredResources;
  const availableResources = executionPreflight.availableResources;
  const resourcesSatisfied = preflightResourcesAllowExecution(executionPreflight);
  const plannedCpuWorkers = resourceRequirements
    ? plannedWorkerCount(
      resourceRequirements.cpuWorkers,
      resourceRequirements.cpuNodes,
      resourceRequirements.requiresCpu,
    )
    : 0;
  const plannedGpuWorkers = resourceRequirements
    ? plannedWorkerCount(
      resourceRequirements.gpuWorkers,
      resourceRequirements.gpuNodes,
      resourceRequirements.requiresGpu,
    )
    : 0;
  const unconstrainedNodes = resourceRequirements?.anyNodes ?? [];
  const hasCpuConstrainedNodes = (resourceRequirements?.cpuNodes.length ?? 0) > 0;
  const hasGpuConstrainedNodes = (resourceRequirements?.gpuNodes.length ?? 0) > 0;
  const resourceDescription = !resourceRequirements
    ? ''
    : hasCpuConstrainedNodes && hasGpuConstrainedNodes
      ? 'This workflow has both CPU- and GPU-constrained tasks.'
      : hasCpuConstrainedNodes
        ? 'This workflow has CPU-constrained tasks.'
        : hasGpuConstrainedNodes
          ? 'This workflow has GPU-constrained tasks.'
          : 'This workflow has no resource-constrained tasks.';
  const unconstrainedDescription = unconstrainedNodes.length > 0
    ? ` ${unconstrainedNodes.length.toLocaleString()} unconstrained ${unconstrainedNodes.length === 1 ? 'node may' : 'nodes may'} run on any Worker.`
    : '';

  const serverPlanMatches = executionPreflight.windowShape?.length === parsedWindowShape.length
    && executionPreflight.windowShape.every(
      (size, index) => size === parsedWindowShape[index],
    );
  const estimatedWindows = useMemo(() => {
    if (
      serverPlanMatches
      && Number.isSafeInteger(executionPreflight.totalWindows)
      && Number(executionPreflight.totalWindows) >= 0
    ) {
      return BigInt(executionPreflight.totalWindows as number);
    }
    return estimateWindowCount(outputShape, parsedWindowShape);
  }, [executionPreflight.totalWindows, outputShape, parsedWindowShape, serverPlanMatches]);

  const windowGridShape = useMemo(() => (
    serverPlanMatches && executionPreflight.windowGridShape
      ? executionPreflight.windowGridShape
      : calculateWindowGridShape(outputShape, parsedWindowShape)
  ), [executionPreflight.windowGridShape, outputShape, parsedWindowShape, serverPlanMatches]);

  const recoveryLocation: RecoveryLocation = recoveryMode === 'output_sidecar'
    ? { mode: 'output_sidecar', anchorNodeId }
    : { mode: 'custom', directory: customDirectory.trim() };
  const resolvedRecoveryDirectory = resolveRecoveryDirectory(recoveryLocation, outputs);
  const recoveryLocationValid = recoveryMode === 'output_sidecar'
    ? outputs.some(output => output.nodeId === anchorNodeId)
    : isAbsoluteServerPath(customDirectory);
  const inspectedCurrentDirectory = sameServerPath(
    inspection?.recoveryDirectory,
    resolvedRecoveryDirectory,
  );

  const updateWindowInput = (index: number, value: string) => {
    setWindowInputs(current => current.map((item, itemIndex) => (
      itemIndex === index ? value : item
    )));
  };

  const clearInspection = () => {
    setInspection(null);
    setInspectionError(null);
  };

  const handleSubmit = () => {
    if (!isConnected || isExecuting || !resourcesSatisfied) return;
    if (mode === 'window') {
      if (
        !windowAvailable
        || !windowShapeValid
        || !maxInFlightValid
        || !recoveryLocationValid
        || (resumeAction !== 'new' && !inspectedCurrentDirectory)
      ) return;
      confirmExecution({
        mode: 'window',
        windowShape: parsedWindowShape,
        ...(parsedMaxInFlight === null
          ? {}
          : { maxInFlightWindows: parsedMaxInFlight }),
        resumeAction,
        recoveryLocation,
      });
      return;
    }
    confirmExecution({ mode: 'full_graph' });
  };

  const handleInspect = async () => {
    if (!resolvedRecoveryDirectory) return;
    setIsInspecting(true);
    setInspection(null);
    setInspectionError(null);
    try {
      setInspection(await inspectRecoveryDirectory(resolvedRecoveryDirectory));
    } catch (error) {
      setInspectionError((error as Error).message);
    } finally {
      setIsInspecting(false);
    }
  };

  const handleKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      cancelExecutionDialog();
      return;
    }
    if (event.key !== 'Tab' || !dialogRef.current) return;

    const focusable = Array.from(
      dialogRef.current.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR),
    ).filter(element => !element.hasAttribute('disabled'));
    if (focusable.length === 0) {
      event.preventDefault();
      dialogRef.current.focus();
      return;
    }

    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  };

  const windowReason = executionPreflight.reason
    || 'Every execution root must return a Dask Array with the same shape.';
  const canSubmit = isConnected && !isExecuting && resourcesSatisfied && (
    mode === 'full_graph' || (
      windowAvailable
      && windowShapeValid
      && maxInFlightValid
      && recoveryLocationValid
      && (resumeAction === 'new' || inspectedCurrentDirectory)
    )
  );

  return (
    <div
      className="fixed inset-0 z-[10000] flex items-center justify-center p-4"
      style={{ backgroundColor: 'var(--color-bg-overlay)' }}
      onMouseDown={event => {
        if (event.target === event.currentTarget) cancelExecutionDialog();
      }}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="execution-config-title"
        aria-describedby="execution-config-description"
        tabIndex={-1}
        onKeyDown={handleKeyDown}
        className="flex max-h-[calc(100vh-2rem)] w-full max-w-lg flex-col overflow-hidden rounded-[var(--radius-lg)] border"
        style={{
          backgroundColor: 'var(--color-bg-surface)',
          borderColor: 'var(--color-border-default)',
          boxShadow: 'var(--shadow-floating)',
        }}
      >
        <div
          className="flex items-start justify-between gap-4 border-b px-5 py-4"
          style={{ borderColor: 'var(--color-border-subtle)' }}
        >
          <div>
            <h2
              id="execution-config-title"
              className="text-[15px] font-semibold"
              style={{ color: 'var(--color-text-primary)' }}
            >
              Execution Configuration
            </h2>
            <p
              id="execution-config-description"
              className="mt-1 text-[11px]"
              style={{ color: 'var(--color-text-muted)' }}
            >
              Choose how this run should be submitted to Dask.
            </p>
          </div>
          <button
            type="button"
            onClick={cancelExecutionDialog}
            aria-label="Close execution configuration"
            className="flex h-7 w-7 shrink-0 items-center justify-center rounded-[var(--radius-md)] text-lg transition-colors hover:bg-[var(--color-bg-field-hover)]"
            style={{ color: 'var(--color-text-muted)' }}
          >
            X
          </button>
        </div>

        <form
          className="flex min-h-0 flex-1 flex-col"
          onSubmit={event => {
            event.preventDefault();
            handleSubmit();
          }}
        >
          <div className="space-y-5 overflow-y-auto px-5 py-5">
            {resourceRequirements && (
              <div
                className="rounded-[var(--radius-md)] border px-3 py-2.5 text-[11px]"
                style={{
                  borderColor: resourcesSatisfied
                    ? 'var(--color-border-subtle)'
                    : 'var(--color-danger)',
                  backgroundColor: resourcesSatisfied
                    ? 'var(--color-bg-field)'
                    : 'var(--color-danger-soft)',
                  color: resourcesSatisfied
                    ? 'var(--color-text-secondary)'
                    : 'var(--color-danger)',
                }}
              >
                <div className="font-semibold" style={{ color: 'var(--color-text-primary)' }}>
                  Planned Dask cluster
                </div>
                <div className="mt-1">
                  {resourceDescription}{unconstrainedDescription}
                </div>
                <div className="mt-2 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 font-mono text-[10px]">
                  <span style={{ color: 'var(--color-text-muted)' }}>Will start or reuse</span>
                  <span>
                    CPU Workers: {plannedCpuWorkers.toLocaleString()}
                    {' · '}GPU Workers: {plannedGpuWorkers.toLocaleString()}
                  </span>
                  <span style={{ color: 'var(--color-text-muted)' }}>Current cluster</span>
                  <span>
                    CPU Workers: {availableResources?.cpuWorkers ?? 'not started'}
                    {' · '}GPU Workers: {availableResources?.gpuWorkers ?? 'not started'}
                  </span>
                </div>
                {!resourcesSatisfied && (
                  <div className="mt-1.5 font-medium">
                    {executionPreflight.resourceError
                      || 'The active Dask cluster cannot satisfy this workflow.'}
                  </div>
                )}
              </div>
            )}

            <fieldset>
              <legend
                className="mb-2 text-[11px] font-semibold uppercase tracking-wide"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                Execution Mode
              </legend>
              <div className="space-y-2">
                <label
                  className="flex cursor-pointer items-start gap-3 rounded-[var(--radius-md)] border p-3"
                  style={{
                    borderColor: mode === 'full_graph' ? 'var(--color-accent)' : 'var(--color-border-default)',
                    backgroundColor: mode === 'full_graph' ? 'var(--color-accent-soft)' : 'var(--color-bg-field)',
                  }}
                >
                  <input
                    data-autofocus
                    type="radio"
                    name="execution-mode"
                    value="full_graph"
                    checked={mode === 'full_graph'}
                    onChange={() => setMode('full_graph')}
                    className="mt-0.5 accent-[var(--color-accent)]"
                  />
                  <span>
                    <span className="block text-[12px] font-medium">Full Graph Execution</span>
                    <span className="mt-0.5 block text-[10px]" style={{ color: 'var(--color-text-muted)' }}>
                      Submit the complete terminal Dask collections at once.
                    </span>
                  </span>
                </label>

                <label
                  className={`flex items-start gap-3 rounded-[var(--radius-md)] border p-3 ${windowAvailable ? 'cursor-pointer' : 'cursor-not-allowed opacity-60'}`}
                  style={{
                    borderColor: mode === 'window' ? 'var(--color-accent)' : 'var(--color-border-default)',
                    backgroundColor: mode === 'window' ? 'var(--color-accent-soft)' : 'var(--color-bg-field)',
                  }}
                >
                  <input
                    type="radio"
                    name="execution-mode"
                    value="window"
                    checked={mode === 'window'}
                    disabled={!windowAvailable}
                    aria-describedby={!windowAvailable ? 'window-unavailable-reason' : undefined}
                    onChange={() => setMode('window')}
                    className="mt-0.5 accent-[var(--color-accent)]"
                  />
                  <span>
                    <span className="block text-[12px] font-medium">Window Execution</span>
                    <span className="mt-0.5 block text-[10px]" style={{ color: 'var(--color-text-muted)' }}>
                      Submit one final-array window at a time with resumable checkpoints.
                    </span>
                  </span>
                </label>
              </div>
              {!windowAvailable && (
                <p
                  id="window-unavailable-reason"
                  className="mt-2 rounded-[var(--radius-sm)] px-2.5 py-2 text-[10px]"
                  style={{ color: 'var(--color-warning)', backgroundColor: 'var(--color-warning-soft)' }}
                >
                  Window Execution unavailable: {windowReason}
                </p>
              )}
            </fieldset>

            {mode === 'window' && windowAvailable && (
              <div className="space-y-4">
                <div>
                  <div className="text-[11px] font-semibold" style={{ color: 'var(--color-text-secondary)' }}>
                    Output Array Shape
                  </div>
                  <div
                    className="mt-1 rounded-[var(--radius-md)] border px-3 py-2 font-mono text-[12px]"
                    style={{
                      color: 'var(--color-text-primary)',
                      backgroundColor: 'var(--color-bg-field)',
                      borderColor: 'var(--color-border-subtle)',
                    }}
                  >
                    ({outputShape.join(', ')})
                  </div>
                </div>

                <div>
                  <div className="text-[11px] font-semibold" style={{ color: 'var(--color-text-secondary)' }}>
                    Window Shape
                  </div>
                  <div className="mt-2 flex flex-wrap items-end gap-2">
                    {outputShape.map((_, index) => (
                      <label key={index} className="min-w-[72px] flex-1">
                        <span className="mb-1 block text-[9px]" style={{ color: 'var(--color-text-muted)' }}>
                          Axis {index}
                        </span>
                        <input
                          type="number"
                          min="1"
                          step="1"
                          inputMode="numeric"
                          value={windowInputs[index] ?? ''}
                          aria-label={`Window shape axis ${index}`}
                          aria-invalid={!Number.isSafeInteger(parsedWindowShape[index]) || parsedWindowShape[index] <= 0}
                          onChange={event => updateWindowInput(index, event.target.value)}
                          className="h-8 w-full rounded-[var(--radius-md)] border bg-[var(--color-bg-field)] px-2 text-center font-mono text-[12px] outline-none focus:border-[var(--color-border-focus)] focus:ring-1 focus:ring-[var(--color-border-focus)]/30"
                          style={{
                            color: 'var(--color-text-primary)',
                            borderColor: 'var(--color-border-default)',
                          }}
                        />
                      </label>
                    ))}
                  </div>
                  {!windowShapeValid && (
                    <p className="mt-2 text-[10px]" style={{ color: 'var(--color-danger)' }}>
                      Enter one positive integer for every output dimension.
                    </p>
                  )}
                </div>

                <label className="block">
                  <span
                    className="text-[11px] font-semibold"
                    style={{ color: 'var(--color-text-secondary)' }}
                  >
                    Maximum in-flight Windows
                  </span>
                  <input
                    type="number"
                    min="1"
                    step="1"
                    inputMode="numeric"
                    value={maxInFlightInput}
                    placeholder="Automatic"
                    aria-label="Maximum in-flight Windows"
                    aria-invalid={!maxInFlightValid}
                    onChange={event => setMaxInFlightInput(event.target.value)}
                    className="mt-2 h-8 w-full rounded-[var(--radius-md)] border bg-[var(--color-bg-field)] px-2 font-mono text-[12px] outline-none focus:border-[var(--color-border-focus)] focus:ring-1 focus:ring-[var(--color-border-focus)]/30"
                    style={{
                      color: 'var(--color-text-primary)',
                      borderColor: 'var(--color-border-default)',
                    }}
                  />
                  <span className="mt-1 block text-[10px]" style={{ color: 'var(--color-text-muted)' }}>
                    Controls how many Window graphs the Driver submits and tracks at once. Enter 1 for
                    strictly one-at-a-time execution. Automatic uses 2 × GPU Workers for GPU workflows
                    and 1 for CPU-only workflows, subject to the server cap.
                  </span>
                  {!maxInFlightValid && (
                    <span className="mt-1 block text-[10px]" style={{ color: 'var(--color-danger)' }}>
                      Enter a positive integer.
                    </span>
                  )}
                </label>

                <fieldset>
                  <legend
                    className="mb-2 text-[11px] font-semibold"
                    style={{ color: 'var(--color-text-secondary)' }}
                  >
                    Recovery Action
                  </legend>
                  <div className="grid grid-cols-3 gap-2">
                    {(['new', 'resume', 'restart'] as const).map(action => (
                      <label
                        key={action}
                        className="flex cursor-pointer items-center justify-center gap-1.5 rounded-[var(--radius-md)] border px-2 py-2 text-[11px] capitalize"
                        style={{
                          borderColor: resumeAction === action
                            ? 'var(--color-accent)'
                            : 'var(--color-border-default)',
                          backgroundColor: resumeAction === action
                            ? 'var(--color-accent-soft)'
                            : 'var(--color-bg-field)',
                        }}
                      >
                        <input
                          type="radio"
                          name="resume-action"
                          value={action}
                          checked={resumeAction === action}
                          onChange={() => {
                            setResumeAction(action);
                            clearInspection();
                          }}
                          className="accent-[var(--color-accent)]"
                        />
                        {action}
                      </label>
                    ))}
                  </div>
                  <p className="mt-1.5 text-[10px]" style={{ color: 'var(--color-text-muted)' }}>
                    {resumeAction === 'new'
                      ? 'Create a new recovery record. Existing state is never overwritten silently.'
                      : resumeAction === 'resume'
                        ? 'Continue only incomplete Windows from a validated recovery directory.'
                        : 'Discard completion progress and rerun all Windows after validation.'}
                  </p>
                </fieldset>

                <fieldset>
                  <legend
                    className="mb-2 text-[11px] font-semibold"
                    style={{ color: 'var(--color-text-secondary)' }}
                  >
                    Recovery Location
                  </legend>
                  <div className="space-y-2">
                    <label className="flex items-start gap-2 text-[11px]">
                      <input
                        type="radio"
                        name="recovery-location"
                        checked={recoveryMode === 'output_sidecar'}
                        onChange={() => {
                          setRecoveryMode('output_sidecar');
                          clearInspection();
                        }}
                        className="mt-0.5 accent-[var(--color-accent)]"
                      />
                      <span>
                        <span className="block font-medium">Store next to an output</span>
                        <span className="text-[10px]" style={{ color: 'var(--color-text-muted)' }}>
                          Creates one canonical <span className="font-mono">.workflow</span> sidecar.
                        </span>
                      </span>
                    </label>

                    {recoveryMode === 'output_sidecar' && (
                      <select
                        value={anchorNodeId}
                        aria-label="Recovery sidecar output"
                        onChange={event => {
                          setAnchorNodeId(event.target.value);
                          clearInspection();
                        }}
                        className="h-9 w-full rounded-[var(--radius-md)] border bg-[var(--color-bg-field)] px-2 text-[11px] outline-none focus:border-[var(--color-border-focus)]"
                        style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-default)' }}
                      >
                        {outputs.map(output => (
                          <option key={output.nodeId} value={output.nodeId}>
                            {output.displayName} — {output.path}
                          </option>
                        ))}
                      </select>
                    )}

                    <label className="flex items-start gap-2 text-[11px]">
                      <input
                        type="radio"
                        name="recovery-location"
                        checked={recoveryMode === 'custom'}
                        onChange={() => {
                          setRecoveryMode('custom');
                          clearInspection();
                        }}
                        className="mt-0.5 accent-[var(--color-accent)]"
                      />
                      <span className="font-medium">Custom server directory</span>
                    </label>

                    {recoveryMode === 'custom' && (
                      <div className="flex gap-2">
                        <input
                          type="text"
                          value={customDirectory}
                          aria-label="Custom recovery directory"
                          placeholder="/shared/project/run.workflow"
                          onChange={event => {
                            setCustomDirectory(event.target.value);
                            clearInspection();
                          }}
                          className="h-9 min-w-0 flex-1 rounded-[var(--radius-md)] border bg-[var(--color-bg-field)] px-2 font-mono text-[11px] outline-none focus:border-[var(--color-border-focus)]"
                          style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-default)' }}
                        />
                        <Button type="button" size="md" onClick={openRecoveryBrowser}>
                          Browse
                        </Button>
                      </div>
                    )}
                  </div>

                  <div
                    className="mt-3 rounded-[var(--radius-md)] border px-3 py-2"
                    style={{
                      borderColor: 'var(--color-border-subtle)',
                      backgroundColor: 'var(--color-bg-field)',
                    }}
                  >
                    <div className="text-[9px] uppercase tracking-wide" style={{ color: 'var(--color-text-muted)' }}>
                      Resolved recovery directory
                    </div>
                    <div className="mt-1 break-all font-mono text-[10px]" style={{ color: 'var(--color-text-primary)' }}>
                      {resolvedRecoveryDirectory ?? 'Select a valid absolute server directory.'}
                    </div>
                  </div>

                  {resumeAction !== 'new' && (
                    <div className="mt-2">
                      <Button
                        type="button"
                        size="sm"
                        loading={isInspecting}
                        disabled={!resolvedRecoveryDirectory}
                        onClick={() => { void handleInspect(); }}
                      >
                        Inspect Recovery
                      </Button>
                      {inspection && inspectedCurrentDirectory && (
                        <p className="mt-2 text-[10px]" style={{ color: 'var(--color-success)' }}>
                          {inspection.completedWindows.toLocaleString()} / {inspection.totalWindows.toLocaleString()} Windows complete · {inspection.status}
                        </p>
                      )}
                      {inspectionError && (
                        <p className="mt-2 text-[10px]" style={{ color: 'var(--color-danger)' }}>
                          {inspectionError}
                        </p>
                      )}
                    </div>
                  )}
                </fieldset>

                <div
                  className="flex items-center justify-between rounded-[var(--radius-md)] border px-3 py-2"
                  style={{
                    backgroundColor: 'var(--color-bg-field)',
                    borderColor: 'var(--color-border-subtle)',
                  }}
                >
                  <span className="text-[11px] font-medium" style={{ color: 'var(--color-text-secondary)' }}>
                    Estimated Windows
                  </span>
                  <span className="text-right font-mono text-[11px]" style={{ color: 'var(--color-info)' }} aria-live="polite">
                    <span className="block">
                      {estimatedWindows === null ? '--' : estimatedWindows.toLocaleString()}
                    </span>
                    {windowGridShape && (
                      <span className="block text-[9px]" style={{ color: 'var(--color-text-muted)' }}>
                        grid ({windowGridShape.join(', ')})
                      </span>
                    )}
                  </span>
                </div>
              </div>
            )}
          </div>

          <div
            className="flex shrink-0 items-center justify-end gap-2 border-t px-5 py-3"
            style={{ borderColor: 'var(--color-border-subtle)', backgroundColor: 'var(--color-bg-surface-2)' }}
          >
            <Button type="button" variant="secondary" size="md" onClick={cancelExecutionDialog}>
              Cancel
            </Button>
            <Button type="submit" variant="primary" size="md" disabled={!canSubmit}>
              {mode === 'window' && resumeAction === 'resume'
                ? 'Resume'
                : mode === 'window' && resumeAction === 'restart'
                  ? 'Restart'
                  : 'Execute'}
            </Button>
          </div>
        </form>
      </div>
    </div>
  );
}
