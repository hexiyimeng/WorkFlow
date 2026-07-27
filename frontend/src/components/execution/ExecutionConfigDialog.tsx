import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
} from 'react';
import { useFlow } from '../../hooks/useFlowContext';
import type { ExecutionMode, ExecutionPreflightResponse } from '../../types';
import { estimateWindowCount, isValidWindowShape } from '../../utils/executionConfig';
import { Button } from '../ui/Button';

const FOCUSABLE_SELECTOR = [
  'button:not([disabled])',
  'input:not([disabled])',
  '[href]',
  '[tabindex]:not([tabindex="-1"])',
].join(',');

const EMPTY_OUTPUT_SHAPE: number[] = [];

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
  } = useFlow();
  const outputShape = executionPreflight.output_shape ?? EMPTY_OUTPUT_SHAPE;
  const [mode, setMode] = useState<ExecutionMode>('full_graph');
  const [windowInputs, setWindowInputs] = useState<string[]>(() => outputShape.map(() => '1'));
  const dialogRef = useRef<HTMLDivElement>(null);
  const openerRef = useRef<HTMLElement | null>(null);

  const windowAvailable = Boolean(executionPreflight.windowable && outputShape.length > 0);

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

  const estimatedWindows = useMemo(
    () => estimateWindowCount(outputShape, parsedWindowShape),
    [outputShape, parsedWindowShape],
  );

  const updateWindowInput = (index: number, value: string) => {
    setWindowInputs(current => current.map((item, itemIndex) => (
      itemIndex === index ? value : item
    )));
  };

  const handleSubmit = () => {
    if (mode === 'window') {
      if (!windowAvailable || !windowShapeValid) return;
      confirmExecution({ mode: 'window', windowShape: parsedWindowShape });
      return;
    }
    confirmExecution({ mode: 'full_graph' });
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
  const canSubmit = mode === 'full_graph' || (windowAvailable && windowShapeValid);

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
                  <span className="font-mono text-[12px]" style={{ color: 'var(--color-info)' }} aria-live="polite">
                    {estimatedWindows === null ? '--' : estimatedWindows.toLocaleString()}
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
              Execute
            </Button>
          </div>
        </form>
      </div>
    </div>
  );
}
