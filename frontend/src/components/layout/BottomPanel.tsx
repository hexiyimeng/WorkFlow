import { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import { useFlow } from '../../hooks/useFlowContext';
import { Pill } from '../ui/Pill';
import { IconButton } from '../ui/IconButton';
import { Button } from '../ui/Button';

function shortId(id: string | null): string {
  return id ? id.slice(0, 8) : '—';
}
function formatTime(ts: string | number): string {
  if (!ts) return '--:--:--';
  const d = new Date(ts);
  if (isNaN(d.getTime())) return '--:--:--';
  return d.toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

const PHASE_LABELS: Record<string, string> = {
  idle: 'Idle', graph_building: 'Building', submitted: 'Queued',
  running: 'Running', cancelling: 'Stopping', succeeded: 'Done',
  disconnected: 'Disconnected', interrupted: 'Interrupted',
  failed: 'Failed', cancelled: 'Cancelled',
};
const PHASE_PILL: Record<string, 'idle' | 'info' | 'running' | 'success' | 'danger' | 'warning' | 'muted'> = {
  idle: 'idle', graph_building: 'info', submitted: 'info',
  running: 'running', cancelling: 'warning', succeeded: 'success',
  disconnected: 'warning', interrupted: 'warning',
  failed: 'danger', cancelled: 'warning',
};

type Tab = 'logs' | 'errors';

const LOG_ICONS: Record<string, string> = { error: '✕', success: '✓', warning: '⚠', info: '›' };
const LOG_COLORS: Record<string, string> = {
  error: 'var(--color-danger)',
  success: 'var(--color-success)',
  warning: 'var(--color-warning)',
  info: 'var(--color-console-info)',
};

export default function BottomPanel() {
  const { logs, clearLogs, isConsoleOpen, toggleConsole, executionState, websocketStatus, isExecuting, isCancelling, stopFlow } = useFlow();
  const [tab, setTab] = useState<Tab>('logs');
  const endRef = useRef<HTMLDivElement>(null);
  const [height, setHeight] = useState(220);
  const isDragging = useRef(false);

  useEffect(() => {
    if (isConsoleOpen) endRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs, isConsoleOpen]);

  useEffect(() => {
    if (
      websocketStatus === 'reconnecting'
      && executionState.phase === 'disconnected'
      && !isConsoleOpen
    ) toggleConsole();
  }, [websocketStatus, executionState.phase, isConsoleOpen, toggleConsole]);

  const startResizing = useCallback((e: React.MouseEvent) => {
    e.preventDefault(); isDragging.current = true;
    document.body.style.cursor = 'row-resize'; document.body.style.userSelect = 'none';
  }, []);
  const stopResizing = useCallback(() => {
    isDragging.current = false; document.body.style.cursor = ''; document.body.style.userSelect = '';
  }, []);
  const resize = useCallback((e: MouseEvent) => {
    if (isDragging.current) setHeight(Math.max(100, Math.min(window.innerHeight - e.clientY, window.innerHeight * 0.7)));
  }, []);
  useEffect(() => {
    window.addEventListener('mousemove', resize);
    window.addEventListener('mouseup', stopResizing);
    return () => { window.removeEventListener('mousemove', resize); window.removeEventListener('mouseup', stopResizing); };
  }, [resize, stopResizing]);

  const phase = executionState.phase;
  const execId = executionState.executionId;
  const errorLogs = useMemo(() => logs.filter(l => l.type === 'error'), [logs]);
  const windowProgress = executionState.windowProgress;
  const showWindowProgress = windowProgress !== null && (isExecuting || isCancelling);
  const activeWindowWidth = windowProgress && windowProgress.totalWindows > 0
    ? Math.max(100 / windowProgress.totalWindows, 0.75)
    : 0;
  const hasActiveWindow = Boolean(
    windowProgress
    && windowProgress.windowStatus === 'running'
    && windowProgress.totalWindows > 0
  );
  const activeWindowOffset = windowProgress && windowProgress.totalWindows > 0
    ? (windowProgress.currentWindow - 1) * 100 / windowProgress.totalWindows
    : 0;

  const mainMessage = useMemo(() => {
    if (phase === 'idle') return 'Ready';
    if (phase === 'graph_building') return 'Building graph...';
    if (phase === 'submitted') return 'Execution queued';
    if (phase === 'running' && windowProgress) return windowProgress.message;
    if (phase === 'running') return 'Workflow running';
    if (phase === 'cancelling') return 'Stopping...';
    if (phase === 'disconnected') return 'Backend connection lost; checking execution status...';
    if (phase === 'interrupted') return 'Backend execution interrupted; Window runs can be resumed from Recovery';
    if (phase === 'succeeded') return 'Completed successfully';
    if (phase === 'failed') return executionState.lastError ? `Failed: ${executionState.lastError.slice(0, 80)}` : 'Execution failed';
    if (phase === 'cancelled') return 'Execution cancelled';
    return phase;
  }, [phase, executionState.lastError, windowProgress]);

  const wsVariant = websocketStatus === 'connected' ? 'success' : websocketStatus === 'reconnecting' ? 'warning' : 'danger';
  const wsLabel = websocketStatus === 'connected' ? 'Connected' : websocketStatus === 'reconnecting' ? 'Reconnecting' : 'Offline';

  return (
    <div
      className="absolute bottom-0 left-0 w-full z-40 flex flex-col"
      style={{
        height: isConsoleOpen ? height : 42,
        backgroundColor: 'var(--color-console-bg)',
        borderTop: '1px solid var(--color-console-border)',
        transition: 'height 200ms ease',
      }}
    >
      {/* Resize handle */}
      {isConsoleOpen && (
        <div
          onMouseDown={startResizing}
          className="absolute top-0 left-0 right-0 h-1.5 cursor-row-resize z-50 flex items-center justify-center"
          style={{ top: 0 }}
          onMouseEnter={e => (e.currentTarget.style.backgroundColor = 'var(--color-accent-soft)')}
          onMouseLeave={e => (e.currentTarget.style.backgroundColor = 'transparent')}
        >
          <div className="w-10 h-0.5 rounded-full" style={{ backgroundColor: 'var(--color-border-default)' }} />
        </div>
      )}

      {/* ---- Collapsed / Summary bar ---- */}
      <div
        className="h-[42px] shrink-0 flex items-center px-3 gap-3 border-b cursor-pointer select-none"
        style={{ borderColor: 'var(--color-console-border)' }}
        onClick={toggleConsole}
      >
        {/* Collapse chevron */}
        <div
          className="w-4 h-4 flex items-center justify-center transition-transform duration-150"
          style={{ transform: isConsoleOpen ? 'rotate(0deg)' : 'rotate(-90deg)', color: 'var(--color-text-muted)' }}
        >
          <svg className="w-3 h-3" viewBox="0 0 24 24" fill="currentColor">
            <path d="M7 10l5 5 5-5z" />
          </svg>
        </div>

        {/* WS status */}
        <Pill variant={wsVariant} dot pulse={websocketStatus === 'reconnecting'} className="text-[9px]">
          {wsLabel}
        </Pill>

        {/* Phase */}
        <Pill variant={PHASE_PILL[phase] ?? 'muted'} dot pulse={phase === 'running' || phase === 'cancelling'} className="text-[9px]">
          {PHASE_LABELS[phase] ?? phase}
        </Pill>

        {/* Execution ID */}
        {execId && (
          <span className="text-[10px] font-mono px-1.5 py-0.5 rounded shrink-0" style={{ backgroundColor: 'var(--color-bg-field)', color: 'var(--color-info)', border: '1px solid var(--color-info)/20' }}>
            {shortId(execId)}
          </span>
        )}

        {/* Message / Window progress */}
        {showWindowProgress && windowProgress ? (
          <div className="flex-1 min-w-[240px] max-w-[560px] flex flex-col gap-1">
            <div className="flex items-center justify-between gap-3 text-[10px] font-mono">
              <span className="truncate" style={{ color: 'var(--color-console-text)' }}>
                {windowProgress.windowStatus === 'finalizing'
                  ? `Finalizing · ${windowProgress.completedWindows} / ${windowProgress.totalWindows} Windows`
                  : windowProgress.message}
              </span>
              <span className="shrink-0 tabular-nums" style={{ color: 'var(--color-accent)' }}>
                {windowProgress.progress.toFixed(1)}%
              </span>
            </div>
            <div
              role="progressbar"
              aria-label="Window execution progress"
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={windowProgress.progress}
              aria-valuetext={`${windowProgress.completedWindows} of ${windowProgress.totalWindows} Windows completed`}
              className="relative h-1.5 rounded-full overflow-hidden"
              style={{ backgroundColor: 'var(--color-bg-field)' }}
            >
              <div
                className="absolute inset-y-0 left-0 rounded-full transition-[width] duration-300"
                style={{
                  width: `${windowProgress.progress}%`,
                  backgroundColor: 'var(--color-accent)',
                }}
              />
              {hasActiveWindow && (
                <div
                  className="absolute inset-y-0 rounded-full animate-pulse"
                  style={{
                    left: `${activeWindowOffset}%`,
                    width: `${activeWindowWidth}%`,
                    backgroundColor: 'var(--color-info)',
                  }}
                />
              )}
            </div>
          </div>
        ) : (
          <span className="flex-1 truncate text-[11px] font-mono" style={{ color: 'var(--color-console-text)' }}>
            {mainMessage}
          </span>
        )}

        {/* Actions */}
        <div className="flex items-center gap-1 shrink-0" onClick={e => e.stopPropagation()}>
          {(isExecuting || isCancelling) && (
            <Button
              variant="danger"
              size="xs"
              onClick={stopFlow}
              disabled={websocketStatus !== 'connected'}
            >
              ■ Stop
            </Button>
          )}
          <IconButton size="xs" onClick={() => clearLogs()} title="Clear logs">
            <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
            </svg>
          </IconButton>
        </div>
      </div>

      {/* ---- Expanded body ---- */}
      {isConsoleOpen && (
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Tab bar */}
          <div className="flex items-center gap-0.5 px-3 pt-1.5 pb-1 border-b shrink-0" style={{ borderColor: 'var(--color-console-border)' }}>
            {(['logs', 'errors'] as Tab[]).map(t => (
              <button
                key={t}
                onClick={() => setTab(t)}
                className="px-3 py-1 rounded-[var(--radius-sm)] text-[10px] font-semibold uppercase tracking-wider transition-all"
                style={{
                  backgroundColor: tab === t ? 'var(--color-accent-soft)' : 'transparent',
                  color: tab === t ? 'var(--color-accent)' : 'var(--color-text-muted)',
                }}
              >
                {t}
                {t === 'errors' && errorLogs.length > 0 && (
                  <span className="ml-1 px-1 py-px rounded-full text-[9px]" style={{ backgroundColor: 'var(--color-danger-soft)', color: 'var(--color-danger)' }}>
                    {errorLogs.length}
                  </span>
                )}
              </button>
            ))}
          </div>

          {/* Log list */}
          <div className="flex-1 overflow-y-auto custom-scrollbar font-mono">
            {tab === 'logs' && (
              logs.length === 0 ? (
                <div className="px-4 py-4 text-[11px] italic" style={{ color: 'var(--color-text-muted)' }}>
                  {'>'} System ready. No logs yet.
                </div>
              ) : (
                logs.map(log => (
                  <div
                    key={log.id}
                    className="flex items-start gap-3 px-3 py-0.5 hover:bg-[var(--color-bg-field-hover)] border-l-2 transition-colors"
                    style={{ borderColor: log.type === 'error' ? 'var(--color-danger)' : log.type === 'warning' ? 'var(--color-warning)' : 'transparent' }}
                  >
                    <span className="text-[10px] shrink-0 w-16 text-right pt-px" style={{ color: 'var(--color-text-muted)' }}>
                      {formatTime(log.timestamp)}
                    </span>
                    <span style={{ color: LOG_COLORS[log.type] ?? 'var(--color-console-info)' }} className="text-[12px] shrink-0 w-3 flex items-center justify-center">
                      {LOG_ICONS[log.type] ?? '›'}
                    </span>
                    <span className="text-[11px] flex-1 break-all" style={{ color: 'var(--color-console-text)' }}>
                      {log.message}
                    </span>
                  </div>
                ))
              )
            )}
            {tab === 'errors' && (
              errorLogs.length === 0 ? (
                <div className="px-4 py-4 text-[11px] italic" style={{ color: 'var(--color-text-muted)' }}>
                  No errors. All good.
                </div>
              ) : (
                errorLogs.map(log => (
                  <div
                    key={log.id}
                    className="flex items-start gap-3 px-3 py-0.5 hover:bg-[var(--color-bg-field-hover)] border-l-2 border-[var(--color-danger)] transition-colors"
                  >
                    <span className="text-[10px] shrink-0 w-16 text-right pt-px" style={{ color: 'var(--color-text-muted)' }}>
                      {formatTime(log.timestamp)}
                    </span>
                    <span className="text-[12px] shrink-0 w-3 flex items-center justify-center" style={{ color: 'var(--color-danger)' }}>✕</span>
                    <span className="text-[11px] flex-1 break-all" style={{ color: 'var(--color-danger)' }}>
                      {log.message}
                    </span>
                  </div>
                ))
              )
            )}
            <div ref={endRef} />
          </div>
        </div>
      )}
    </div>
  );
}
