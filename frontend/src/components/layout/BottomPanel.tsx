import { useEffect, useMemo, useRef, useState } from 'react';
import { ArrowDownToLine, Check, ChevronDown, ChevronUp, CircleCheck, Copy, Search, Terminal, Trash2, X, AlertCircle } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';
import { usePanelResize } from '../../hooks/usePanelResize';
import type { LogEntry } from '../../types';

function formatTime(timestamp: string | number): string {
  const date = new Date(timestamp);
  return Number.isNaN(date.getTime()) ? '--:--:--' : date.toLocaleTimeString([], { hour12: false });
}
function LogMessage({ log }: { log: LogEntry }) {
  return <div className="output-log__message">{log.message}
    {log.details && <details className="output-log__details"><summary>Error details</summary><pre>{log.details}</pre></details>}
  </div>;
}
const PHASE_LABELS: Record<string, string> = { idle: 'Ready', graph_building: 'Building graph', submitted: 'Queued', running: 'Running',
  cancelling: 'Stopping', succeeded: 'Completed', disconnected: 'Disconnected', interrupted: 'Interrupted', failed: 'Failed', cancelled: 'Cancelled' };
type Filter = 'all' | 'errors' | 'warnings';

export default function BottomPanel() {
  const { logs, clearLogs, isConsoleOpen, toggleConsole, executionState, websocketStatus, isExecuting, isCancelling, stopFlow } = useFlow();
  const [filter, setFilter] = useState<Filter>('all');
  const [query, setQuery] = useState('');
  const [follow, setFollow] = useState(true);
  const [copyState, setCopyState] = useState<'idle' | 'copied' | 'failed'>('idle');
  const bodyRef = useRef<HTMLDivElement>(null);
  const lastShownErrorRef = useRef<string | null>(null);
  const { size, separatorProps } = usePanelResize('y', 240, 140, typeof window === 'undefined' ? 600 : Math.max(140, Math.floor(window.innerHeight * .65)), -1);
  const errors = useMemo(() => logs.filter(log => log.type === 'error'), [logs]);
  const warnings = useMemo(() => logs.filter(log => log.type === 'warning'), [logs]);
  const visible = useMemo(() => {
    const source = filter === 'errors' ? errors : filter === 'warnings' ? warnings : logs;
    const search = query.trim().toLowerCase();
    return search ? source.filter(log => `${log.message} ${log.details || ''}`.toLowerCase().includes(search)) : source;
  }, [errors, warnings, logs, filter, query]);
  const latestErrorId = errors.at(-1)?.id;
  useEffect(() => {
    if (latestErrorId && latestErrorId !== lastShownErrorRef.current) {
      lastShownErrorRef.current = latestErrorId;
      if (!isConsoleOpen) toggleConsole();
    }
  }, [latestErrorId, isConsoleOpen, toggleConsole]);
  useEffect(() => {
    if (websocketStatus === 'reconnecting' && executionState.phase === 'disconnected' && !isConsoleOpen) toggleConsole();
  }, [websocketStatus, executionState.phase, isConsoleOpen, toggleConsole]);
  useEffect(() => {
    if (follow && isConsoleOpen && bodyRef.current) bodyRef.current.scrollTop = bodyRef.current.scrollHeight;
  }, [visible, follow, isConsoleOpen]);
  const windowProgress = executionState.windowProgress ?? null;
  const phase = executionState.phase;
  const message = phase === 'failed' ? executionState.lastError || 'Execution failed'
    : phase === 'disconnected' ? 'Connection lost; checking execution status…'
    : phase === 'running' ? 'Processing workflow' : '';
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(visible.map(log => `[${formatTime(log.timestamp)}] [${log.type.toUpperCase()}] ${log.message}${log.details ? `\n${log.details}` : ''}`).join('\n'));
      setCopyState('copied');
    } catch { setCopyState('failed'); }
  };

  return <section className="output-panel" aria-label="Execution output" style={{ height: isConsoleOpen ? size : 38 }}>
    {isConsoleOpen && <div {...separatorProps} aria-label="Resize output panel" className="output-resize-handle" />}
    <div className="output-summary">
      <button className="output-heading" onClick={toggleConsole} aria-expanded={isConsoleOpen} aria-label={isConsoleOpen ? 'Collapse output' : 'Expand output'}>
        <Terminal size={14} /><span>Output</span><small>{logs.length}</small>{isConsoleOpen ? <ChevronDown size={13} /> : <ChevronUp size={13} />}
      </button>
      <span className="output-connection" data-state={websocketStatus} title={websocketStatus === 'connected' ? 'Backend connected' : 'Backend disconnected'}><i />{websocketStatus === 'connected' ? 'Connected' : websocketStatus === 'reconnecting' ? 'Reconnecting' : 'Offline'}</span>
      <span className="output-phase" data-phase={phase}>{PHASE_LABELS[phase] || phase}</span>
      {windowProgress && (isExecuting || isCancelling) ? <div className="output-window-progress">
        <span>{windowProgress.message}</span><strong>{windowProgress.completedWindows} / {windowProgress.totalWindows}<small>{windowProgress.progress.toFixed(1)}%</small></strong>
        <div className="output-progress-track" role="progressbar" aria-label="Window execution progress" aria-valuemin={0} aria-valuemax={100}
          aria-valuenow={windowProgress.progress} aria-valuetext={`${windowProgress.completedWindows} of ${windowProgress.totalWindows} Windows completed`}>
          <div style={{ width: `${windowProgress.progress}%` }} />
          {windowProgress.windowStatus === 'running' && windowProgress.totalWindows > 0 && <i style={{ left: `${(windowProgress.currentWindow - 1) * 100 / windowProgress.totalWindows}%`, width: `${Math.max(100 / windowProgress.totalWindows, .75)}%` }} />}
        </div>
      </div> : <span className="output-summary-message" title={message}>{message}</span>}
      {executionState.executionId && <code className="output-execution-id" title={`Execution: ${executionState.executionId}`}>{executionState.executionId.slice(0, 8)}</code>}
      {(isExecuting || isCancelling) && <button className="output-stop" onClick={stopFlow} disabled={websocketStatus !== 'connected' || isCancelling}>{isCancelling ? 'Stopping…' : 'Stop'}</button>}
      {!isConsoleOpen && errors.length > 0 && <button className="output-error-count" onClick={() => { setFilter('errors'); toggleConsole(); }}>{errors.length} errors</button>}
      {isConsoleOpen && <button className="workspace-icon-button" title="Collapse output" aria-label="Close output" onClick={toggleConsole}><X size={14} /></button>}
    </div>
    {isConsoleOpen && <>
      <div className="output-toolbar">
        <div className="output-tabs" role="tablist" aria-label="Output filters">
          {(['all', 'errors', 'warnings'] as Filter[]).map(tab => <button role="tab" key={tab} aria-selected={filter === tab} onClick={() => { setFilter(tab); setCopyState('idle'); }}>
            {tab === 'all' ? 'All events' : tab === 'errors' ? 'Errors' : 'Warnings'}<small>{tab === 'all' ? logs.length : tab === 'errors' ? errors.length : warnings.length}</small>
          </button>)}
        </div>
        <div className="output-search"><Search size={12} /><input aria-label="Search output" placeholder="Filter output…" value={query} onChange={event => { setQuery(event.target.value); setCopyState('idle'); }} />
          {query && <button aria-label="Clear output search" onClick={() => setQuery('')}><X size={12} /></button>}
        </div>
        <button className="workspace-icon-button" aria-label="Follow output" aria-pressed={follow} title={follow ? 'Auto-scroll enabled' : 'Resume auto-scroll'} onClick={() => setFollow(current => !current)}><ArrowDownToLine size={14} /></button>
        <button className="workspace-icon-button" aria-label="Copy visible output" title={copyState === 'copied' ? 'Copied' : 'Copy visible output'} disabled={!visible.length} onClick={() => { void copy(); }}>{copyState === 'copied' ? <Check size={14} /> : <Copy size={14} />}</button>
        <button className="workspace-icon-button" aria-label="Clear logs" title="Clear logs" disabled={!logs.length} onClick={() => { clearLogs(); setCopyState('idle'); }}><Trash2 size={14} /></button>
        {copyState === 'failed' && <span role="status" className="output-copy-status">Copy unavailable; select the text to copy.</span>}
        {copyState === 'copied' && <span role="status" className="sr-only">Output copied</span>}
      </div>
      <div ref={bodyRef} className="output-body custom-scrollbar" onScroll={event => {
        const container = event.currentTarget;
        setFollow(container.scrollHeight - container.scrollTop - container.clientHeight < 24);
      }}>
        {visible.length === 0 ? <div className="output-empty">{filter === 'errors' ? <CircleCheck size={22} /> : <Terminal size={22} />}<strong>{query ? 'No matching events' : filter === 'errors' ? 'No errors' : filter === 'warnings' ? 'No warnings' : 'Ready for execution'}</strong><span>{query ? 'Try another search term or filter.' : 'Execution messages and diagnostics will appear here.'}</span></div> :
          visible.map(log => <div key={log.id} className="output-log" data-level={log.type}>
            <time>{formatTime(log.timestamp)}</time><span className="output-log__level">{log.type === 'error' ? <AlertCircle size={12} /> : <span className="output-log__dot" />}{log.type}</span><LogMessage log={log} />
          </div>)}
      </div>
    </>}
  </section>;
}
