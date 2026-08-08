// src/components/layout/Header.tsx
import { useRef, type ChangeEvent } from 'react';
import { useReactFlow, getNodesBounds, getViewportForBounds } from '@xyflow/react';
import { FolderClock, RefreshCw, SlidersHorizontal } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';
import { Button } from '../ui/Button';
import { IconButton } from '../ui/IconButton';
import { Pill } from '../ui/Pill';
import {
  serializeWorkflowDocument,
  parseWorkflowDocument,
  hydrateFlowWithLatestSpecs,
} from '../../utils/workflowPersistence';
import { formatWorkflowExecutionSettingsSummary } from '../../utils/workflowExecutionSettings';

const SunIcon = () => (
  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
  </svg>
);
const MoonIcon = () => (
  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
  </svg>
);
const LoadIcon = () => (
  <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
  </svg>
);
const SaveIcon = () => (
  <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3 3m0 0l-3-3m3 3V4" />
  </svg>
);
const PlusIcon = () => (
  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M12 4v16m8-8H4" />
  </svg>
);
const XIcon = () => (
  <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
  </svg>
);
const DaskIcon = () => (
  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
  </svg>
);
const RunIcon = () => (
  <svg className="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 20 20">
    <path d="M6.3 2.841A1.5 1.5 0 004 4.11V15.89a1.5 1.5 0 002.3 1.269l9.344-5.89a1.5 1.5 0 000-2.538L6.3 2.84z" />
  </svg>
);
const StopIcon = () => (
  <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
    <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8 7a1 1 0 00-1 1v4a1 1 0 001 1h4a1 1 0 001-1V8a1 1 0 00-1-1H8z" clipRule="evenodd" />
  </svg>
);

const PHASE_LABELS: Record<string, string> = {
  idle: 'Ready',
  graph_building: 'Building',
  submitted: 'Queued',
  running: 'Running',
  cancelling: 'Stopping',
  disconnected: 'Disconnected',
  interrupted: 'Interrupted',
  succeeded: 'Done',
  failed: 'Failed',
  cancelled: 'Cancelled',
};

const PHASE_PILL: Record<string, 'idle' | 'info' | 'running' | 'success' | 'danger' | 'warning' | 'muted'> = {
  idle: 'idle',
  graph_building: 'info',
  submitted: 'info',
  running: 'running',
  cancelling: 'warning',
  disconnected: 'warning',
  interrupted: 'warning',
  succeeded: 'success',
  failed: 'danger',
  cancelled: 'warning',
};

export default function Header() {
  const {
    theme, toggleTheme,
    workflows, activeWorkflow, activeWorkflowId,
    createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow,
    loadWorkflowDocument,
    activeExecutionSettings, activeExecutionSettingsConfigured,
    openExecutionSettings,
    runFlow, stopFlow, reloadNodes,
    nodeDefs,
    dashboardUrl,
    isReloadingNodes,
    executionState,
    isConnected, isExecuting, isPreflighting, isExecutionLocked, addLog,
    recoveredGraphView, openRecoveryBrowser,
    closeRecoveredGraph,
  } = useFlow();

  const reactFlowInstance = useReactFlow();
  const fileInputRef = useRef<HTMLInputElement>(null);

  // === Save (export stripped/serialized workflow JSON) ===
  const handleSave = () => {
    if (!reactFlowInstance) return;
    const currentFlow = workflows.find(w => w.id === activeWorkflowId);
    const defaultName = currentFlow ? currentFlow.name : `workflow_${Date.now()}`;
    const fileName = prompt('Save workflow as:', defaultName);
    if (fileName === null) return;
    const finalName = fileName.trim() || defaultName;

    const serialized = serializeWorkflowDocument({
      workflowId: activeWorkflow?.workflowId ?? activeWorkflow?.id ?? activeWorkflowId,
      metadata: activeWorkflow?.metadata ?? {},
      nodes: reactFlowInstance.getNodes() as import('@xyflow/react').Node<import('../../types').NodeData>[],
      edges: reactFlowInstance.getEdges(),
      workflowName: finalName,
      timestamp: Date.now(),
    });

    const blob = new Blob(
      [JSON.stringify(serialized, null, 2)],
      { type: 'application/json' }
    );
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `${finalName}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(link.href);
  };

  // === Load (import + hydrate using shared utility) ===
  const handleLoad = (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    if (isExecutionLocked) {
      alert('Cannot load workflow while execution is running.');
      return;
    }

    const reader = new FileReader();
    reader.onload = (ev) => {
      let parsed: Record<string, unknown>;
      try {
        parsed = JSON.parse(ev.target?.result as string);
      } catch (err) {
        alert(`Load failed: ${(err as Error).message}`);
        return;
      }

      const document = parseWorkflowDocument(parsed);
      if (!document || !Array.isArray(document.nodes) || !Array.isArray(document.edges)) {
        alert('Invalid file format');
        return;
      }

      if (Object.keys(nodeDefs).length === 0) {
        alert('Node definitions not loaded yet. Please wait and try again.');
        return;
      }

      const result = hydrateFlowWithLatestSpecs(document, nodeDefs);
      loadWorkflowDocument(document, result.nodes, result.edges);

      if (result.invalidNodeTypes.length > 0) {
        alert(`Loaded ${result.nodes.length} nodes (${result.invalidNodeTypes.length} unavailable node type(s) marked invalid)`);
      } else if (result.removedEdges > 0) {
        alert(`Loaded ${result.nodes.length} nodes, removed ${result.removedEdges} invalid connection(s)`);
      }

      const bounds = getNodesBounds(result.nodes);
      if (bounds && bounds.width > 0) {
        const vp = getViewportForBounds(bounds, window.innerWidth, window.innerHeight, 0.1, 2, 0.1);
        reactFlowInstance.setViewport(vp);
      }
    };
    reader.readAsText(file);
    e.target.value = '';
  };

  const phase = executionState.phase;
  const phaseLabel = PHASE_LABELS[phase] ?? phase;
  const executionSettingsSummary = formatWorkflowExecutionSettingsSummary(
    activeExecutionSettings,
  );

  return (
    <header
      className="h-12 flex items-center px-3 gap-3 border-b select-none"
      style={{
        backgroundColor: 'var(--color-bg-surface)',
        borderColor: 'var(--color-border-subtle)',
        boxShadow: 'var(--shadow-panel)',
        zIndex: 30,
      }}
    >
      <input type="file" ref={fileInputRef} onChange={handleLoad} accept=".json" className="hidden" />

      {/* ---- Brand ---- */}
      <div className="flex items-center gap-2.5 w-44 shrink-0">
        <div
          className="w-8 h-8 rounded-[var(--radius-md)] flex items-center justify-center text-white font-bold text-[11px] shrink-0"
          style={{ backgroundColor: 'var(--color-accent)' }}
        >
          WF
        </div>
        <div className="flex flex-col">
          <span className="text-[13px] font-bold leading-tight" style={{ color: 'var(--color-text-primary)' }}>
            Brain<span style={{ color: 'var(--color-accent)' }}>Flow</span>
          </span>
          <span className="text-[9px] leading-tight" style={{ color: 'var(--color-text-muted)' }}>Workflow Studio</span>
        </div>
      </div>

      {/* ---- Tabs ---- */}
      <div className="flex-1 flex items-end h-full gap-0.5 overflow-x-auto overflow-y-hidden [&::-webkit-scrollbar]:hidden [-ms-overflow-style:'none'] [scrollbar-width:'none']">
        {workflows.map(wf => {
          const isActive = wf.id === activeWorkflowId;
          return (
            <div
              key={wf.id}
              onClick={() => { if (isExecutionLocked) { addLog('Cannot switch workflow while executing', 'warning'); return; } switchWorkflow(wf.id); }}
              onDoubleClick={() => { const n = prompt('Rename:', wf.name); if (n) renameWorkflow(wf.id, n.trim()); }}
              className={[
                'group relative flex items-center gap-1.5 px-3 rounded-t-md transition-all cursor-pointer min-w-[90px] max-w-[160px] border',
                isExecutionLocked ? 'cursor-not-allowed opacity-60' : '',
              ].join(' ')}
              style={{
                height: isActive ? 'calc(100% + 1px)' : '28px',
                marginTop: isActive ? '0' : '4px',
                backgroundColor: isActive ? 'var(--color-bg-canvas)' : 'var(--color-bg-field)',
                borderColor: isActive ? 'var(--color-border-subtle)' : 'transparent',
                borderBottomColor: isActive ? 'var(--color-bg-canvas)' : 'transparent',
                color: isActive ? 'var(--color-text-primary)' : 'var(--color-text-secondary)',
                fontWeight: isActive ? 500 : 400,
                zIndex: isActive ? 2 : 1,
              }}
            >
              <span className="truncate text-[11px] flex-1">{wf.name}</span>
              {workflows.length > 1 && (
                <button
                  onClick={(e) => { e.stopPropagation(); if (confirm(`Delete "${wf.name}"?`)) deleteWorkflow(wf.id); }}
                  className="w-4 h-4 rounded flex items-center justify-center opacity-0 group-hover:opacity-100 hover:bg-red-500/20 hover:text-red-400 transition-all"
                  style={{ color: 'var(--color-text-muted)' }}
                >
                  <XIcon />
                </button>
              )}
              {isActive && (
                <div className="absolute -bottom-px left-0 right-0 h-px" style={{ backgroundColor: 'var(--color-bg-canvas)' }} />
              )}
            </div>
          );
        })}
        <button
          onClick={createWorkflow}
          disabled={isExecutionLocked}
          className="h-7 w-7 flex items-center justify-center rounded transition-colors shrink-0 mt-0.5"
          style={{ color: 'var(--color-text-muted)', opacity: isExecutionLocked ? 0.4 : 1 }}
          onMouseEnter={e => !isExecutionLocked && (e.currentTarget.style.backgroundColor = 'var(--color-bg-field-hover)')}
          onMouseLeave={e => (e.currentTarget.style.backgroundColor = 'transparent')}
          title="New Workflow"
        >
          <PlusIcon />
        </button>
      </div>

      {/* ---- Right Controls ---- */}
      <div className="flex items-center gap-1 shrink-0">

        {/* Execution phase pill */}
        <Pill
          variant={PHASE_PILL[phase] ?? 'muted'}
          dot
          pulse={phase === 'running' || phase === 'cancelling'}
        >
          {phaseLabel}
        </Pill>

        {recoveredGraphView && (
          <Pill variant="warning" dot>
            Recovery · Read only
          </Pill>
        )}

        {/* Separator */}
        <div className="w-px h-5 mx-0.5" style={{ backgroundColor: 'var(--color-border-subtle)' }} />

        {/* Dashboard */}
        <IconButton
          onClick={() => { if (!dashboardUrl) { alert('Dashboard unavailable'); return; } window.open(dashboardUrl, '_blank'); }}
          title="Dask Dashboard"
        >
          <DaskIcon />
        </IconButton>

        <IconButton
          onClick={() => { void reloadNodes(); }}
          disabled={isExecutionLocked || isReloadingNodes}
          title="Reload Nodes"
        >
          <RefreshCw className={isReloadingNodes ? 'w-4 h-4 animate-spin' : 'w-4 h-4'} />
        </IconButton>

        {/* Theme */}
        <IconButton onClick={toggleTheme} title="Toggle Theme">
          {theme === 'dark' ? <MoonIcon /> : <SunIcon />}
        </IconButton>

        {/* Separator */}
        <div className="w-px h-5 mx-0.5" style={{ backgroundColor: 'var(--color-border-subtle)' }} />

        {/* Load / Save */}
        <IconButton
          onClick={() => fileInputRef.current?.click()}
          disabled={isExecutionLocked}
          title="Load workflow"
          style={{ opacity: isExecutionLocked ? 0.4 : 1 }}
        >
          <LoadIcon />
        </IconButton>
        <IconButton
          onClick={handleSave}
          disabled={isExecutionLocked}
          title="Save workflow"
          style={{ opacity: isExecutionLocked ? 0.4 : 1 }}
        >
          <SaveIcon />
        </IconButton>

        {/* Primary execution controls: settings, recovery, then Run. */}
        <div className="w-px h-5 mx-0.5" style={{ backgroundColor: 'var(--color-border-subtle)' }} />
        <Button
          variant="secondary"
          size="md"
          onClick={openExecutionSettings}
          disabled={isExecuting || isPreflighting || recoveredGraphView !== null}
          icon={<SlidersHorizontal className="h-3.5 w-3.5" />}
          title={`Execution Settings: ${executionSettingsSummary}`}
        >
          <span>Execution Settings</span>
          <span className="ml-1 text-[9px] font-normal" style={{ color: 'var(--color-text-muted)' }}>
            {activeExecutionSettingsConfigured
              ? executionSettingsSummary
              : `Not configured · ${executionSettingsSummary}`}
          </span>
        </Button>
        <Button
          variant="secondary"
          size="md"
          onClick={openRecoveryBrowser}
          disabled={isExecuting || isPreflighting || !isConnected}
          icon={<FolderClock className="h-3.5 w-3.5" />}
          title={isConnected ? 'Inspect or execute a recovery record' : 'Reconnect to open Recovery'}
        >
          Recovery
        </Button>
        {recoveredGraphView && !isExecuting && (
          <Button
            variant="secondary"
            size="md"
            onClick={closeRecoveredGraph}
            disabled={executionState.phase === 'disconnected'}
          >
            Close Recovery
          </Button>
        )}
        {isExecuting ? (
          <Button variant="danger" size="md" onClick={stopFlow} icon={<StopIcon />} disabled={!isConnected}>
            Stop
          </Button>
        ) : (
          <Button
            variant="primary"
            size="md"
            onClick={() => { void runFlow(); }}
            icon={<RunIcon />}
            loading={isPreflighting}
            disabled={isExecutionLocked || !isConnected || recoveredGraphView !== null}
          >
            {isPreflighting ? 'Checking...' : 'Run'}
          </Button>
        )}
      </div>
    </header>
  );
}
