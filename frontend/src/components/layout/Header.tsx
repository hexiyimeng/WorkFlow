import { useRef, useState, type ChangeEvent } from 'react';
import { useReactFlow } from '@xyflow/react';
import { Activity, FileDown, FolderOpen, FolderClock, RefreshCw, SlidersHorizontal, Sun, Moon, Plus, X, Play, Square, Undo2, Redo2, Terminal, Workflow } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';
import { Button } from '../ui/Button';
import { IconButton } from '../ui/IconButton';
import { serializeWorkflowDocument, parseWorkflowDocument, hydrateFlowWithLatestSpecs } from '../../utils/workflowPersistence';
import { formatWorkflowExecutionSettingsSummary } from '../../utils/workflowExecutionSettings';

export default function Header() {
  const {
    theme, toggleTheme, workflows, activeWorkflow, activeWorkflowId,
    createWorkflow, switchWorkflow, deleteWorkflow, renameWorkflow, loadWorkflowDocument,
    activeExecutionSettings, activeExecutionSettingsConfigured, openExecutionSettings,
    runFlow, stopFlow, reloadNodes, nodeDefs, dashboardUrl, isReloadingNodes,
    executionState, isConnected, isExecuting, isPreflighting, isExecutionLocked,
    recoveredGraphView, openRecoveryBrowser, closeRecoveredGraph,
    undo, redo, toggleConsole, isConsoleOpen, nodes = [], edges = [],
  } = useFlow();
  const reactFlowInstance = useReactFlow();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [renaming, setRenaming] = useState<string | null>(null);
  const [nameDraft, setNameDraft] = useState('');
  const cancelRenameRef = useRef(false);
  // === Save (export stripped/serialized workflow JSON) ===
  const handleSave = () => {
    if (!reactFlowInstance) return;
    const currentFlow = workflows.find(w => w.id === activeWorkflowId);
    const defaultName = currentFlow ? currentFlow.name : `workflow_${Date.now()}`;
    const finalName = defaultName.trim() || 'workflow';

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

      requestAnimationFrame(() => { void reactFlowInstance.fitView({ padding: .16, maxZoom: 1, duration: 200 }); });
    };
    reader.readAsText(file);
    e.target.value = '';
  };

  const executionSettingsSummary = formatWorkflowExecutionSettingsSummary(activeExecutionSettings);
  const commitRename = (id: string) => {
    if (!cancelRenameRef.current && nameDraft.trim()) renameWorkflow(id, nameDraft.trim());
    cancelRenameRef.current = false;
    setRenaming(null);
  };

  return (
    <header className="workspace-header">
      <input aria-label="Import workflow JSON" type="file" ref={fileInputRef} onChange={handleLoad} accept=".json" className="hidden" />
      <div className="workspace-titlebar">
        <div className="workspace-brand"><span className="workspace-brand__mark"><Workflow size={19} /></span><span>BrainFlow<small>Workflow Studio</small></span></div>
        <div className="workflow-tabs" role="tablist" aria-label="Open workflows">
          {workflows.map(wf => <div key={wf.id} className="workflow-tab" data-active={wf.id === activeWorkflowId || undefined}>
            {renaming === wf.id ? <input autoFocus className="workflow-tab__rename" aria-label="Workflow name" value={nameDraft}
              onChange={event => setNameDraft(event.target.value)} onBlur={() => commitRename(wf.id)}
              onKeyDown={event => {
                if (event.nativeEvent.isComposing) return;
                if (event.key === 'Enter') event.currentTarget.blur();
                if (event.key === 'Escape') { cancelRenameRef.current = true; event.currentTarget.blur(); }
              }} /> : <button role="tab" aria-selected={wf.id === activeWorkflowId} disabled={isExecutionLocked}
                title={`${wf.name} · Double-click to rename`} onClick={() => switchWorkflow(wf.id)}
                onDoubleClick={() => { if (!isExecutionLocked) { setNameDraft(wf.name); setRenaming(wf.id); } }}>
                <Workflow size={13} /><span>{wf.name}</span>
              </button>}
            {workflows.length > 1 && <button className="workflow-tab__close" title={`Close ${wf.name}`} aria-label={`Close ${wf.name}`} disabled={isExecutionLocked}
              onClick={() => { if (confirm(`Close "${wf.name}"?`)) deleteWorkflow(wf.id); }}><X size={12} /></button>}
          </div>)}
          <IconButton title="New Workflow" aria-label="New Workflow" onClick={createWorkflow} disabled={isExecutionLocked}><Plus size={16} /></IconButton>
        </div>
        <div className="workspace-utilities">
          <IconButton title="Dask Dashboard" aria-label="Dask Dashboard" disabled={!dashboardUrl}
            onClick={() => dashboardUrl && window.open(dashboardUrl, '_blank', 'noopener,noreferrer')}><Activity size={16} /></IconButton>
          <IconButton title="Reload Nodes" aria-label="Reload Nodes" disabled={isExecutionLocked || isReloadingNodes} onClick={() => { void reloadNodes(); }}>
            <RefreshCw size={15} className={isReloadingNodes ? 'animate-spin' : ''} /></IconButton>
          <IconButton title="Toggle output" aria-label="Toggle output" active={isConsoleOpen} onClick={toggleConsole}><Terminal size={16} /></IconButton>
          <IconButton title="Toggle Theme" aria-label="Toggle Theme" onClick={toggleTheme}>{theme === 'dark' ? <Moon size={16} /> : <Sun size={16} />}</IconButton>
        </div>
      </div>
      <div className="workspace-actionbar">
        <div className="workspace-file-actions" role="toolbar" aria-label="Workflow actions">
          <button className="workspace-text-button" title="Load workflow" onClick={() => fileInputRef.current?.click()} disabled={isExecutionLocked}><FolderOpen size={15} /><span>Open</span></button>
          <button className="workspace-text-button" title="Save workflow" onClick={handleSave} disabled={isExecutionLocked}><FileDown size={15} /><span>Save</span></button>
          <span className="workspace-divider" />
          <IconButton title="Undo" aria-label="Undo" disabled={isExecutionLocked} onClick={undo}><Undo2 size={15} /></IconButton>
          <IconButton title="Redo" aria-label="Redo" disabled={isExecutionLocked} onClick={redo}><Redo2 size={15} /></IconButton>
          <span className="workspace-document-info">{nodes.length} nodes<span>·</span>{edges.length} connections</span>
        </div>
        <div className="workspace-run-actions">
          {recoveredGraphView && <span className="workspace-recovery-badge">Recovery · Read only</span>}
          <span className="workspace-settings-summary" title={executionSettingsSummary} data-configured={activeExecutionSettingsConfigured || undefined}>
            {!activeExecutionSettingsConfigured && <span className="workspace-status-dot" />}{activeExecutionSettingsConfigured ? executionSettingsSummary : `Not configured · ${executionSettingsSummary}`}
          </span>
          <Button variant="ghost" size="md" onClick={openExecutionSettings} disabled={isExecuting || isPreflighting || recoveredGraphView !== null}
            icon={<SlidersHorizontal size={15} />} title={`Execution Settings: ${executionSettingsSummary}`}><span>Execution Settings</span></Button>
          <Button variant="ghost" size="md" onClick={openRecoveryBrowser} disabled={isExecuting || isPreflighting || !isConnected}
            icon={<FolderClock size={15} />} title={isConnected ? 'Inspect or execute a recovery record' : 'Reconnect to open Recovery'}>
          Recovery
        </Button>
          {recoveredGraphView && !isExecuting && <Button variant="secondary" size="md" onClick={closeRecoveredGraph} disabled={executionState.phase === 'disconnected'}>Close Recovery</Button>}
          <span className="workspace-divider" />
          {isExecuting ? <Button variant="danger" size="md" onClick={stopFlow} disabled={!isConnected} icon={<Square size={13} fill="currentColor" />}>Stop</Button> :
            <Button variant="primary" size="md" className="workspace-run-button" onClick={() => { void runFlow(); }} icon={<Play size={13} fill="currentColor" />}
              loading={isPreflighting} disabled={isExecutionLocked || !isConnected || recoveredGraphView !== null}>
              {isPreflighting ? 'Checking...' : 'Run'}
            </Button>}
        </div>
      </div>
    </header>
  );
}
