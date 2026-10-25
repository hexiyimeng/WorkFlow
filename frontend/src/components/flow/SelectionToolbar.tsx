import { createPortal } from 'react-dom';
import { useState } from 'react';
import { NodeToolbar, Position, getNodesBounds, useViewport } from '@xyflow/react';
import { Trash2, Info, Palette, Boxes, LogIn, Ungroup, SkipForward, Save, X, Pencil, Plug, Unplug } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';
import { bypassMapping, convertSelection, unpackSubgraph, exposePort } from '../../utils/subgraphs';

export default function SelectionToolbar() {
  const { nodes, edges, subgraphPath, isExecutionLocked, handleDelete, editGraph, enterSubgraph, saveSubgraphToLibrary, addLog, updateNodeData } = useFlow();
  const selected = nodes.filter(n => n.selected);
  const single = selected.length === 1 ? selected[0] : null;
  const [showInfo, setShowInfo] = useState(false);
  const viewport = useViewport();
  const [showPorts, setShowPorts] = useState(false);
  const [showColors, setShowColors] = useState(false);
  if (!selected.length) return null;
  const editable = selected.filter(n => !n.data.boundary);
  const canBypass = editable.length > 0 && editable.length === selected.length && editable.every(n => bypassMapping(n));
  const action = (operation: () => void) => { try { operation(); } catch (error) { addLog((error as Error).message, 'error'); } };
  return <>
    <NodeToolbar nodeId={selected.map(n => n.id)} isVisible={!selected.some(n => n.dragging)} position={getNodesBounds(selected).y * viewport.zoom + viewport.y < 60 ? Position.Bottom : Position.Top} offset={14}>
      <div className="selection-toolbar nodrag nopan" role="toolbar" aria-label="Selected node actions">
        <button aria-label="Delete selected nodes" title="Delete selected nodes" disabled={isExecutionLocked || !editable.length} onClick={handleDelete}><Trash2 size={16} /></button>
        <button aria-label="Node information" title="Node information" onClick={() => setShowInfo(true)}><Info size={16} /></button>
        <button aria-label="Node color" title="Node color" disabled={isExecutionLocked} onClick={() => setShowColors(value => !value)}><Palette size={16} /></button>
        {single && <button aria-label="Rename node" title="Rename node" disabled={isExecutionLocked}
          onClick={() => { const name = prompt('Node title', single.data.title || single.data.nodeSpec.display_name); if (name?.trim()) updateNodeData(single.id, { title: name.trim() }); }}><Pencil size={16} /></button>}
        {single?.data.subgraph ? <>
          <button aria-label="Enter selected subgraph" title="Enter subgraph" onClick={() => enterSubgraph(single.id)}><LogIn size={16} /></button>
          <button aria-label="Unpack subgraph" title="Unpack subgraph" disabled={isExecutionLocked} onClick={() => action(() => editGraph(graph => unpackSubgraph(graph, single.id)))}><Ungroup size={16} /></button>
          <button aria-label="Save subgraph to library" title="Save subgraph to library" disabled={isExecutionLocked} onClick={() => saveSubgraphToLibrary(single)}><Save size={16} /></button>
        </> : <button aria-label="Convert selection to subgraph" title="Convert selection to subgraph" disabled={isExecutionLocked || !editable.length || editable.length !== selected.length}
          onClick={() => action(() => editGraph(graph => convertSelection(graph, single ? single.data.title || single.data.nodeSpec.display_name : 'Subgraph')))}><Boxes size={16} /></button>}
        {subgraphPath.length > 0 && single && !single.data.boundary && <button aria-label="Expose subgraph ports" title="Expose input/output ports" disabled={isExecutionLocked} onClick={() => setShowPorts(value => !value)}><Plug size={16} /></button>}
        {single?.data.boundary && subgraphPath.length > 0 && <button aria-label="Remove subgraph port" title="Remove port and its external connections" disabled={isExecutionLocked}
          onClick={() => editGraph(graph => ({ nodes: graph.nodes.filter(n => n.id !== single.id), edges: graph.edges.filter(e => e.source !== single.id && e.target !== single.id) }))}><Unplug size={16} /></button>}
        {showPorts && single && <div className="node-port-menu">
          <strong>Expose to parent graph</strong>
          {Object.entries({ ...single.data.nodeSpec.input.required, ...single.data.nodeSpec.input.optional }).filter(([, config]) => typeof config[0] === 'string' && !['STRING','INT','LONG','FLOAT','BOOLEAN'].includes(config[0])).map(([name]) => <button key={name} disabled={edges.some(e => e.target === single.id && e.targetHandle === name)} onClick={() => action(() => { editGraph(graph => exposePort(graph, single.id, 'input', name)); setShowPorts(false); })}>Input ? {name}</button>)}
          {single.data.nodeSpec.output.map((type, index) => <button key={index} onClick={() => action(() => { editGraph(graph => exposePort(graph, single.id, 'output', String(index))); setShowPorts(false); })}>Output ? {single.data.nodeSpec.output_name?.[index] || type}</button>)}
          <small>Edit scalar parameters by entering the subgraph.</small>
        </div>}
        <button aria-label="Toggle bypass" title={canBypass ? 'Toggle bypass' : 'Bypass requires an explicit, compatible input/output mapping'} disabled={isExecutionLocked || !canBypass}
          data-active={selected.every(n => n.data.bypassed) || undefined} onClick={() => {
            const bypassed = !selected.every(n => n.data.bypassed);
            editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => n.selected ? { ...n, data: { ...n.data, bypassed } } : n) }));
          }}><SkipForward size={16} /></button>
        {showColors && <div className="node-color-menu" role="group" aria-label="Node colors">
          {[undefined, '#779ac3', '#6fa795', '#c59b62', '#a08bd5', '#cd7f85', '#888888'].map((color, index) => <button key={color || 'default'}
            aria-label={color ? `Set color ${color}` : 'Reset node color'} title={color || 'Default'} style={{ background: color || 'var(--color-bg-surface-2)' }}
            onClick={() => { editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => n.selected ? { ...n, data: { ...n.data, color } } : n) })); setShowColors(false); }}>{index === 0 ? '×' : ''}</button>)}
        </div>}
      </div>
    </NodeToolbar>
    {showInfo && createPortal(<div className="library-modal-backdrop nodrag nopan" onKeyDown={event => { if (event.key === 'Escape') setShowInfo(false); }} onClick={() => setShowInfo(false)}><section className="library-modal node-info-dialog" role="dialog" aria-modal="true" aria-label="Node information" onClick={event => event.stopPropagation()}>
      <header><h2>{single?.data.title || single?.data.nodeSpec.display_name || `${selected.length} selected nodes`}</h2><button autoFocus aria-label="Close node information" onClick={() => setShowInfo(false)}><X size={18} /></button></header>
      {selected.map(node => <div key={node.id}><p>{node.data.nodeSpec.description || (node.data.subgraph ? 'An independently editable group of nodes.' : node.data.opType)}</p>
        <dl><dt>ID</dt><dd>{node.id}</dd><dt>Type</dt><dd>{node.data.opType}</dd><dt>Worker type</dt><dd>{node.data.nodeSpec.required_worker_profile || '—'}</dd></dl>
        <pre>{JSON.stringify({ inputs: node.data.nodeSpec.input, outputs: node.data.nodeSpec.output, parameters: node.data.values }, null, 2)}</pre></div>)}
    </section></div>, document.body)}
  </>;
}
