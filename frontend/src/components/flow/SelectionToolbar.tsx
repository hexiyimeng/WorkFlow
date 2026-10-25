import { useState } from 'react';
import { NodeToolbar, Position, getNodesBounds, useViewport } from '@xyflow/react';
import { Trash2, Info, Palette, Boxes, LogIn, Ungroup, SkipForward, Save, Pencil, Plug, Unplug } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';
import { bypassMapping, convertSelection, unpackSubgraph, exposePort, removeBoundary } from '../../utils/subgraphs';
import NodeColorPicker from '../nodes/NodeColorPicker';

export default function SelectionToolbar() {
  const { nodes, edges, subgraphPath, isExecutionLocked, handleDelete, editGraph, enterSubgraph, saveSubgraphToLibrary, addLog, updateNodeData, openNodeInspector } = useFlow();
  const selected = nodes.filter(n => n.selected);
  const single = selected.length === 1 ? selected[0] : null;
  const viewport = useViewport();
  const [showPorts, setShowPorts] = useState(false);
  const [showColors, setShowColors] = useState(false);
  if (!selected.length) return null;
  const editable = selected.filter(n => !n.data.boundary);
  const canBypass = editable.length > 0 && editable.length === selected.length && editable.every(n => bypassMapping(n));
  const action = (operation: () => void) => { try { operation(); } catch (error) { addLog((error as Error).message, 'error'); } };
  return <NodeToolbar nodeId={selected.map(n => n.id)} isVisible={!selected.some(n => n.dragging)} position={getNodesBounds(selected).y * viewport.zoom + viewport.y < 60 ? Position.Bottom : Position.Top} offset={14}>
    <div className="selection-toolbar nodrag nopan" role="toolbar" aria-label="Selected node actions">
      <button aria-label="Delete selected nodes" title="删除所选节点" disabled={isExecutionLocked || !editable.length} onClick={handleDelete}><Trash2 size={16} /></button>
      <button aria-label="Node information" title="节点详情 · 参数 / 信息 / 设置" onClick={openNodeInspector}><Info size={16} /></button>
      <button aria-label="Node color" title="节点颜色" disabled={isExecutionLocked} onClick={() => setShowColors(value => !value)}><Palette size={16} /></button>
      {single && <button aria-label="Rename node" title="重命名节点" disabled={isExecutionLocked}
        onClick={() => { const name = prompt('节点名称', single.data.title || single.data.nodeSpec.display_name); if (name?.trim()) updateNodeData(single.id, { title: name.trim() }); }}><Pencil size={16} /></button>}
      {single?.data.subgraph ? <>
        <button aria-label="Enter selected subgraph" title="进入子图" onClick={() => enterSubgraph(single.id)}><LogIn size={16} /></button>
        <button aria-label="Unpack subgraph" title="展开子图" disabled={isExecutionLocked} onClick={() => action(() => editGraph(graph => unpackSubgraph(graph, single.id)))}><Ungroup size={16} /></button>
        <button aria-label="Save subgraph to library" title="保存子图到工作流库" disabled={isExecutionLocked} onClick={() => saveSubgraphToLibrary(single)}><Save size={16} /></button>
      </> : <button aria-label="Convert selection to subgraph" title="将选区转化为子图（支持单个节点）" disabled={isExecutionLocked || !editable.length || editable.length !== selected.length}
        onClick={() => action(() => editGraph(graph => convertSelection(graph, single ? single.data.title || single.data.nodeSpec.display_name : '新子图')))}><Boxes size={16} /></button>}
      {subgraphPath.length > 0 && single && !single.data.boundary && <button aria-label="Expose subgraph ports" title="公开输入 / 输出到父图" disabled={isExecutionLocked} onClick={() => setShowPorts(value => !value)}><Plug size={16} /></button>}
      {single?.data.boundary && subgraphPath.length > 0 && <button aria-label="Remove subgraph port" title="移除接口及外部连线" disabled={isExecutionLocked}
        onClick={() => editGraph(graph => removeBoundary(graph, single.id))}><Unplug size={16} /></button>}
      {showPorts && single && <div className="node-port-menu">
        <strong>公开到父图</strong>
        {Object.keys({ ...single.data.nodeSpec.input.required, ...single.data.nodeSpec.input.optional }).map(name => <button key={name} disabled={edges.some(e => e.target === single.id && e.targetHandle === name)} onClick={() => action(() => { editGraph(graph => exposePort(graph, single.id, 'input', name)); setShowPorts(false); })}>输入 → {single.data.inputLabels?.[name] || name}</button>)}
        {single.data.nodeSpec.output.map((type, index) => <button key={index} onClick={() => action(() => { editGraph(graph => exposePort(graph, single.id, 'output', String(index))); setShowPorts(false); })}>输出 → {single.data.outputLabels?.[index] || single.data.nodeSpec.output_name?.[index] || type}</button>)}
        <small>参数也可公开；已有连线的输入请先断开。</small>
      </div>}
      <button aria-label="Toggle bypass" title={canBypass ? '忽略 / 恢复节点（Bypass）' : '此节点没有可用于忽略的输入输出对应关系'} disabled={isExecutionLocked || !canBypass}
        data-active={selected.every(n => n.data.bypassed) || undefined} onClick={() => {
          const bypassed = !selected.every(n => n.data.bypassed);
          editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => n.selected ? { ...n, data: { ...n.data, bypassed } } : n) }));
        }}><SkipForward size={16} /></button>
      {showColors && <div className="node-color-menu"><NodeColorPicker color={single?.data.color} disabled={isExecutionLocked} onPresetChosen={() => setShowColors(false)} onChange={color => {
        editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => n.selected ? { ...n, data: { ...n.data, color } } : n) }));
      }} /></div>}
    </div>
  </NodeToolbar>;
}
