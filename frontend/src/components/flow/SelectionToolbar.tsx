import { useState } from 'react';
import { NodeToolbar, Position, getNodesBounds, useViewport } from '@xyflow/react';
import { Trash2, Info, Palette, Boxes, LogIn, Ungroup, SkipForward, Save, Pencil, Plug, Unplug, Group, Scan, MousePointer2, Minimize2, Maximize2 } from 'lucide-react';
import { createGroup, fitGroup, groupContents, isGroup } from '../../utils/groups';
import { useFlow } from '../../hooks/useFlowContext';
import { bypassMapping, convertSelection, unpackSubgraph, exposePort } from '../../utils/subgraphs';
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
  const collapsible = editable.filter(n => !isGroup(n));
  const collapsed = collapsible.every(n => n.data.collapsed);
  const canBypass = editable.length > 0 && editable.length === selected.length && editable.every(n => bypassMapping(n));
  const action = (operation: () => void) => { try { operation(); } catch (error) { addLog((error as Error).message, 'error'); } };
  return <NodeToolbar nodeId={selected.map(n => n.id)} isVisible={!selected.some(n => n.dragging)} position={getNodesBounds(selected).y * viewport.zoom + viewport.y < 60 ? Position.Bottom : Position.Top} offset={14}>
    <div className="selection-toolbar nodrag nopan" role="toolbar" aria-label="Selected node actions">
      <button aria-label="Delete selected nodes" title={selected.some(n => n.data.boundary) ? '删除所选节点 / 子图接口及连线' : '删除所选节点'} disabled={isExecutionLocked} onClick={handleDelete}><Trash2 size={16} /></button>
      <button aria-label="Node information" title="节点详情 · 参数 / 信息 / 设置" onClick={openNodeInspector}><Info size={16} /></button>
      {collapsible.length > 0 && <button aria-label={collapsed ? 'Expand selected nodes' : 'Minimize selected nodes'} title={collapsed ? '展开节点' : '最小化节点'} disabled={isExecutionLocked}
        onClick={() => editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => collapsible.some(c => c.id === n.id) ? { ...n, data: { ...n.data, collapsed: !collapsed } } : n) }))}>{collapsed ? <Maximize2 size={16} /> : <Minimize2 size={16} />}</button>}
      <button aria-label="Node color" title="节点颜色" disabled={isExecutionLocked} onClick={() => setShowColors(value => !value)}><Palette size={16} /></button>
      <button aria-label="Create group" title="为选区创建组（只整理布局）" disabled={isExecutionLocked}
        onClick={() => editGraph(graph => ({ ...graph, nodes: createGroup(graph.nodes) }))}><Group size={16} /></button>
      {single && isGroup(single) && <>
        <button aria-label="Fit group to contents" title="组框适应内部节点" disabled={isExecutionLocked} onClick={() => editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => n.id === single.id ? fitGroup(n, graph.nodes) : n) }))}><Scan size={16} /></button>
        <button aria-label="Select group contents" title="选择组内节点" onClick={() => editGraph(graph => {
          const ids = new Set(groupContents(single, graph.nodes).map(n => n.id));
          return { ...graph, nodes: graph.nodes.map(n => ({ ...n, selected: ids.has(n.id) })) };
        })}><MousePointer2 size={16} /></button>
      </>}
      {single && !single.data.boundary && <button aria-label="Rename node" title="重命名节点" disabled={isExecutionLocked}
        onClick={() => { const name = prompt('节点名称', single.data.title || single.data.nodeSpec.display_name); if (name?.trim()) updateNodeData(single.id, { title: name.trim() }); }}><Pencil size={16} /></button>}
      {single?.data.subgraph ? <>
        <button aria-label="Enter selected subgraph" title="进入子图" onClick={() => enterSubgraph(single.id)}><LogIn size={16} /></button>
        <button aria-label="Unpack subgraph" title="展开子图" disabled={isExecutionLocked} onClick={() => action(() => editGraph(graph => unpackSubgraph(graph, single.id)))}><Ungroup size={16} /></button>
        <button aria-label="Save subgraph to library" title="保存为可复用节点（Blueprint）" disabled={isExecutionLocked} onClick={() => saveSubgraphToLibrary(single)}><Save size={16} /></button>
      </> : <button aria-label="Convert selection to subgraph" title="将选区转化为子图（支持单个节点）" disabled={isExecutionLocked || !editable.length || editable.length !== selected.length}
        onClick={() => action(() => editGraph(graph => convertSelection(graph, single ? single.data.title || single.data.nodeSpec.display_name : '新子图')))}><Boxes size={16} /></button>}
      {subgraphPath.length > 0 && single && !single.data.boundary && !isGroup(single) && <button aria-label="Expose subgraph ports" title="公开输入 / 输出到父图" disabled={isExecutionLocked} onClick={() => setShowPorts(value => !value)}><Plug size={16} /></button>}
      {single?.data.boundary && subgraphPath.length > 0 && <button aria-label="Remove subgraph port" title="移除接口及外部连线" disabled={isExecutionLocked}
        onClick={handleDelete}><Unplug size={16} /></button>}
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
