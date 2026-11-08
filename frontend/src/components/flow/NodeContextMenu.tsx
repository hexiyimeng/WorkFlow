import { useState } from 'react';
import { Copy, Trash2, Pencil, Info, Palette, Minimize2, Maximize2, Boxes, LogIn, Ungroup, Save, SkipForward, Group, Scan, Unplug } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';
import { bypassMapping, convertSelection, unpackSubgraph } from '../../utils/subgraphs';
import { createGroup, fitGroup, groupContents, isGroup } from '../../utils/groups';
import ActionMenu, { type MenuAction } from '../ui/ActionMenu';
import NodeColorPicker from '../nodes/NodeColorPicker';

export default function NodeContextMenu({ x, y, onClose }: { x: number; y: number; onClose: () => void }) {
  const { nodes, isExecutionLocked: locked, editGraph, updateNodeData, handleCopy, handleDelete,
    enterSubgraph, saveSubgraphToLibrary, openNodeInspector, addLog } = useFlow();
  const [colors, setColors] = useState(false);
  const selected = nodes.filter(n => n.selected);
  const single = selected.length === 1 ? selected[0] : undefined;
  const editable = selected.length > 0 && selected.every(n => !n.data.boundary);
  const collapsible = selected.filter(n => !n.data.boundary && !isGroup(n));
  const collapsed = collapsible.every(n => n.data.collapsed);
  const items: MenuAction[] = [];
  const add = (item: MenuAction) => items.push({ ...item, action: () => { try { item.action(); } catch (error) { addLog((error as Error).message, 'error'); } } });
  if (single && !single.data.boundary) add({ label: '重命名', icon: <Pencil size={16} />, disabled: locked, action: () => {
    const title = prompt(isGroup(single) ? '组名称' : '节点名称', single.data.title || single.data.nodeSpec.display_name);
    if (title?.trim()) updateNodeData(single.id, { title: title.trim() });
  } });
  add({ label: '复制', icon: <Copy size={16} />, shortcut: 'Ctrl+C', disabled: locked || !editable, action: handleCopy });
  add({ label: '节点信息', icon: <Info size={16} />, action: openNodeInspector });
  if (collapsible.length) add({ label: collapsed ? '展开节点' : '最小化节点', icon: collapsed ? <Maximize2 size={16} /> : <Minimize2 size={16} />, disabled: locked,
    action: () => editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => collapsible.some(c => c.id === n.id) ? { ...n, data: { ...n.data, collapsed: !collapsed } } : n) })) });
  add({ label: '颜色', icon: <Palette size={16} />, disabled: locked, keepOpen: true, action: () => setColors(value => !value) });
  if (single?.data.subgraph) {
    add({ label: '进入子图', icon: <LogIn size={16} />, separator: true, action: () => enterSubgraph(single.id) });
    add({ label: '解包子图', icon: <Ungroup size={16} />, disabled: locked, action: () => editGraph(graph => unpackSubgraph(graph, single.id)) });
    add({ label: '保存为子图蓝图', icon: <Save size={16} />, disabled: locked, action: () => saveSubgraphToLibrary(single) });
  } else if (editable) add({ label: '将选区转化为子图', icon: <Boxes size={16} />, separator: true, disabled: locked,
    action: () => editGraph(graph => convertSelection(graph, single ? single.data.title || single.data.nodeSpec.display_name : '新子图')) });
  if (single && isGroup(single)) {
    add({ label: '组框适应内容', icon: <Scan size={16} />, disabled: locked, action: () => editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => n.id === single.id ? fitGroup(n, graph.nodes) : n) })) });
    add({ label: '选择组内节点', icon: <Group size={16} />, action: () => editGraph(graph => {
      const ids = new Set(groupContents(single, graph.nodes).map(n => n.id));
      return { ...graph, nodes: graph.nodes.map(n => ({ ...n, selected: ids.has(n.id) })) };
    }) });
  } else if (editable) add({ label: '为选区创建组', icon: <Group size={16} />, disabled: locked, action: () => editGraph(graph => ({ ...graph, nodes: createGroup(graph.nodes) })) });
  if (editable && selected.every(n => bypassMapping(n))) add({ label: selected.every(n => n.data.bypassed) ? '取消忽略' : '忽略节点', icon: <SkipForward size={16} />, disabled: locked,
    action: () => editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => n.selected ? { ...n, data: { ...n.data, bypassed: !selected.every(s => s.data.bypassed) } } : n) })) });
  if (single?.data.boundary) add({ label: '移除接口及连线', icon: <Unplug size={16} />, disabled: locked, separator: true, action: handleDelete });
  else add({ label: single && isGroup(single) ? '删除组框（保留节点）' : '删除', icon: <Trash2 size={16} />, shortcut: 'Delete', separator: true, disabled: locked, action: handleDelete });
  return <ActionMenu x={x} y={y} label="节点右键菜单" items={items} onClose={onClose}>
    {colors && <NodeColorPicker color={single?.data.color} disabled={locked} onPresetChosen={onClose}
      onChange={color => editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => n.selected ? { ...n, data: { ...n.data, color } } : n) }))} />}
  </ActionMenu>;
}
