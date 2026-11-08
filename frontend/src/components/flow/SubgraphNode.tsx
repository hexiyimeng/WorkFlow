import { useLayoutEffect } from 'react';
import { Handle, Position, useUpdateNodeInternals, type NodeProps, type Node } from '@xyflow/react';
import { ArrowDownToLine, ArrowUpFromLine, Boxes, LogIn, Plus, ChevronDown } from 'lucide-react';
import type { NodeData } from '../../types';
import { useFlow } from '../../hooks/useFlowContext';
import { NodeParameter } from '../nodes/NodeParameter';
import { isWidgetInput, nodeInputValues, withNodeInputValue, inputSourceLabel } from '../../utils/nodeInputs';
import CollapsedPorts from '../nodes/CollapsedPorts';
import { isGroup } from '../../utils/groups';
import { nodeColorStyle } from '../../utils/nodePresentation';

export function SubgraphNode({ id, data, selected }: NodeProps<Node<NodeData>>) {
  const { enterSubgraph, editGraph, updateNodeData, nodes, edges, isExecutionLocked: locked } = useFlow();
  const collapsed = !!data.collapsed;
  const sub = data.subgraph!;
  const update = useUpdateNodeInternals();
  const ports = [...sub.inputs, ...sub.outputs].map(p => `${p.id}:${p.name}:${p.type}`).join('|');
  const linkedPorts = edges.filter(e => e.target === id).map(e => e.targetHandle).join('|');
  const values = nodeInputValues({ id, data }, { nodes, edges });
  useLayoutEffect(() => { update(id); }, [id, ports, linkedPorts, collapsed, update]);
  return <article className="subgraph-node workflow-node" data-selected={selected || undefined} data-collapsed={collapsed || undefined}
    style={nodeColorStyle(data.color)} onDoubleClick={event => { event.stopPropagation(); enterSubgraph(id); }}>
    <header className="workflow-node__header">
      <button className="workflow-node__collapse nodrag" title={collapsed ? '展开节点' : '最小化节点'} aria-label={collapsed ? 'Expand node' : 'Collapse node'} aria-expanded={!collapsed} disabled={locked}
        onDoubleClick={event => event.stopPropagation()} onClick={() => updateNodeData(id, { collapsed: !collapsed })}><ChevronDown size={14} /></button>
      <Boxes size={17} /><h3 className="workflow-node__title">{data.title || '子图'}</h3>
      <button className="workspace-icon-button nodrag" title="进入子图" aria-label="Enter subgraph" onClick={() => enterSubgraph(id)}><LogIn size={16} /></button></header>
    {collapsed ? <CollapsedPorts nodeId={id} inputs={sub.inputs} outputs={sub.outputs.map((p, i) => ({ id: String(i), name: p.name }))} /> : <>
    <div className="subgraph-node__ports"><div>{sub.inputs.filter(port => !isWidgetInput(port.config)).map(port => <div key={port.id} title={port.type}>
      <Handle type="target" position={Position.Left} id={port.id} isConnectable={!locked} /><span>{port.name}</span>
    </div>)}</div><div>{sub.outputs.map((port, index) => <div key={port.id} title={port.type}>
      <span>{port.name}</span><Handle type="source" position={Position.Right} id={String(index)} isConnectable={!locked} />
    </div>)}</div></div>
    <div className="node-parameters">{sub.inputs.filter(port => isWidgetInput(port.config)).map(port => {
      const linked = edges.some(e => e.target === id && e.targetHandle === port.id);
      return <NodeParameter key={port.id} name={port.id} label={port.name} config={port.config!} value={values[port.id]} disabled={locked || linked} linked={linked}
        sourceLabel={inputSourceLabel(id, port.id, { nodes, edges })}
        onChange={(_, value) => editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => n.id === id ? withNodeInputValue(n, port.id, value) : n) }))}
        slot={<Handle type="target" position={Position.Left} id={port.id} className="node-widget-slot" aria-label={`Input ${port.name} (${port.type})`} isConnectable={!locked} />} />;
    })}</div>
    <footer>{sub.nodes.filter(n => !n.data.boundary && !isGroup(n)).length} 个节点 · 双击进入子图</footer></>}
  </article>;
}

export function BoundaryNode({ id, data, selected }: NodeProps<Node<NodeData>>) {
  const { updateNodeData, isExecutionLocked: locked } = useFlow();
  const input = data.boundary === 'input';
  return <article className="boundary-node" data-selected={selected || undefined} style={nodeColorStyle(data.color)}>
    <header>{input ? <ArrowDownToLine size={15} /> : <ArrowUpFromLine size={15} />}<span>子图{input ? '输入' : '输出'}</span></header>
    <div>{!input && <Handle type="target" position={Position.Left} id="value" />}
      <strong>{data.title}</strong>{input && <Handle type="source" position={Position.Right} id="0" />}</div>
    <small>{String(data.values.portType || '*')}</small>
    {input && data.boundaryConfig && <div className="boundary-default"><NodeParameter name="value" label="默认值" config={data.boundaryConfig} value={data.values.value} disabled={locked}
      onChange={(_, value) => updateNodeData(id, { values: { ...data.values, value } })} /></div>}
  </article>;
}

export function InterfacePlaceholder({ data }: NodeProps<Node<NodeData>>) {
  const input = data.opType === '__add_input__';
  return <div className="interface-placeholder"><Plus size={14} /><span>新增{input ? '输入' : '输出'}</span><small>{input ? '连接到内部参数或数据输入' : '从内部输出连接到此处'}</small>
    <Handle type={input ? 'source' : 'target'} position={input ? Position.Right : Position.Left} id="new" aria-label={input ? '新增子图输入' : '新增子图输出'} />
  </div>;
}
