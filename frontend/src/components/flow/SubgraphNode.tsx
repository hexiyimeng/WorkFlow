import { useLayoutEffect } from 'react';
import { Handle, Position, useUpdateNodeInternals, type NodeProps, type Node } from '@xyflow/react';
import { ArrowDownToLine, ArrowUpFromLine, Boxes, LogIn } from 'lucide-react';
import type { NodeData } from '../../types';
import { useFlow } from '../../hooks/useFlowContext';

export function SubgraphNode({ id, data, selected }: NodeProps<Node<NodeData>>) {
  const { enterSubgraph } = useFlow();
  const sub = data.subgraph!;
  const update = useUpdateNodeInternals();
  const ports = [...sub.inputs, ...sub.outputs].map(p => `${p.id}:${p.name}:${p.type}`).join('|');
  useLayoutEffect(() => { update(id); }, [id, ports, update]);
  return <article className="subgraph-node workflow-node" data-selected={selected || undefined}
    style={{ borderColor: data.color }} onDoubleClick={event => { event.stopPropagation(); enterSubgraph(id); }}>
    <header className="workflow-node__header" style={{ borderTopColor: data.color }}><Boxes size={17} /><h3>{data.title || 'Subgraph'}</h3>
      <button className="workspace-icon-button nodrag" title="Enter subgraph" aria-label="Enter subgraph" onClick={() => enterSubgraph(id)}><LogIn size={16} /></button></header>
    <div className="subgraph-node__ports"><div>{sub.inputs.map(port => <div key={port.id} title={port.type}>
      <Handle type="target" position={Position.Left} id={port.id} /><span>{port.name}</span>
    </div>)}</div><div>{sub.outputs.map((port, index) => <div key={port.id} title={port.type}>
      <span>{port.name}</span><Handle type="source" position={Position.Right} id={String(index)} />
    </div>)}</div></div>
    <footer>{sub.nodes.filter(n => !n.data.boundary).length} nodes · Double-click to enter</footer>
  </article>;
}

export function BoundaryNode({ data, selected }: NodeProps<Node<NodeData>>) {
  const input = data.boundary === 'input';
  return <article className="boundary-node" data-selected={selected || undefined}>
    <header>{input ? <ArrowDownToLine size={15} /> : <ArrowUpFromLine size={15} />}<span>Subgraph {input ? 'input' : 'output'}</span></header>
    <div>{!input && <Handle type="target" position={Position.Left} id="value" />}
      <strong>{data.title}</strong>{input && <Handle type="source" position={Position.Right} id="0" />}</div>
    <small>{String(data.values.portType || '*')}</small>
  </article>;
}
