import { Handle, Position, useNodeConnections } from '@xyflow/react';

/** Keep every real handle mounted when minimized, so existing links remain valid. */
export default function CollapsedPorts({ nodeId, inputs, outputs }: {
  nodeId: string; inputs: { id: string; name: string }[]; outputs: { id: string; name: string }[];
}) {
  const links = useNodeConnections({ id: nodeId });
  const connectedInputs = inputs.filter(p => links.some(e => e.target === nodeId && e.targetHandle === p.id));
  const connectedOutputs = outputs.filter(p => links.some(e => e.source === nodeId && e.sourceHandle === p.id));
  return <div className="collapsed-ports">
    {/* Invisible anchors retain individual IDs; each side shares one visual endpoint.
        Expand before connecting, rather than guessing a port from overlapping handles. */}
    {inputs.map(p => <Handle key={`in-${p.id}`} type="target" position={Position.Left} id={p.id} aria-hidden isConnectable={false} />)}
    {outputs.map(p => <Handle key={`out-${p.id}`} type="source" position={Position.Right} id={p.id} aria-hidden isConnectable={false} />)}
    {connectedInputs.length > 0 && <span className="collapsed-ports__marker collapsed-ports__marker--input" title={`输入：${connectedInputs.map(p => p.name).join('、')}（展开节点编辑连线）`} />}
    {connectedOutputs.length > 0 && <span className="collapsed-ports__marker collapsed-ports__marker--output" title={`输出：${connectedOutputs.map(p => p.name).join('、')}（展开节点编辑连线）`} />}
  </div>;
}
