import type { CSSProperties } from 'react';
import { NodeResizer, type Node, type NodeProps } from '@xyflow/react';
import { Group } from 'lucide-react';
import type { NodeData } from '../../types';
import { useFlow } from '../../hooks/useFlowContext';
import './group.css';

export default function GroupNode({ id, data, selected }: NodeProps<Node<NodeData>>) {
  const { isExecutionLocked, editGraph } = useFlow();
  return <div className="group-frame" style={{ '--group-color': data.color || '#597da3' } as CSSProperties}>
    <NodeResizer isVisible={selected && !isExecutionLocked} minWidth={240} minHeight={160}
      onResizeEnd={(_, size) => editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => n.id === id
        ? { ...n, position: { x: size.x, y: size.y }, data: { ...n.data, group: { width: size.width, height: size.height } } } : n) }))} />
    <div className="group-frame__title" title="拖动标题一起移动组内节点；按住 Alt 只移动组框。删除组会保留节点。">
      <Group size={19} /><strong>{data.title || '组'}</strong><span>组</span>
    </div>
  </div>;
}
