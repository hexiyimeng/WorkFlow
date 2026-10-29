import type { Node } from '@xyflow/react';
import type { NodeData } from '../types';
import type { Graph } from './subgraphs';
import { inputDefault, isWidgetInput, nodeInputValues, withNodeInputValue } from './nodeInputs';
import { visibleNodeInputNames } from './inputVisibility';

export const inspectorNodeTitle = (node: Node<NodeData>) => node.data.title || node.data.nodeSpec.display_name;
export function inspectorSearchText(node: Node<NodeData>): string {
  return [inspectorNodeTitle(node), node.data.opType,
    ...Object.keys({ ...node.data.nodeSpec.input.required, ...node.data.nodeSpec.input.optional }),
    ...Object.values(node.data.inputLabels || {}), ...Object.values(node.data.outputLabels || {}),
    ...node.data.nodeSpec.output, ...(node.data.nodeSpec.output_name || []),
    ...(node.data.subgraph ? [...node.data.subgraph.inputs, ...node.data.subgraph.outputs].map(p => p.name) : []),
    ...(node.data.subgraph?.nodes.filter(n => !n.data.boundary && !n.data.group).map(inspectorSearchText) || []),
  ].join(' ').toLowerCase();
}
export function resettableInputs(node: Node<NodeData>, graph: Graph) {
  const visible = visibleNodeInputNames(node.data.nodeSpec, nodeInputValues(node, graph));
  return Object.entries({ ...node.data.nodeSpec.input.required, ...node.data.nodeSpec.input.optional })
    .filter(([name, config]) => visible.has(name) && isWidgetInput(config)
      && !graph.edges.some(e => e.target === node.id && e.targetHandle === name));
}
export function resetNodeParameters(graph: Graph, id: string): Graph {
  return { ...graph, nodes: graph.nodes.map(node => node.id !== id ? node
    : resettableInputs(node, graph).reduce((next, [name, config]) => withNodeInputValue(next, name, inputDefault(config)), node)) };
}
