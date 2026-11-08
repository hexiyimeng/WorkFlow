import type { Graph } from './subgraphs.ts';
import { flattenGraph } from './subgraphs.ts';

export function compileWorkflow(document: Graph): Record<string, { type: string; inputs: Record<string, unknown> }> {
  const flat = flattenGraph(document);
  return Object.fromEntries(flat.nodes.map(node => {
    if (node.data._invalid) throw new Error(`Node type is unavailable: ${node.data.opType}`);
    const inputs = { ...node.data.values };
    for (const edge of flat.edges.filter(e => e.target === node.id)) {
      if (edge.targetHandle) inputs[edge.targetHandle] = [edge.source, Number(edge.sourceHandle || 0)];
    }
    return [node.id, { type: node.data.opType, inputs }];
  }));
}
