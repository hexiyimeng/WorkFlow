import type { Edge, Node } from '@xyflow/react';
import type { NodeData, NodeSpec, SubgraphGraph } from '../types';
import { resolveNodeOutputTypes, canConnectPorts } from './portTypes.ts';
import { visibleNodeInputNames } from './inputVisibility.ts';

export type Graph = { nodes: Node<NodeData>[]; edges: Edge[] };
export const newNodeId = () => `node-${crypto.randomUUID()}`;
const primitive = new Set(['STRING', 'INT', 'LONG', 'FLOAT', 'BOOLEAN']);

export function subgraphSpec(graph: SubgraphGraph): NodeSpec {
  return { type: '__subgraph__', display_name: 'Subgraph', category: 'Subgraphs',
    input: { required: Object.fromEntries(graph.inputs.map(p => [p.id, [p.type]])) },
    output: graph.outputs.map(p => p.type), output_name: graph.outputs.map(p => p.name) };
}

export function graphAtPath(root: Graph, path: string[]): Graph {
  let graph = root;
  for (const id of path) {
    const next = graph.nodes.find(n => n.id === id)?.data.subgraph;
    if (!next) return graph;
    graph = next;
  }
  return graph;
}

export function updateGraphAtPath(root: Graph, path: string[], change: (graph: Graph) => Graph): Graph {
  if (!path.length) return change(root);
  const [id, ...rest] = path;
  let edges = root.edges;
  const nodes = root.nodes.map(node => {
    if (node.id !== id || !node.data.subgraph) return node;
    const inner = updateGraphAtPath(node.data.subgraph, rest, change);
    const subgraph = refreshInterface({ ...node.data.subgraph, ...inner });
    edges = edges.flatMap(edge => {
      if (edge.target === id && !subgraph.inputs.some(p => p.id === edge.targetHandle)) return [];
      if (edge.source === id) {
        const previousPort = node.data.subgraph!.outputs[Number(edge.sourceHandle || 0)];
        const newIndex = subgraph.outputs.findIndex(p => p.id === previousPort?.id);
        return newIndex < 0 ? [] : [{ ...edge, sourceHandle: String(newIndex) }];
      }
      return [edge];
    });
    return { ...node, data: { ...node.data, subgraph, nodeSpec: subgraphSpec(subgraph) } };
  });
  return { ...root, nodes, edges };
}

function refreshInterface(subgraph: SubgraphGraph): SubgraphGraph {
  const collect = (kind: 'input' | 'output') => {
    const previous = kind === 'input' ? subgraph.inputs : subgraph.outputs;
    const existing = previous.filter(p => subgraph.nodes.some(n => n.id === p.nodeId && n.data.boundary === kind));
    const added = subgraph.nodes.filter(n => n.data.boundary === kind && !existing.some(p => p.nodeId === n.id));
    return [...existing, ...added.map(n => ({ id: `${kind}_${n.id}`, nodeId: n.id,
      name: n.data.title || kind, type: String(n.data.values.portType || '*') }))];
  };
  const inputs = collect('input').map(port => ({ ...port,
    name: subgraph.nodes.find(n => n.id === port.nodeId)?.data.title || port.name }));
  const outputs = collect('output').map(port => {
    const edge = subgraph.edges.find(e => e.target === port.nodeId);
    const source = subgraph.nodes.find(n => n.id === edge?.source);
    return { ...port, name: subgraph.nodes.find(n => n.id === port.nodeId)?.data.title || port.name,
      type: source ? resolveNodeOutputTypes(source.data.nodeSpec, source.data.values)[Number(edge?.sourceHandle || 0)] || port.type : port.type };
  });
  const nodes = subgraph.nodes.map(node => {
    const port = outputs.find(p => p.nodeId === node.id);
    return port ? { ...node, data: { ...node.data, values: { ...node.data.values, portType: port.type },
      nodeSpec: { ...node.data.nodeSpec, input: { required: { value: [port.type] as [string] } } } } } : node;
  });
  return { ...subgraph, nodes, inputs, outputs };
}

export function exposePort(graph: Graph, nodeId: string, kind: 'input' | 'output', handle: string): Graph {
  const node = graph.nodes.find(n => n.id === nodeId);
  if (!node || node.data.boundary) throw new Error('Select an internal node.');
  const config = node.data.nodeSpec.input.required[handle] || node.data.nodeSpec.input.optional?.[handle];
  const type = kind === 'input' ? String(config?.[0] || '') : resolveNodeOutputTypes(node.data.nodeSpec, node.data.values)[Number(handle)];
  if (!type || (kind === 'input' && (Array.isArray(config?.[0]) || primitive.has(type)))) throw new Error('Only connection ports can be exposed here. Edit scalar parameters inside the subgraph.');
  if (kind === 'input' && graph.edges.some(e => e.target === nodeId && e.targetHandle === handle)) throw new Error('Disconnect this input before exposing it.');
  const id = newNodeId();
  const title = kind === 'input' ? handle : node.data.nodeSpec.output_name?.[Number(handle)] || `Output ${Number(handle) + 1}`;
  const boundary: Node<NodeData> = { id, type: 'boundary', position: { x: kind === 'input' ? node.position.x - 280 : node.position.x + 380, y: node.position.y },
    data: { opType: `__${kind}__`, boundary: kind, title, values: { portType: type },
      nodeSpec: { type: `__${kind}__`, display_name: title, category: 'Interface', input: { required: kind === 'output' ? { value: [type] } : {} }, output: kind === 'input' ? [type] : [] } } };
  const edge = kind === 'input' ? { id: newNodeId(), source: id, sourceHandle: '0', target: nodeId, targetHandle: handle }
    : { id: newNodeId(), source: nodeId, sourceHandle: handle, target: id, targetHandle: 'value' };
  return { nodes: [...graph.nodes, boundary], edges: [...graph.edges, edge] };
}

export function mapGraphNodes(nodes: Node<NodeData>[], change: (node: Node<NodeData>) => Node<NodeData>): Node<NodeData>[] {
  return nodes.map(node => {
    let next = change(node.data.subgraph ? { ...node, data: { ...node.data,
      subgraph: { ...node.data.subgraph, nodes: mapGraphNodes(node.data.subgraph.nodes, change) } } } : node);
    if (next.data.subgraph) {
      const states = next.data.subgraph.nodes.filter(n => !n.data.boundary).map(n => n.data.runState);
      const state = states.includes('failed') ? 'failed' : states.includes('running') ? 'running' : states.includes('submitted') ? 'submitted'
        : states.length && states.every(s => s === 'done') ? 'done' : states.includes('cancelled') ? 'cancelled' : undefined;
      next = { ...next, data: { ...next.data, runState: state } };
    }
    return next;
  });
}

export function convertSelection(graph: Graph, title: string): Graph {
  const selected = graph.nodes.filter(n => n.selected && !n.data.boundary);
  if (!selected.length) throw new Error('Select at least one ordinary node or subgraph.');
  const ids = new Set(selected.map(n => n.id));
  const id = newNodeId();
  const left = Math.min(...selected.map(n => n.position.x));
  const top = Math.min(...selected.map(n => n.position.y));
  const sub: SubgraphGraph = { nodes: selected.map(n => ({ ...n, selected: false,
    position: { x: n.position.x - left + 280, y: n.position.y - top + 100 } })),
    edges: graph.edges.filter(e => ids.has(e.source) && ids.has(e.target)), inputs: [], outputs: [] };
  const inputForTarget = new Map<string, string>();
  const outputForSource = new Map<string, number>();
  for (const node of selected) {
    const visible = visibleNodeInputNames(node.data.nodeSpec, node.data.values);
    for (const [name, config] of Object.entries({ ...node.data.nodeSpec.input.required, ...node.data.nodeSpec.input.optional })) {
      const incoming = graph.edges.find(e => e.target === node.id && e.targetHandle === name);
      if (!visible.has(name) || (incoming && ids.has(incoming.source))) continue;
      if (Array.isArray(config[0]) || primitive.has(String(config[0]))) continue;
      const boundaryId = newNodeId();
      const portId = `in_${sub.inputs.length}`;
      const portName = selected.length === 1 ? name : `${node.data.title || node.data.nodeSpec.display_name} · ${name}`;
      sub.inputs.push({ id: portId, name: portName, type: String(config[0]), nodeId: boundaryId });
      inputForTarget.set(`${node.id}:${name}`, portId);
      sub.nodes.push({ id: boundaryId, type: 'boundary', position: { x: 0, y: 100 + 120 * sub.inputs.length },
        data: { opType: '__input__', title: portName, boundary: 'input', values: { portType: String(config[0]) },
          nodeSpec: { type: '__input__', display_name: portName, category: 'Interface', input: { required: {} }, output: [String(config[0])] } } });
      sub.edges.push({ id: newNodeId(), source: boundaryId, sourceHandle: '0', target: node.id, targetHandle: name });
    }
    resolveNodeOutputTypes(node.data.nodeSpec, node.data.values).forEach((type, index) => {
      const boundaryId = newNodeId();
      const name = node.data.nodeSpec.output_name?.[index] || `Output ${index + 1}`;
      outputForSource.set(`${node.id}:${index}`, sub.outputs.length);
      sub.outputs.push({ id: `out_${sub.outputs.length}`, name, type, nodeId: boundaryId });
      sub.nodes.push({ id: boundaryId, type: 'boundary', position: { x: 900, y: 100 + 120 * sub.outputs.length },
        data: { opType: '__output__', title: name, boundary: 'output', values: { portType: type },
          nodeSpec: { type: '__output__', display_name: name, category: 'Interface', input: { required: { value: [type] } }, output: [] } } });
      sub.edges.push({ id: newNodeId(), source: node.id, sourceHandle: String(index), target: boundaryId, targetHandle: 'value' });
    });
  }
  const edges = graph.edges.filter(e => !(ids.has(e.source) && ids.has(e.target))).map(edge => {
    const e = { ...edge };
    if (ids.has(e.target)) {
      const port = inputForTarget.get(`${e.target}:${e.targetHandle}`);
      if (!port) throw new Error('Cannot expose this input as a subgraph port.');
      e.target = id; e.targetHandle = port;
    }
    if (ids.has(e.source)) { e.sourceHandle = String(outputForSource.get(`${e.source}:${e.sourceHandle || '0'}`)); e.source = id; }
    return e;
  });
  return { nodes: [...graph.nodes.filter(n => !ids.has(n.id)).map(n => ({ ...n, selected: false })),
    { id, type: 'subgraph', position: { x: left, y: top }, selected: true,
      data: { opType: '__subgraph__', title, values: {}, nodeSpec: subgraphSpec(sub), subgraph: sub } }], edges };
}

// Expand wrappers first, then resolve virtual interface nodes. Atomic IDs remain
// stable, including Window recovery anchors and runtime status messages.
function expand(graph: Graph, depth = 0): Graph {
  if (depth > 32) throw new Error('Subgraph nesting exceeds 32 levels.');
  let nodes: Node<NodeData>[] = [];
  const edges = [...graph.edges];
  for (const node of graph.nodes) {
    if (!node.data.subgraph) { nodes.push(node); continue; }
    const sub = node.data.subgraph;
    const inner = expand(sub, depth + 1);
    nodes = nodes.concat(inner.nodes);
    edges.push(...inner.edges);
    for (const edge of edges) {
      // Never mutate the source document's edges.
      if (edge.target === node.id || edge.source === node.id) {
        const index = edges.indexOf(edge);
        const next = { ...edge };
        if (next.target === node.id) {
          const port = sub.inputs.find(p => p.id === next.targetHandle);
          if (!port) throw new Error(`Missing input on ${node.data.title || node.id}.`);
          next.target = port.nodeId; next.targetHandle = 'value';
        }
        if (next.source === node.id) {
          const port = sub.outputs[Number(next.sourceHandle || 0)];
          if (!port) throw new Error(`Missing output on ${node.data.title || node.id}.`);
          next.source = port.nodeId; next.sourceHandle = '0';
        }
        edges[index] = next;
      }
    }
  }
  return { nodes, edges };
}

export function flattenGraph(graph: Graph): Graph {
  const flat = expand(graph);
  const byId = new Map(flat.nodes.map(n => [n.id, n]));
  if (byId.size !== flat.nodes.length) throw new Error('Duplicate node IDs in workflow.');
  const resolve = (id: string, handle: string, seen = new Set<string>()): { source: string; sourceHandle: string } | null => {
    const node = byId.get(id);
    if (!node) throw new Error(`Connection references missing node ${id}.`);
    if (!node.data.boundary && !node.data.bypassed) return { source: id, sourceHandle: handle };
    if (seen.has(id)) throw new Error('Cyclic subgraph or bypass connection.');
    const nextSeen = new Set(seen).add(id);
    const input = node.data.boundary ? 'value' : bypassMapping(node)?.[handle];
    if (!input) throw new Error(`Node ${node.data.title || node.data.nodeSpec.display_name} cannot be bypassed.`);
    const edge = flat.edges.find(e => e.target === id && e.targetHandle === input);
    if (!edge) {
      if (node.data.boundary === 'input') return null;
      throw new Error(`Unconnected ${node.data.boundary === 'output' ? 'subgraph output' : 'bypass input'}: ${node.data.title || id}.`);
    }
    return resolve(edge.source, edge.sourceHandle || '0', nextSeen);
  };
  const nodes = flat.nodes.filter(n => !n.data.boundary && !n.data.bypassed);
  const edges: Edge[] = [];
  for (const edge of flat.edges) {
    const target = byId.get(edge.target);
    if (!target) throw new Error(`Connection references missing node ${edge.target}.`);
    if (target.data.boundary || target.data.bypassed) continue;
    const source = resolve(edge.source, edge.sourceHandle || '0');
    if (source) {
      const sourceNode = byId.get(source.source)!;
      const output = resolveNodeOutputTypes(sourceNode.data.nodeSpec, sourceNode.data.values)[Number(source.sourceHandle)];
      const input = target.data.nodeSpec.input.required[edge.targetHandle || ''] || target.data.nodeSpec.input.optional?.[edge.targetHandle || ''];
      if (!output || !input || typeof input[0] !== 'string' || !canConnectPorts(output, input[0]).ok)
        throw new Error(`Incompatible connection: ${sourceNode.data.title || sourceNode.id} → ${target.data.title || target.id}.`);
      if (edges.some(e => e.target === edge.target && e.targetHandle === edge.targetHandle)) throw new Error('Multiple connections to the same input.');
      edges.push({ ...edge, ...source });
    }
  }
  const visited = new Set<string>(), visiting = new Set<string>();
  const visit = (id: string) => {
    if (visiting.has(id)) throw new Error('Workflow contains a dependency cycle.');
    if (visited.has(id)) return;
    visiting.add(id);
    for (const edge of edges.filter(e => e.target === id)) visit(edge.source);
    visiting.delete(id); visited.add(id);
  };
  nodes.forEach(n => visit(n.id));
  return { nodes, edges };
}

export function unpackSubgraph(graph: Graph, id: string): Graph {
  const wrapper = graph.nodes.find(n => n.id === id);
  if (!wrapper?.data.subgraph) return graph;
  // Only expand the selected level; preserve nested wrappers.
  const sub = wrapper.data.subgraph;
  const boundaryIds = new Set([...sub.inputs, ...sub.outputs].map(p => p.nodeId));
  const expandedEdges = [...sub.edges, ...graph.edges.map(edge => {
    const next = { ...edge };
    if (next.target === id) {
      const port = sub.inputs.find(p => p.id === next.targetHandle);
      if (!port) throw new Error('Missing subgraph input.');
      next.target = port.nodeId; next.targetHandle = 'value';
    }
    if (next.source === id) {
      const port = sub.outputs[Number(next.sourceHandle || 0)];
      if (!port) throw new Error('Missing subgraph output.');
      next.source = port.nodeId; next.sourceHandle = '0';
    }
    return next;
  })];
  const resolve = (edge: Edge, seen = new Set<string>()): Edge | null => {
    if (!boundaryIds.has(edge.source)) return edge;
    if (seen.has(edge.source)) throw new Error('Cyclic subgraph interface.');
    const previous = expandedEdges.find(e => e.target === edge.source);
    if (!previous) {
      if (sub.inputs.some(p => p.nodeId === edge.source)) return null;
      throw new Error('Connect this subgraph output before unpacking.');
    }
    return resolve({ ...edge, source: previous.source, sourceHandle: previous.sourceHandle }, new Set(seen).add(edge.source));
  };
  const edges = expandedEdges.filter(e => !boundaryIds.has(e.target)).flatMap(edge => {
    const resolved = resolve(edge); return resolved ? [resolved] : [];
  });
  return { nodes: [...graph.nodes.filter(n => n.id !== id).map(n => ({ ...n, selected: false })),
    ...sub.nodes.filter(n => !boundaryIds.has(n.id)).map(n => ({ ...n, selected: true,
      position: { x: n.position.x + wrapper.position.x, y: n.position.y + wrapper.position.y } }))], edges };

}

export function bypassMapping(node: Node<NodeData>): Record<string, string> | null {
  if (node.data.subgraph || node.data.boundary || node.data.nodeSpec.output_node) return null;
  // Rechunk is the only built-in shape-preserving identity bypass. Algorithms
  // can explicitly opt in through NodeSpec; matching array types alone is unsafe.
  const explicit = node.data.nodeSpec.bypassMapping;
  const mapping = explicit || (node.data.opType === 'DaskRechunk'
    ? { '0': Object.keys(node.data.nodeSpec.input.required).find(k => !primitive.has(String(node.data.nodeSpec.input.required[k][0]))) || '' } : null);
  if (!mapping || !node.data.nodeSpec.output.length) return null;
  return node.data.nodeSpec.output.every((_, i) => {
    const input = node.data.nodeSpec.input.required[mapping[String(i)]] || node.data.nodeSpec.input.optional?.[mapping[String(i)]];
    return input && typeof input[0] === 'string' && canConnectPorts(resolveNodeOutputTypes(node.data.nodeSpec, node.data.values)[i], input[0]).ok;
  }) ? mapping : null;
}

export function cloneGraphIds(graph: Graph): Graph {
  const idMap = new Map(graph.nodes.map(n => [n.id, newNodeId()]));
  return { nodes: graph.nodes.map(n => {
    const sub = n.data.subgraph;
    const cloned = sub ? cloneGraphIds(sub) : null;
    const innerIds = sub && cloned ? new Map(sub.nodes.map((inner, i) => [inner.id, cloned.nodes[i].id])) : null;
    return { ...n, id: idMap.get(n.id)!, data: { ...n.data,
      ...(sub && cloned && innerIds ? { subgraph: { ...sub, ...cloned,
        inputs: sub.inputs.map(p => ({ ...p, nodeId: innerIds.get(p.nodeId)! })),
        outputs: sub.outputs.map(p => ({ ...p, nodeId: innerIds.get(p.nodeId)! })) } } : {}) } };
  }), edges: graph.edges.map(e => ({ ...e, id: newNodeId(), source: idMap.get(e.source)!, target: idMap.get(e.target)! })) };
}
