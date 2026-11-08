import type { Node, Edge } from '@xyflow/react';
import type { InputDefinition, NodeData } from '../types';

const scalar = new Set(['INT', 'LONG', 'FLOAT', 'STRING', 'BOOLEAN']);
/** Display connection metadata only. Never evaluate upstream tasks to preview an input. */
export function inputSourceLabel(nodeId: string, input: string, graph: { nodes: Node<NodeData>[]; edges: Edge[] }): string | undefined {
  const edge = graph.edges.find(e => e.target === nodeId && e.targetHandle === input);
  if (!edge) return undefined;
  const source = graph.nodes.find(n => n.id === edge.source);
  if (!source) return `节点 ${edge.source} · 输出 ${edge.sourceHandle ?? '0'}`;
  const index = Number(edge.sourceHandle ?? 0);
  const title = source.data.title || source.data.nodeSpec.display_name || source.data.opType;
  const output = source.data.outputLabels?.[String(index)] || source.data.subgraph?.outputs[index]?.name
    || source.data.nodeSpec.output_name?.[index] || (source.data.boundary === 'input' ? '子图输入' : `输出 ${index + 1}`);
  return `${title} · ${output}`;
}
export function isWidgetInput(config: InputDefinition | undefined): boolean {
  return !!config && (Array.isArray(config[0]) || scalar.has(config[0]));
}
export function inputPortType(config: InputDefinition | undefined): string {
  return !config ? '*' : Array.isArray(config[0]) ? 'STRING' : config[0];
}
export function nodeInputConfig(node: Node<NodeData>, name: string): InputDefinition | undefined {
  return node.data.nodeSpec.input.required[name] || node.data.nodeSpec.input.optional?.[name];
}
export function inputDefault(config: InputDefinition | undefined): unknown {
  if (!config) return undefined;
  if (config[1]?.default !== undefined) return config[1].default;
  return Array.isArray(config[0]) ? config[0][0] : config[0] === 'STRING' ? '' : config[0] === 'BOOLEAN' ? false : scalar.has(config[0]) ? 0 : undefined;
}
export function nodeInputValue(node: Node<NodeData>, name: string): unknown {
  const sub = node.data.subgraph;
  const port = sub?.inputs.find(p => p.id === name);
  const boundary = port && sub?.nodes.find(n => n.id === port.nodeId);
  if (boundary && Object.hasOwn(boundary.data.values, 'value')) return boundary.data.values.value;
  return Object.hasOwn(node.data.values, name) ? node.data.values[name] : inputDefault(nodeInputConfig(node, name));
}

/** Resolve editable defaults at local boundary links; task output values remain unknown until execution. */
export function nodeInputValues(node: Pick<Node<NodeData>, 'id' | 'data'>, graph: { nodes: Node<NodeData>[]; edges: Edge[] }): Record<string, unknown> {
  const values = { ...node.data.values, ...Object.fromEntries(Object.keys({ ...node.data.nodeSpec.input.required, ...node.data.nodeSpec.input.optional }).map(name => [name, nodeInputValue(node as Node<NodeData>, name)])) };
  for (const edge of graph.edges.filter(e => e.target === node.id)) {
    const source = graph.nodes.find(n => n.id === edge.source);
    if (source?.data.boundary === 'input' && edge.targetHandle && Object.hasOwn(source.data.values, 'value')) values[edge.targetHandle] = source.data.values.value;
  }
  return values;
}
export function withNodeInputValue(node: Node<NodeData>, name: string, value: unknown): Node<NodeData> {
  const sub = node.data.subgraph;
  const port = sub?.inputs.find(p => p.id === name);
  if (!sub || !port) return { ...node, data: { ...node.data, values: { ...node.data.values, [name]: value } } };
  const values = { ...node.data.values };
  delete values[name];
  return { ...node, data: { ...node.data, values, subgraph: { ...sub,
    nodes: sub.nodes.map(n => n.id === port.nodeId ? { ...n, data: { ...n.data, values: { ...n.data.values, value } } } : n) } } };
}
export function validInputLiteral(value: unknown, config: InputDefinition): boolean {
  if (Array.isArray(config[0])) return config[0].includes(String(value)) && typeof value === 'string';
  const type = config[0];
  if (type === 'STRING') return typeof value === 'string';
  if (type === 'BOOLEAN') return typeof value === 'boolean';
  if (['FLOAT', 'INT', 'LONG'].includes(type)) return typeof value === 'number' && Number.isFinite(value)
    && (type === 'FLOAT' || Number.isSafeInteger(value))
    && (typeof config[1]?.min !== 'number' || value >= config[1].min)
    && (typeof config[1]?.max !== 'number' || value <= config[1].max);
  return false;
}
