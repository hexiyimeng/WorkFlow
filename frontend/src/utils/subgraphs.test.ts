import assert from 'node:assert/strict';
import type { Node } from '@xyflow/react';
import type { NodeData, NodeSpec } from '../types';
import { cloneGraphIds, convertSelection, exposePort, flattenGraph, graphAtPath, unpackSubgraph, updateGraphAtPath, type Graph } from './subgraphs.ts';
import { compileWorkflow } from './compileWorkflow.ts';
import { documentFingerprint } from './libraryApi.ts';
import { hydrateFlowWithLatestSpecs, parseStoredFlow, serializeFlowForStorage, serializeWorkflowDocument } from './workflowPersistence.ts';

const spec = (type: string, inputs: Record<string, [string]> = {}, outputs = ['DASK_ARRAY[float32]']): NodeSpec => ({ type, display_name: type, category: 'Test', input: { required: inputs }, output: outputs });
const definitions = { Reader: spec('Reader'), Process: spec('Process', { image: ['DASK_ARRAY[float32]'] }), Writer: { ...spec('Writer', { image: ['DASK_ARRAY[float32]'] }, []), output_node: true } };
const node = (id: string, type: keyof typeof definitions): Node<NodeData> => ({ id, type: 'dynamic', position: { x: 100, y: 100 }, data: { opType: type, nodeSpec: definitions[type], values: {} } });
const graph: Graph = { nodes: [node('read', 'Reader'), node('process', 'Process'), node('write', 'Writer')], edges: [
  { id: 'first', source: 'read', sourceHandle: '0', target: 'process', targetHandle: 'image' },
  { id: 'second', source: 'process', sourceHandle: '0', target: 'write', targetHandle: 'image' },
] };
const semantic = (g: Graph) => ({ ids: g.nodes.map(n => n.id).sort(), edges: g.edges.map(e => [e.source, e.sourceHandle, e.target, e.targetHandle].join('/')).sort() });
const original = JSON.stringify(graph);
const converted = convertSelection({ ...graph, nodes: graph.nodes.map(n => ({ ...n, selected: n.id === 'process' })) }, 'One node');
const wrapper = converted.nodes.find(n => n.data.subgraph)!;
assert(wrapper && wrapper.data.subgraph!.nodes.filter(n => !n.data.boundary).length === 1);
assert.deepEqual(semantic(flattenGraph(converted)), semantic(graph), 'single-node wrapping preserves dependencies');
assert.equal(JSON.stringify(graph), original, 'conversion/compilation do not mutate source document');
assert.deepEqual(semantic(flattenGraph(unpackSubgraph(converted, wrapper.id))), semantic(graph));

const nested = convertSelection({ ...converted, nodes: converted.nodes.map(n => ({ ...n, selected: n.id !== 'read' })) }, 'Nested with writer');
assert.deepEqual(semantic(flattenGraph(nested)), semantic(graph), 'nested wrappers preserve terminal writer identity');
const document = serializeWorkflowDocument({ workflowId: 'wf', nodes: nested.nodes, edges: nested.edges });
const raw = parseStoredFlow(JSON.parse(JSON.stringify(document)))!;
const hydrated = hydrateFlowWithLatestSpecs(raw, definitions);
assert.equal(documentFingerprint(document), documentFingerprint(serializeWorkflowDocument({ workflowId: 'wf', nodes: hydrated.nodes, edges: hydrated.edges })), 'opening an unchanged workflow must not mark it dirty');
assert.deepEqual(semantic(flattenGraph(hydrated)), semantic(graph), 'JSON round-trip rehydrates virtual ports and algorithm specs');
assert(!JSON.stringify(document).includes('nodeSpec'), 'runtime specs never enter saved subgraphs');
const clone = cloneGraphIds(hydrated);
assert.equal(flattenGraph({ nodes: [...hydrated.nodes, ...clone.nodes], edges: [...hydrated.edges, ...clone.edges] }).nodes.length, 6, 'copies get fresh internal and interface IDs');

const childGraph = graphAtPath(converted, [wrapper.id]);
const changed = updateGraphAtPath(converted, [wrapper.id], inner => ({ ...inner, nodes: inner.nodes.map(n => n.id === 'process' ? { ...n, data: { ...n.data, values: { diameter: 15 } } } : n) }));
assert.equal(flattenGraph(changed).nodes.find(n => n.id === 'process')?.data.values.diameter, 15);
assert.equal(childGraph.nodes.find(n => n.id === 'process')?.data.values.diameter, undefined, 'editing is immutable');
assert.equal(flattenGraph(changed).nodes.find(n => n.id === 'write')?.id, 'write');

const rechunk = node('process', 'Process');
rechunk.data.nodeSpec = { ...rechunk.data.nodeSpec, bypassMapping: { '0': 'image' } };
rechunk.data.bypassed = true;
assert.equal(flattenGraph({ ...graph, nodes: graph.nodes.map(n => n.id === 'process' ? rechunk : n) }).edges.find(e => e.target === 'write')?.source, 'read');
assert.throws(() => flattenGraph({ ...graph, nodes: graph.nodes.map(n => n.id === 'process' ? { ...n, data: { ...n.data, bypassed: true } } : n) }), /cannot be bypassed/);
assert.throws(() => flattenGraph({ ...graph, nodes: [...graph.nodes, graph.nodes[0]] }), /Duplicate/);

const missingDefs = hydrateFlowWithLatestSpecs(serializeFlowForStorage(graph.nodes, graph.edges), { Reader: definitions.Reader, Writer: definitions.Writer });
assert.equal(missingDefs.edges.length, 2, 'opening on another server preserves edges for missing plugins');
assert.deepEqual(compileWorkflow(nested), compileWorkflow(graph), 'preflight and execute receive the same ordinary graph as before wrapping');
const removedPort = updateGraphAtPath(converted, [wrapper.id], inner => {
  const id = wrapper.data.subgraph!.outputs[0].nodeId;
  return { nodes: inner.nodes.filter(n => n.id !== id), edges: inner.edges.filter(e => e.source !== id && e.target !== id) };
});
assert(!removedPort.edges.some(e => e.source === wrapper.id), 'removing a boundary removes its outer connections atomically');
const exposed = updateGraphAtPath(removedPort, [wrapper.id], inner => exposePort(inner, 'process', 'output', '0'));
assert.equal(exposed.nodes.find(n => n.id === wrapper.id)?.data.subgraph?.outputs.length, 1, 'internal outputs can be exposed later');
const passThrough = updateGraphAtPath(converted, [wrapper.id], inner => {
  const sub = wrapper.data.subgraph!;
  return { nodes: inner.nodes.filter(n => n.data.boundary), edges: [{ id: 'straight', source: sub.inputs[0].nodeId, sourceHandle: '0', target: sub.outputs[0].nodeId, targetHandle: 'value' }] };
});
assert.deepEqual(semantic(flattenGraph(unpackSubgraph(passThrough, wrapper.id))), semantic(flattenGraph(passThrough)), 'unpack supports a directly connected input/output interface');
assert.throws(() => parseStoredFlow({ nodes: [{ id: 'bad', data: { subgraph: { nodes: [], edges: [] } } }], edges: [] }), /Invalid subgraph interface/);
assert.throws(() => parseStoredFlow({ schemaVersion: 99, nodes: [], edges: [] }), /Unsupported/);
const timestampParameter = serializeWorkflowDocument({ workflowId: 'wf', nodes: [{ ...graph.nodes[0], data: { ...graph.nodes[0].data, values: { timestamp: 1, selected: 'first' } } }], edges: [] });
const changedParameter = JSON.parse(JSON.stringify(timestampParameter));
changedParameter.nodes[0].data.values.timestamp = 2;
assert.notEqual(documentFingerprint(timestampParameter), documentFingerprint(changedParameter), 'algorithm parameters named timestamp/selected are real changes');
assert.throws(() => flattenGraph({ nodes: [node('a', 'Process'), node('b', 'Process')], edges: [
  { id: 'ab', source: 'a', sourceHandle: '0', target: 'b', targetHandle: 'image' },
  { id: 'ba', source: 'b', sourceHandle: '0', target: 'a', targetHandle: 'image' },
] }), /cycle/);
console.log('subgraph conversion, nesting, persistence, copying, bypass and anchor tests passed');
