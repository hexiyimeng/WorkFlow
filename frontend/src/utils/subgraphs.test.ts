import assert from 'node:assert/strict';
import type { Node } from '@xyflow/react';
import type { NodeData, NodeSpec } from '../types';
import { cloneGraphIds, convertSelection, editSubgraphPort, exposePort, flattenGraph, graphAtPath, unpackSubgraph, updateGraphAtPath, normalizeGraph, deleteSelection, type Graph } from './subgraphs.ts';
import { nodeInputValue, nodeInputValues, withNodeInputValue } from './nodeInputs.ts';
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
const isolated = convertSelection({ nodes: [{ ...node('process', 'Process'), selected: true }], edges: [] }, 'Private ports');
assert.equal(isolated.nodes[0].data.subgraph!.inputs.length, 0, 'unconnected inputs stay private by default');
assert.equal(isolated.nodes[0].data.subgraph!.outputs.length, 0, 'unconnected outputs stay private by default');
const wholePipeline = convertSelection({ ...graph, nodes: graph.nodes.map(n => ({ ...n, selected: true })) }, 'Pipeline');
assert.equal(wholePipeline.nodes[0].data.subgraph!.inputs.length, 0);
assert.equal(wholePipeline.nodes[0].data.subgraph!.outputs.length, 0, 'internal connections do not create public ports');
assert.deepEqual(compileWorkflow(wholePipeline), compileWorkflow(graph), 'private interfaces do not change internal execution');
const manualInterfaces = updateGraphAtPath(isolated, [isolated.nodes[0].id], inner => exposePort(exposePort(inner, 'process', 'input', 'image'), 'process', 'output', '0'));
assert.equal(manualInterfaces.nodes[0].data.subgraph!.inputs.length, 1);
assert.equal(manualInterfaces.nodes[0].data.subgraph!.outputs.length, 1, 'inputs and outputs can still be exposed explicitly');
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

const paramSpec: NodeSpec = { ...definitions.Process, input: { required: {
  image: ['DASK_ARRAY[float32]'], diameter: ['FLOAT', { default: 10, min: 0, max: 100 }],
  mode: [['auto', 'manual'], { default: 'auto' }], path: ['STRING', { default: '/data' }], enabled: ['BOOLEAN', { default: true }],
} } };
const paramGraph = convertSelection({ ...graph, nodes: graph.nodes.map(n => n.id === 'process' ? { ...n, selected: true, data: { ...n.data, nodeSpec: paramSpec, values: { diameter: 24, mode: 'manual', path: '/input.zarr', enabled: false } } } : n) }, 'Parameters');
const wrapperId = paramGraph.nodes.find(n => n.data.subgraph)!.id;
let promoted = paramGraph;
for (const name of ['diameter', 'mode', 'path', 'enabled']) promoted = updateGraphAtPath(promoted, [wrapperId], inner => exposePort(inner, 'process', 'input', name));
const paramWrapper = promoted.nodes.find(n => n.id === wrapperId)!;
const diameterPort = paramWrapper.data.subgraph!.inputs.find(p => p.name === 'diameter')!;
const modePort = paramWrapper.data.subgraph!.inputs.find(p => p.name === 'mode')!;
assert.deepEqual(modePort.config?.[0], ['auto', 'manual'], 'dropdown configuration remains available on the host');
assert.equal(modePort.type, 'STRING', 'dropdown slots accept a string output');
assert.equal(nodeInputValue(paramWrapper, diameterPort.id), 24, 'promotion carries the current literal');
const defaults = compileWorkflow(promoted).process.inputs;
assert.deepEqual(defaults, { diameter: 24, mode: 'manual', path: '/input.zarr', enabled: false, image: ['read', 0] }, 'promoted values compile as literals, not extra backend tasks');
const changedPromoted = { ...promoted, nodes: promoted.nodes.map(n => n.id === wrapperId ? withNodeInputValue(n, diameterPort.id, 36) : n) };
assert.equal(compileWorkflow(changedPromoted).process.inputs.diameter, 36);
assert.equal(nodeInputValue(paramWrapper, diameterPort.id), 24, 'each edit preserves the previous graph');
assert.equal(compileWorkflow(unpackSubgraph(changedPromoted, wrapperId)).process.inputs.diameter, 36, 'unpack keeps promoted defaults');
const paramDocument = serializeWorkflowDocument({ workflowId: 'parameter-test', nodes: changedPromoted.nodes, edges: changedPromoted.edges });
const paramHydrated = hydrateFlowWithLatestSpecs(parseStoredFlow(JSON.parse(JSON.stringify(paramDocument)))!, { ...definitions, Process: paramSpec });
assert.deepEqual(compileWorkflow(paramHydrated), compileWorkflow(changedPromoted), 'all parameter types survive saving and loading');
assert.equal(documentFingerprint(paramDocument), documentFingerprint(serializeWorkflowDocument({ workflowId: 'parameter-test', nodes: paramHydrated.nodes, edges: paramHydrated.edges })));
const nestedPromoted = convertSelection({ ...changedPromoted, nodes: changedPromoted.nodes.map(n => ({ ...n, selected: n.id === wrapperId })) }, 'Nested parameters');
const outerId = nestedPromoted.nodes.find(n => n.data.subgraph)!.id;
const nestedHost = updateGraphAtPath(nestedPromoted, [outerId], inner => exposePort(inner, wrapperId, 'input', diameterPort.id));
const outerPort = nestedHost.nodes.find(n => n.id === outerId)!.data.subgraph!.inputs.find(p => p.config?.[0] === 'FLOAT')!;
const nestedEdited = editSubgraphPort(nestedHost, outerId, 'input', outerPort.id, { value: 48 });
assert.equal(compileWorkflow(nestedEdited).process.inputs.diameter, 48, 'nested boundary defaults override the inner default');
assert.equal(compileWorkflow(unpackSubgraph(nestedEdited, outerId)).process.inputs.diameter, 48, 'unpacking a level preserves the nested default');
const scalarSource: Node<NodeData> = { id: 'number', type: 'dynamic', position: { x: -100, y: 0 }, data: { opType: 'Number', values: {}, nodeSpec: spec('Number', {}, ['FLOAT']) } };
const wired = { nodes: [...changedPromoted.nodes, scalarSource], edges: [...changedPromoted.edges, { id: 'number-edge', source: 'number', sourceHandle: '0', target: wrapperId, targetHandle: diameterPort.id }] };
assert.deepEqual(compileWorkflow(wired).process.inputs.diameter, ['number', 0], 'an external dependency takes precedence over a promoted literal');
assert.equal(compileWorkflow({ ...wired, edges: wired.edges.filter(e => e.id !== 'number-edge') }).process.inputs.diameter, 36, 'disconnect restores the stored default');
const wrappedWired = convertSelection({ nodes: [scalarSource, { ...node('process', 'Process'), selected: true, data: { opType: 'Process', nodeSpec: paramSpec, values: {} } }], edges: [{ id: 'scalar', source: 'number', sourceHandle: '0', target: 'process', targetHandle: 'diameter' }] }, 'Wired scalar');
assert.deepEqual(compileWorkflow(wrappedWired).process.inputs.diameter, ['number', 0], 'conversion preserves existing scalar connections');
const renamed = editSubgraphPort(changedPromoted, wrapperId, 'input', diameterPort.id, { name: '直径', move: -1 });
assert.equal(renamed.nodes.find(n => n.id === wrapperId)!.data.subgraph!.inputs.find(p => p.id === diameterPort.id)!.name, '直径');
assert.deepEqual(compileWorkflow(renamed), compileWorkflow(changedPromoted), 'renaming/reordering does not change executable parameter names');
assert.equal(editSubgraphPort(wired, wrapperId, 'input', diameterPort.id, { remove: true }).edges.some(e => e.id === 'number-edge'), false, 'removing a port removes its external connection');
assert.equal(compileWorkflow(editSubgraphPort(changedPromoted, wrapperId, 'input', diameterPort.id, { remove: true })).process.inputs.diameter, 36, 'removing a public scalar input demotes its current default into the internal widget');
const changedInner = graphAtPath(changedPromoted, [wrapperId]);
assert.equal(nodeInputValues(changedInner.nodes.find(n => n.id === 'process')!, changedInner).diameter, 36, 'inner widgets display the same default as their boundary');
const twoOutputs = updateGraphAtPath(changedPromoted, [wrapperId], inner => exposePort(inner, 'process', 'output', '0'));
const firstOutput = twoOutputs.nodes.find(n => n.id === wrapperId)!.data.subgraph!.outputs[0];
const sortedOutputs = editSubgraphPort(twoOutputs, wrapperId, 'output', firstOutput.id, { move: 1 });
assert.equal(sortedOutputs.edges.find(e => e.target === 'write')!.sourceHandle, '1', 'output reordering retargets existing parent edges to the same stable port');
assert.deepEqual(compileWorkflow(sortedOutputs), compileWorkflow(changedPromoted));
const styled = updateGraphAtPath(changedPromoted, [wrapperId], inner => ({ ...inner, nodes: inner.nodes.map(n => n.id === 'process' ? { ...n, data: { ...n.data, color: '#6fa795', inputLabels: { diameter: '直径' }, outputLabels: { '0': '分割结果' }, showAdvanced: false } } : n) }));
const styledDoc = serializeWorkflowDocument({ workflowId: 'styled', nodes: styled.nodes, edges: styled.edges });
const styledHydrated = hydrateFlowWithLatestSpecs(parseStoredFlow(JSON.parse(JSON.stringify(styledDoc)))!, { ...definitions, Process: paramSpec });
const styledChild = graphAtPath(styledHydrated, [wrapperId]).nodes.find(n => n.id === 'process')!;
assert.equal(styledChild.data.color, '#6fa795');
assert.equal(styledChild.data.inputLabels?.diameter, '直径');
assert.equal(styledChild.data.outputLabels?.['0'], '分割结果');
assert.equal(documentFingerprint(styledDoc), documentFingerprint(serializeWorkflowDocument({ workflowId: 'styled', nodes: styledHydrated.nodes, edges: styledHydrated.edges })), 'UI settings survive reload without dirtying a clean document');
const dtypeSpec: NodeSpec = { type: 'DaskTypeCast', display_name: 'Type Cast', category: 'Utility', input: { required: { image: ['DASK_ARRAY[any]'], target_dtype: [['float32', 'uint16'], { default: 'float32' }] } }, output: ['DASK_ARRAY[any]'] };
const dtypeWrapped = convertSelection({ ...graph, nodes: graph.nodes.map(n => n.id === 'process' ? { ...n, selected: true, data: { ...n.data, opType: 'DaskTypeCast', nodeSpec: dtypeSpec, values: { target_dtype: 'float32' } } } : n.id === 'write' ? { ...n, data: { ...n.data, nodeSpec: { ...n.data.nodeSpec, input: { required: { image: ['DASK_ARRAY[any]'] } } } } } : n) }, 'Type Cast');
const dtypeId = dtypeWrapped.nodes.find(n => n.data.subgraph)!.id;
const dtypePromoted = updateGraphAtPath(dtypeWrapped, [dtypeId], inner => exposePort(inner, 'process', 'input', 'target_dtype'));
const dtypePort = dtypePromoted.nodes.find(n => n.id === dtypeId)!.data.subgraph!.inputs.find(p => p.config)!;
const dtypeChanged = editSubgraphPort(dtypePromoted, dtypeId, 'input', dtypePort.id, { value: 'uint16' });
assert.equal(dtypeChanged.nodes.find(n => n.id === dtypeId)!.data.nodeSpec.output[0], 'DASK_ARRAY[uint16]', 'a promoted type parameter updates the host output type');
assert.equal(compileWorkflow(dtypeChanged).process.inputs.target_dtype, 'uint16');
assert.throws(() => compileWorkflow(editSubgraphPort(changedPromoted, wrapperId, 'input', modePort.id, { value: 'missing-choice' })), /参数/);
assert.throws(() => compileWorkflow(editSubgraphPort(changedPromoted, wrapperId, 'input', diameterPort.id, { value: -1 })), /参数/);
assert.throws(() => parseStoredFlow({ nodes: [{ id: 'bad', data: { inputLabels: { value: 3 } } }], edges: [] }), /display label/);
assert.throws(() => parseStoredFlow({ nodes: [{ id: 'bad', data: { boundaryConfig: [3] } }], edges: [] }), /parameter definition/);
console.log('scalar slots, promoted defaults, enum validation, nested values, interface editing and persistence tests passed');

const inlineDtype = normalizeGraph({ ...dtypePromoted, nodes: dtypePromoted.nodes.map(n => n.id === dtypeId ? withNodeInputValue(n, dtypePort.id, 'uint16') : n) }, dtypePromoted);
assert.equal(inlineDtype.nodes.find(n => n.id === dtypeId)!.data.nodeSpec.output[0], 'DASK_ARRAY[uint16]', 'inline edits also refresh interface types');
const dtypeNested = convertSelection({ ...dtypePromoted, nodes: dtypePromoted.nodes.map(n => ({ ...n, selected: n.id === dtypeId })) }, 'Outer dtype');
const dtypeOuter = dtypeNested.nodes.find(n => n.data.subgraph)!;
const dtypeNestedExposed = updateGraphAtPath(dtypeNested, [dtypeOuter.id], g => exposePort(g, dtypeId, 'input', dtypePort.id));
const dtypeOuterPort = dtypeNestedExposed.nodes.find(n => n.id === dtypeOuter.id)!.data.subgraph!.inputs.find(p => p.config)!;
const dtypeNestedChanged = editSubgraphPort(dtypeNestedExposed, dtypeOuter.id, 'input', dtypeOuterPort.id, { value: 'uint16' });
assert.equal(dtypeNestedChanged.nodes.find(n => n.id === dtypeOuter.id)!.data.nodeSpec.output[0], 'DASK_ARRAY[uint16]', 'types follow defaults through two boundary levels');
assert.equal(compileWorkflow(dtypeNestedChanged).process.inputs.target_dtype, 'uint16');
assert.equal(nodeInputValue(graphAtPath(dtypeNestedChanged, [dtypeOuter.id]).nodes.find(n => n.id === dtypeId)!, dtypePort.id), 'float32', 'contextual inference must not overwrite the nested fallback');
const staleDocument = serializeFlowForStorage(dtypeNestedChanged.nodes, dtypeNestedChanged.edges);
staleDocument.nodes.find(n => n.id === dtypeOuter.id)!.data.subgraph!.outputs[0].type = 'DASK_ARRAY[float32]';
const freshTypes = hydrateFlowWithLatestSpecs(staleDocument, { ...definitions, DaskTypeCast: dtypeSpec });
assert.equal(freshTypes.nodes.find(n => n.id === dtypeOuter.id)!.data.nodeSpec.output[0], 'DASK_ARRAY[uint16]', 'loading recomputes outdated interface type metadata');

const conditionalSpec: NodeSpec = { ...paramSpec, input: { required: { mode: [['explicit', 'match'], { default: 'explicit' }] },
  optional: { reference: ['DASK_ARRAY[float32]', { visible_when: { mode: 'match' } }] } } };
const conditional: Graph = { nodes: [node('read', 'Reader'), { ...node('process', 'Process'), data: { opType: 'Process', nodeSpec: conditionalSpec, values: { mode: 'match' } } }],
  edges: [{ id: 'reference-edge', source: 'read', target: 'process', sourceHandle: '0', targetHandle: 'reference' }] };
const hidden = normalizeGraph({ ...conditional, nodes: conditional.nodes.map(n => n.id === 'process' ? withNodeInputValue(n, 'mode', 'explicit') : n) }, conditional);
assert.equal(hidden.edges.length, 0, 'inspector and inline edits remove a link when its controlling parameter hides the input');
const conditionalWrapped = convertSelection({ ...conditional, nodes: conditional.nodes.map(n => ({ ...n, selected: n.id === 'process' })) }, 'Conditional');
const conditionalId = conditionalWrapped.nodes.find(n => n.data.subgraph)!.id;
const conditionalExposed = updateGraphAtPath(conditionalWrapped, [conditionalId], g => exposePort(g, 'process', 'input', 'mode'));
// Simulate a different fallback, while the exposed mode remains match.
const differentFallback = updateGraphAtPath(conditionalExposed, [conditionalId], g => ({ ...g, nodes: g.nodes.map(n => n.id === 'process' ? withNodeInputValue(n, 'mode', 'explicit') : n) }));
const renamedConditional = normalizeGraph(updateGraphAtPath(differentFallback, [conditionalId], g => ({ ...g, nodes: g.nodes.map(n => n.id === 'process' ? { ...n, data: { ...n.data, title: 'Renamed', showAdvanced: true } } : n) })), differentFallback);
assert.deepEqual(compileWorkflow(renamedConditional), compileWorkflow(differentFallback), 'metadata edits preserve links controlled by a promoted default');
console.log('conditional dependency edits and nested interface type regressions passed');

const wiredBeforeDelete = JSON.stringify(wired);
const deletedParameter = updateGraphAtPath(wired, [wrapperId], inner => deleteSelection({ ...inner,
  nodes: inner.nodes.map(n => ({ ...n, selected: n.id === diameterPort.nodeId })) }));
assert(!graphAtPath(deletedParameter, [wrapperId]).nodes.some(n => n.id === diameterPort.nodeId));
assert(!deletedParameter.nodes.find(n => n.id === wrapperId)!.data.subgraph!.inputs.some(p => p.id === diameterPort.id), 'deleting an interface removes the public port');
assert(!deletedParameter.edges.some(e => e.id === 'number-edge'), 'deleting an interface removes its external connection');
assert.equal(compileWorkflow(deletedParameter).process.inputs.diameter, 36, 'deleting an input retains the edited default on its consumer');
assert.equal(JSON.stringify(wired), wiredBeforeDelete, 'selection deletion preserves the previous graph for undo');
const outputBoundary = paramWrapper.data.subgraph!.outputs[0].nodeId;
const deletedOutput = updateGraphAtPath(wired, [wrapperId], inner => deleteSelection({ ...inner,
  nodes: inner.nodes.map(n => ({ ...n, selected: n.id === outputBoundary })) }));
assert(!deletedOutput.edges.some(e => e.source === wrapperId), 'output deletion removes parent connections');
const mixedDeleted = updateGraphAtPath(wired, [wrapperId], inner => deleteSelection({ ...inner,
  nodes: inner.nodes.map(n => ({ ...n, selected: n.id === diameterPort.nodeId || n.id === 'process' })) }));
const mixedInner = graphAtPath(mixedDeleted, [wrapperId]);
assert(!mixedInner.nodes.some(n => n.id === 'process' || n.id === diameterPort.nodeId));
assert(mixedInner.edges.every(e => mixedInner.nodes.some(n => n.id === e.source) && mixedInner.nodes.some(n => n.id === e.target)), 'mixed node/interface deletion leaves no dangling internal links');
console.log('selection deletion, interface defaults and parent connection cleanup tests passed');
