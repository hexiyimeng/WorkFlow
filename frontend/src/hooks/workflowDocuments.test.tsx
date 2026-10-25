import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import type { NodeData, NodeSpec } from '../types';
import type { Graph } from '../utils/subgraphs';

async function main() {
  const dom = new JSDOM('<!doctype html><div id="root"></div>', { url: 'http://workflow-test.local/' });
  Object.assign(globalThis, { window: dom.window, document: dom.window.document, localStorage: dom.window.localStorage,
    HTMLElement: dom.window.HTMLElement, IS_REACT_ACT_ENVIRONMENT: true });
  const { act, useEffect, useState } = await import('react');
  const { createRoot } = await import('react-dom/client');
  const { useWorkflows } = await import('./useWorkflows');
  const { useAutoSave } = await import('./useAutoSave');
  const { useSubgraphEditor } = await import('./useSubgraphEditor');
  const { convertSelection, flattenGraph } = await import('../utils/subgraphs');
  const spec = (type: string, input = false): NodeSpec => ({ type, display_name: type, category: 'Test',
    input: { required: input ? { image: ['DASK_ARRAY[float32]'] } : {} }, output: ['DASK_ARRAY[float32]'] });
  const nodeDefs = { Reader: spec('Reader'), Process: spec('Process', true) };
  const initial = { workflowId: 'draft-test', workflow_name: 'Draft test', metadata: {}, nodes: [
    { id: 'reader', type: 'dynamic', position: { x: 0, y: 0 }, data: { opType: 'Reader', values: {} } },
    { id: 'process', type: 'dynamic', position: { x: 400, y: 0 }, data: { opType: 'Process', values: { diameter: 10 } } },
  ], edges: [{ id: 'edge', source: 'reader', sourceHandle: '0', target: 'process', targetHandle: 'image' }] };
  localStorage.setItem('WorkFlow_AUTOSAVE', JSON.stringify(initial));
  let control: { root: Graph; editor: ReturnType<typeof useSubgraphEditor>; workflows: ReturnType<typeof useWorkflows> };
  function Harness() {
    const [root, setRoot] = useState<Graph>({ nodes: [], edges: [] });
    const editor = useSubgraphEditor(root, setRoot);
    const [setters] = useState(() => ({
      nodes: (action: import('react').SetStateAction<Graph['nodes']>) => setRoot(previous => ({ ...previous, nodes: typeof action === 'function' ? action(previous.nodes) : action })),
      edges: (action: import('react').SetStateAction<Graph['edges']>) => setRoot(previous => ({ ...previous, edges: typeof action === 'function' ? action(previous.edges) : action })),
    }));
    const workflows = useWorkflows(root.nodes, root.edges, setters.nodes, setters.edges, nodeDefs, () => undefined);
    useAutoSave(root.nodes, root.edges, setters.nodes, setters.edges, nodeDefs, workflows.activeWorkflow, workflows.loadWorkflowDocument);
    useEffect(() => { control = { root, editor, workflows }; });
    return <div>{editor.nodes.map(node => <span key={node.id}>{node.data.opType}</span>)}</div>;
  }
  const root = createRoot(document.getElementById('root')!);
  await act(async () => { root.render(<Harness />); });
  assert.equal(control!.root.nodes.length, 2, 'the initial draft is hydrated once');
  await act(async () => { control!.editor.editGraph(graph => convertSelection({ ...graph,
    nodes: graph.nodes.map(node => ({ ...node, selected: node.id === 'process' })) }, 'Single algorithm')); });
  const wrapperId = control!.root.nodes.find(n => n.data.subgraph)!.id;
  await act(async () => { control!.editor.enterSubgraph(wrapperId); });
  assert(control!.editor.nodes.some(n => n.data.boundary), 'entering opens the internal graph');
  await act(async () => { control!.editor.setNodes(nodes => nodes.map(node => node.id === 'process'
    ? { ...node, data: { ...node.data, values: { diameter: 42 } } as NodeData } : node)); });
  // The former 500 ms tab sync loop reset the 1000 ms autosave indefinitely.
  await act(async () => { await new Promise(resolve => setTimeout(resolve, 1800)); });
  const saved = JSON.parse(localStorage.getItem('WorkFlow_AUTOSAVE')!);
  assert(saved.nodes.some((node: { id: string }) => node.id === 'reader'), 'saving inside a subgraph keeps the complete parent document');
  assert.equal(saved.nodes.find((node: { id: string }) => node.id === wrapperId).data.subgraph.nodes.find((node: { id: string }) => node.id === 'process').data.values.diameter, 42);
  assert.equal(flattenGraph(control!.root).nodes.find(n => n.id === 'process')?.data.values.diameter, 42);
  const firstTab = control!.workflows.activeWorkflowId;
  await act(async () => { control!.workflows.createWorkflow(); });
  assert.equal(control!.root.nodes.length, 0);
  await act(async () => { control!.workflows.switchWorkflow(firstTab); });
  assert.equal(flattenGraph(control!.root).nodes.find(n => n.id === 'process')?.data.values.diameter, 42, 'switching tabs retains nested edits');
  await act(async () => { root.unmount(); });
  dom.window.close();
  console.log('workflow document navigation, tab persistence and autosave regression tests passed');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
