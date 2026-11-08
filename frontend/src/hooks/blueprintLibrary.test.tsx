import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import type { NodeSpec } from '../types';
import type { Graph } from '../utils/subgraphs';
import type { LibraryRecord } from '../utils/libraryApi';

async function main() {
  const dom = new JSDOM('<!doctype html><div id="root"></div>', { url: 'http://blueprint-test.local/' });
  Object.assign(globalThis, { window: dom.window, document: dom.window.document, localStorage: dom.window.localStorage,
    HTMLElement: dom.window.HTMLElement, IS_REACT_ACT_ENVIRONMENT: true, confirm: () => false });
  const originalFetch = globalThis.fetch;
  const records = new Map<string, LibraryRecord>();
  const writes: string[] = [];
  let delayNextRead: Promise<void> | undefined;
  globalThis.fetch = async (input, init) => {
    const path = String(input).split('/library/')[1];
    const [kind, id] = path.split('/');
    const method = init?.method || 'GET';
    if (method === 'GET' && id && delayNextRead) {
      const delay = delayNextRead; delayNextRead = undefined; await delay;
    }
    if (method === 'GET') return Response.json(id ? records.get(path) : { items: [...records].filter(([key]) => key.startsWith(`${kind}/`)).map(([, value]) => value), warnings: [] });
    const payload = JSON.parse(String(init?.body));
    const current = id ? records.get(path) : undefined;
    if (method === 'PUT' && current?.revision !== payload.revision) return Response.json({ detail: 'This workflow changed elsewhere. Reload it or save a copy.' }, { status: 409 });
    const record: LibraryRecord = { ...payload, id: id || crypto.randomUUID(), revision: (current?.revision || 0) + 1, updatedAt: Date.now() };
    records.set(`${kind}/${record.id}`, record); writes.push(`${method} ${kind}`);
    return Response.json(record);
  };
  const { act, useEffect, useState } = await import('react');
  const { createRoot } = await import('react-dom/client');
  const { useWorkflows } = await import('./useWorkflows');
  const { useAutoSave } = await import('./useAutoSave');
  const { useSubgraphEditor } = await import('./useSubgraphEditor');
  const { useWorkflowLibrary } = await import('./useWorkflowLibrary');
  const { convertSelection, exposePort, flattenGraph } = await import('../utils/subgraphs');
  const { readLibraryReference } = await import('../utils/workflowDrafts');
  const { FlowContext } = await import('../context/FlowContextDef');
  const { default: SubgraphLibraryGroup } = await import('../components/layout/SubgraphLibraryGroup');
  const spec: NodeSpec = { type: 'Process', display_name: 'Process', category: 'Test',
    input: { required: { value: ['FLOAT', { default: 10 }] } }, output: ['FLOAT'] };
  const nodeDefs = { Process: spec };
  const initial: Graph = convertSelection({ nodes: [{ id: 'process', type: 'dynamic', selected: true,
    position: { x: 0, y: 0 }, data: { opType: 'Process', nodeSpec: spec, values: { value: 10 } } }], edges: [] }, 'Process blueprint');
  let control: { graph: Graph; editor: ReturnType<typeof useSubgraphEditor>; workflows: ReturnType<typeof useWorkflows>; library: ReturnType<typeof useWorkflowLibrary> };
  function Harness() {
    const [graph, setGraph] = useState(initial);
    const editor = useSubgraphEditor(graph, setGraph);
    const [setters] = useState(() => ({
      nodes: (action: import('react').SetStateAction<Graph['nodes']>) => setGraph(previous => ({ ...previous, nodes: typeof action === 'function' ? action(previous.nodes) : action })),
      edges: (action: import('react').SetStateAction<Graph['edges']>) => setGraph(previous => ({ ...previous, edges: typeof action === 'function' ? action(previous.edges) : action })),
    }));
    const workflows = useWorkflows(graph.nodes, graph.edges, setters.nodes, setters.edges, nodeDefs, () => undefined);
    useAutoSave(graph.nodes, graph.edges, setters.nodes, setters.edges, nodeDefs, workflows.activeWorkflow, workflows.loadWorkflowDocument);
    const library = useWorkflowLibrary({ nodes: graph.nodes, edges: graph.edges, activeWorkflow: workflows.activeWorkflow,
      workflows: workflows.workflows, nodeDefs, locked: false, subgraphPath: editor.subgraphPath, updateWorkflow: workflows.updateWorkflow,
      openDocument: (doc, nodes, edges, reference) => {
        workflows.openDocument(doc, nodes, edges, reference);
        editor.openSubgraphPath(reference?.kind === 'subgraphs' ? [nodes[0].id] : []);
      }, switchWorkflow: id => { editor.resetSubgraph(); workflows.switchWorkflow(id); }, editGraph: editor.editGraph, addLog: () => undefined });
    useEffect(() => { control = { graph, editor, workflows, library }; });
    return <FlowContext.Provider value={{ ...library, isExecutionLocked: false } as unknown as import('../context/FlowContextDef').FlowContextType}>
      <SubgraphLibraryGroup search="" collapsed={false} toggle={() => undefined} />
    </FlowContext.Provider>;
  }
  const root = createRoot(document.getElementById('root')!);
  try {
    await act(async () => { root.render(<Harness />); });
    const originalTab = control!.workflows.activeWorkflowId;
    await act(async () => { control!.library.saveSubgraphToLibrary(control!.graph.nodes[0]); });
    await act(async () => { assert(await control!.library.saveLibraryDocument(control!.library.librarySaveDialog!, 'Process blueprint', 'Tests')); });
    assert.equal(Boolean(control!.workflows.activeWorkflow!.libraryRef), false, 'publishing does not bind the parent workflow to the blueprint');
    const id = control!.library.libraryItems.subgraphs[0].id;
    const key = `subgraphs/${id}`;
    const docId = records.get(key)!.document.workflowId;
    await act(async () => { document.querySelector<HTMLButtonElement>('[aria-label="Edit blueprint Process blueprint"]')!.click(); });
    assert.equal(control!.workflows.activeWorkflow!.libraryRef?.kind, 'subgraphs');
    assert.equal(control!.editor.subgraphPath.length, 1, 'editing opens the inner graph');
    assert.equal(control!.editor.nodes[0].id, 'process');
    const blueprintTab = control!.workflows.activeWorkflowId;
    const tabCount = control!.workflows.workflows.length;
    await act(async () => { await control!.library.editSubgraphBlueprint(id); });
    assert.equal(control!.workflows.workflows.length, tabCount, 'reopening activates the same editor');
    await act(async () => { control!.editor.openSubgraphPath([control!.graph.nodes[0].id]); });
    await act(async () => { control!.editor.editGraph(inner => exposePort(exposePort({ ...inner,
      nodes: inner.nodes.map(n => ({ ...n, data: { ...n.data, values: { value: 42 } } })) }, 'process', 'input', 'value'), 'process', 'output', '0')); });
    assert(control!.library.workflowDirty);
    await act(async () => { control!.library.requestLibrarySave(); });
    assert.equal(records.get(key)!.revision, 2);
    assert.equal(records.get(key)!.document.workflowId, docId, 'overwrite retains document identity');
    assert.equal(control!.library.libraryItems.subgraphs.length, 1, 'save overwrites rather than publishing a second blueprint');
    assert.equal(control!.library.workflowDirty, false);
    const savedSubgraph = records.get(key)!.document.nodes[0].data.subgraph!;
    assert.equal(savedSubgraph.inputs.length, 1); assert.equal(savedSubgraph.outputs.length, 1);
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 1700)); });
    const draft = JSON.parse(localStorage.getItem('WorkFlow_AUTOSAVE')!);
    assert.equal(readLibraryReference(draft._libraryRef)?.kind, 'subgraphs', 'autosave retains the save destination');
    assert.equal(readLibraryReference(draft._libraryRef)?.revision, 2);
    await act(async () => { control!.editor.resetSubgraph(); control!.workflows.switchWorkflow(originalTab); });
    assert.equal(flattenGraph(control!.graph).nodes[0].data.values.value, 10, 'updating a blueprint does not change existing instances');
    await act(async () => { await control!.library.openLibraryItem('subgraphs', id); });
    assert.equal(control!.workflows.activeWorkflowId, originalTab, 'inserting remains a node operation');
    let releaseRead!: () => void;
    delayNextRead = new Promise<void>(resolve => { releaseRead = resolve; });
    let pendingInsert!: Promise<void>;
    await act(async () => { pendingInsert = control!.library.openLibraryItem('subgraphs', id); });
    await act(async () => { control!.editor.editGraph(g => ({ ...g, nodes: g.nodes.map(n => n.id === initial.nodes[0].id
      ? { ...n, data: { ...n.data, title: 'Edited while loading' } } : n) })); });
    await act(async () => { releaseRead(); await pendingInsert; });
    assert.equal(control!.graph.nodes.find(n => n.id === initial.nodes[0].id)!.data.title, 'Edited while loading', 'a delayed library response must preserve edits made while it loaded');
    const inserted = control!.graph.nodes.find(n => n.id !== initial.nodes[0].id)!;
    assert.equal(inserted.data.subgraph!.inputs.length, 1);
    assert.equal(inserted.data.subgraph!.outputs.length, 1);
    assert(flattenGraph({ nodes: [inserted], edges: [] }).nodes.every(n => n.id !== 'process'), 'inserted instances have fresh IDs');
    delayNextRead = new Promise<void>(resolve => { releaseRead = resolve; });
    await act(async () => { pendingInsert = control!.library.openLibraryItem('subgraphs', id); });
    await act(async () => { control!.editor.enterSubgraph(initial.nodes[0].id); });
    await act(async () => { releaseRead(); await pendingInsert; });
    assert.match(control!.library.libraryError!, /画布或执行状态已改变/);
    assert.equal(control!.editor.nodes.length, 1, 'a delayed insertion cannot land in a different subgraph');
    await act(async () => { control!.editor.resetSubgraph(); });
    delayNextRead = new Promise<void>(resolve => { releaseRead = resolve; });
    await act(async () => { pendingInsert = control!.library.openLibraryItem('subgraphs', id); });
    await act(async () => { control!.workflows.switchWorkflow(blueprintTab); });
    const beforeResponse = JSON.stringify(control!.graph);
    await act(async () => { releaseRead(); await pendingInsert; });
    assert.equal(JSON.stringify(control!.graph), beforeResponse, 'a delayed response cannot overwrite another tab');
    // Another editor changes the definition. Never silently overwrite its work.
    records.get(key)!.revision = 3;
    await act(async () => { control!.library.requestLibrarySave(); });
    assert.match(control!.library.libraryError!, /changed elsewhere/);
    assert.equal(control!.workflows.activeWorkflow!.libraryRef?.revision, 2);
    await act(async () => { control!.library.requestLibrarySave(true); });
    assert.equal(control!.library.librarySaveDialog!.kind, 'subgraphs');
    await act(async () => { assert(await control!.library.saveLibraryDocument(control!.library.librarySaveDialog!, 'Process copy', 'Tests')); });
    assert.equal(control!.library.libraryItems.subgraphs.length, 2);
    assert.equal(control!.workflows.activeWorkflow!.libraryRef?.kind, 'subgraphs');
    assert.notEqual(control!.workflows.activeWorkflow!.libraryRef?.id, id);
    assert(writes.every(write => write.endsWith('subgraphs')), 'blueprint edits and Save as never write into the workflow library');
  } finally {
    await act(async () => { root.unmount(); });
    globalThis.fetch = originalFetch;
    dom.window.close();
  }
  console.log('blueprint edit, overwrite, interfaces, independent instances, autosave, conflicts and Save as tests passed');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
