import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import type { Node } from '@xyflow/react';
import type { NodeData } from '../../types';
import type { FlowContextType } from '../../context/FlowContextDef';

async function main() {
  const dom = new JSDOM('<!doctype html><div id="root"></div>', { url: 'http://localhost/' });
  Object.assign(globalThis, { window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement, IS_REACT_ACT_ENVIRONMENT: true });
  const { act, useState, useEffect } = await import('react');
  const { createRoot } = await import('react-dom/client');
  const { FlowContext } = await import('../../context/FlowContextDef');
  const { default: NodeInspector } = await import('./NodeInspector');
  const initial: Node<NodeData> = { id: 'process', type: 'dynamic', selected: true, position: { x: 0, y: 0 }, data: {
    opType: 'Process', values: { diameter: 12 }, nodeSpec: { type: 'Process', display_name: 'Cellpose', category: 'Segmentation', description: 'Process image blocks',
      input: { required: { diameter: ['FLOAT', { default: 10, step: 1 }] } }, output: ['IMAGE'], output_name: ['segmentation'] },
  } };
  let snapshot = { nodes: [initial], edges: [{ id: 'link', source: 'source', sourceHandle: '0', target: 'process', targetHandle: 'diameter' }] };
  function Harness({ locked = false }: { locked?: boolean }) {
    const [graph, setGraph] = useState(snapshot);
    const [open, setOpen] = useState(true);
    useEffect(() => { snapshot = graph; }, [graph]);
    const context = { nodes: graph.nodes, edges: graph.edges, subgraphPath: [], isNodeInspectorOpen: open, closeNodeInspector: () => setOpen(false), isExecutionLocked: locked,
      editGraph: (change: (graph: typeof snapshot) => typeof snapshot) => setGraph(change(graph)), updateNodeData: (id: string, change: Partial<NodeData>) => setGraph({ ...graph, nodes: graph.nodes.map(n => n.id === id ? { ...n, data: { ...n.data, ...change } } : n) }),
      addLog: (message: string) => { throw new Error(message); },
    } as unknown as FlowContextType;
    return <FlowContext.Provider value={context}><NodeInspector /></FlowContext.Provider>;
  }
  const root = createRoot(document.getElementById('root')!);
  await act(async () => root.render(<Harness />));
  const click = async (selector: string) => act(async () => document.querySelector<HTMLButtonElement>(selector)!.click());
  const diameter = () => document.querySelector<HTMLInputElement>('[aria-label="diameter"]')!;
  assert(diameter().disabled, 'a connected parameter cannot overwrite its source value');
  assert.equal(document.querySelectorAll('[role="dialog"]').length, 0, 'information is a sidebar, not a modal');
  await click('[aria-label="断开 diameter"]');
  assert(!diameter().disabled);
  assert.equal(snapshot.edges.length, 0);
  await click('[aria-label="Increase diameter"]');
  assert.equal(snapshot.nodes[0].data.values.diameter, 13, 'sidebar edits the same graph values as the node');
  await click('[aria-label="重置 diameter"]');
  assert.equal(snapshot.nodes[0].data.values.diameter, 10);
  assert.equal(document.querySelector('[aria-label="输入名称 diameter"]')!.tagName, 'SPAN', 'input field names are read-only text');
  assert.equal(document.querySelector('[aria-label="输出名称 0"]')!.tagName, 'SPAN', 'output field names are read-only text');
  assert.equal(snapshot.nodes[0].data.inputLabels, undefined, 'editing values does not create display aliases');
  await act(async () => { [...document.querySelectorAll<HTMLButtonElement>('[role="tab"]')].find(e => e.textContent === '信息')!.click(); });
  assert(document.querySelector('.inspector-info')!.textContent!.includes('Process image blocks'));
  assert.equal(document.querySelectorAll('.inspector-info pre').length, 0, 'information is human-readable instead of a JSON dump');
  await act(async () => { [...document.querySelectorAll<HTMLButtonElement>('[role="tab"]')].find(e => e.textContent === '设置')!.click(); });
  await click('[aria-label="绿色颜色"]');
  assert.equal(snapshot.nodes[0].data.color, '#6fa795');
  await click('[aria-label="默认颜色"]');
  assert.equal(snapshot.nodes[0].data.color, undefined);
  await act(async () => root.render(<Harness locked />));
  assert(document.querySelector<HTMLButtonElement>('[aria-label="绿色颜色"]')!.disabled, 'execution locks mutations in the sidebar');
  assert(document.querySelector<HTMLInputElement>('[aria-label="节点名称"]')!.disabled);
  await click('[aria-label="关闭节点面板"]');
  assert.equal(document.querySelector('.node-inspector'), null);
  await act(async () => root.unmount());

  const { convertSelection, exposePort, updateGraphAtPath } = await import('../../utils/subgraphs');
  let wrapped = convertSelection({ nodes: [initial], edges: [] }, 'Subgraph');
  const wrapperId = wrapped.nodes[0].id;
  wrapped = updateGraphAtPath(wrapped, [wrapperId], inner => exposePort(inner, 'process', 'input', 'diameter'));
  const second = createRoot(document.getElementById('root')!);
  const readonlyContext = { nodes: wrapped.nodes, edges: [], subgraphPath: [], isNodeInspectorOpen: true, isExecutionLocked: false, closeNodeInspector: () => undefined } as unknown as FlowContextType;
  await act(async () => second.render(<FlowContext.Provider value={readonlyContext}><NodeInspector /></FlowContext.Provider>));
  assert([...document.querySelectorAll('[aria-label^="输入名称"], [aria-label^="输出名称"]')].every(e => e.tagName === 'SPAN'), 'public subgraph port names are read-only');
  const boundary = wrapped.nodes[0].data.subgraph!.nodes.find(n => n.data.boundary === 'input')!;
  await act(async () => second.render(<FlowContext.Provider value={{ ...readonlyContext, nodes: [{ ...boundary, selected: true }], subgraphPath: [wrapperId] }}><NodeInspector /></FlowContext.Provider>));
  assert.equal(document.querySelector('[aria-label="接口名称"]')!.tagName, 'SPAN', 'boundary names cannot be edited inside a subgraph');
  await act(async () => { [...document.querySelectorAll<HTMLButtonElement>('[role="tab"]')].find(e => e.textContent === '设置')!.click(); });
  assert.equal(document.querySelector('[aria-label="接口名称"]')!.tagName, 'SPAN', 'settings cannot bypass the field-name restriction');
  await act(async () => second.unmount());
  dom.window.close();
  console.log('node inspector editing, connections, read-only field names, colors and execution lock tests passed');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
