import { JSDOM } from 'jsdom';

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

async function main() {
  const dom = new JSDOM('<!doctype html><div id="root"></div>', { url: 'http://localhost/' });
  Object.assign(globalThis, { window: dom.window, document: dom.window.document,
    HTMLElement: dom.window.HTMLElement, IS_REACT_ACT_ENVIRONMENT: true });
  let clipboard = '';
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: { clipboard: {
    writeText: async (value: string) => { clipboard = value; },
  }, userAgent: dom.window.navigator.userAgent } });
  const { act, useState } = await import('react');
  const { createRoot } = await import('react-dom/client');
  const { FlowContext } = await import('../../context/FlowContextDef');
  const { default: Sidebar } = await import('./Sidebar');
  const { default: Output } = await import('./BottomPanel');
  const root = createRoot(document.getElementById('root')!);
  const added: string[] = [];
  const context = {
    nodeDefs: {
      Reader: { type: 'Reader', display_name: 'Image Reader', category: 'Plugins/IO', description: 'Read an image', input: { required: {} }, output: [] },
      CustomSeg: { type: 'CustomSeg', display_name: 'Segmentation', category: 'Plugins/Microscopy', description: 'GPU segmentation', required_worker_profile: 'GPU', input: { required: {} }, output: [] },
    },
    addNode: (type: string) => added.push(type), isExecutionLocked: false,
    isConsoleOpen: false, toggleConsole: () => undefined,
  } as unknown as import('../../context/FlowContextDef').FlowContextType;
  await act(async () => { root.render(<FlowContext.Provider value={context}><Sidebar /></FlowContext.Provider>); });
  assert(document.querySelectorAll('.library-node').length === 2, 'node categories start expanded');
  const setInput = async (selector: string, value: string) => {
    await act(async () => {
      const input = document.querySelector<HTMLInputElement>(selector)!;
      Object.getOwnPropertyDescriptor(dom.window.HTMLInputElement.prototype, 'value')!.set!.call(input, value);
      input.dispatchEvent(new dom.window.Event('input', { bubbles: true }));
    });
  };
  const click = async (selector: string) => { await act(async () => { document.querySelector<HTMLButtonElement>(selector)!.click(); }); };
  await setInput('[aria-label="Search nodes"]', 'plugins gpu');
  assert(document.querySelectorAll('.library-node').length === 1, 'search matches both category and description');
  await click('[aria-label="Add Segmentation"]');
  assert(added[0] === 'CustomSeg', 'adding uses the original plugin type');
  await setInput('[aria-label="Search nodes"]', 'unavailable');
  assert(document.body.textContent?.includes('No matching nodes'), 'empty searches have a useful message');
  await click('[aria-label="Clear node search"]');
  await act(async () => { root.render(<FlowContext.Provider value={{ ...context, isExecutionLocked: true }}><Sidebar /></FlowContext.Provider>); });
  const item = document.querySelector<HTMLButtonElement>('[aria-label="Add Image Reader"]')!;
  assert(item.disabled && !item.draggable, 'execution lock prevents both adding and dragging');
  await act(async () => { root.unmount(); });

  const panelRoot = createRoot(document.getElementById('root')!);
  function OutputHarness() {
    const [open, setOpen] = useState(false);
    const [logs, setLogs] = useState<import('../../types').LogEntry[]>([
      { id: 'a', timestamp: 1, type: 'info', message: 'Read metadata' },
      { id: 'b', timestamp: 2, type: 'warning', message: 'Job pending' },
      { id: 'c', timestamp: 3, type: 'error', message: 'Invalid image block', details: 'Traceback: Cellpose on worker-2' },
    ]);
    return <FlowContext.Provider value={{ logs, clearLogs: () => setLogs([]), isConsoleOpen: open,
      toggleConsole: () => setOpen(current => !current), websocketStatus: 'connected',
      executionState: { phase: 'failed', executionId: 'test', lastError: 'Invalid image block', windowProgress: null },
      isExecuting: false, isCancelling: false, stopFlow: () => undefined,
    } as unknown as import('../../context/FlowContextDef').FlowContextType}><Output /></FlowContext.Provider>;
  }
  await act(async () => { panelRoot.render(<OutputHarness />); });
  assert(document.querySelector('.output-heading')?.getAttribute('aria-expanded') === 'true', 'new errors automatically reveal output');
  await act(async () => { document.querySelectorAll<HTMLButtonElement>('[role="tab"]')[1].click(); });
  assert(document.querySelectorAll('.output-log').length === 1, 'error filter hides unrelated events');
  await setInput('[aria-label="Search output"]', 'worker-2');
  assert(document.querySelectorAll('.output-log').length === 1, 'search also covers collapsed diagnostics');
  assert(!document.querySelector('details')!.open, 'long diagnostics remain collapsed');
  await click('[aria-label="Copy visible output"]');
  assert(clipboard.includes('worker-2') && !clipboard.includes('Job pending'), 'copy includes full details of only visible events');
  await click('[aria-label="Follow output"]');
  assert(document.querySelector('[aria-label="Follow output"]')!.getAttribute('aria-pressed') === 'false', 'auto-scroll can be paused');
  await click('[aria-label="Clear logs"]');
  assert(document.querySelectorAll('.output-log').length === 0, 'clear removes the visible log history');
  await act(async () => { panelRoot.unmount(); });
  dom.window.close();
  console.log('workspace interaction tests passed');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
