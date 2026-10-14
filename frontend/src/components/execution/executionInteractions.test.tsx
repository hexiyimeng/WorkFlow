import { JSDOM } from 'jsdom';

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

async function main() {
  const dom = new JSDOM('<!doctype html><html><body><div id="root"></div></body></html>', {
    url: 'http://localhost/',
  });
  Object.assign(globalThis, {
    window: dom.window, document: dom.window.document,
    localStorage: dom.window.localStorage,
    HTMLElement: dom.window.HTMLElement,
    IS_REACT_ACT_ENVIRONMENT: true,
  });
  // React DOM must initialize after the DOM is available.
  const { act } = await import('react');
  const { createRoot } = await import('react-dom/client');
  const { FlowContext } = await import('../../context/FlowContextDef');
  const { default: Drawer } = await import('./ExecutionSettingsDrawer');
  const { ValuePopup } = await import('../DynamicNode');
  const { default: BottomPanel } = await import('../layout/BottomPanel');
  const root = createRoot(document.getElementById('root')!);
  const resources = await import('../../utils/workerResources');
  let saves = 0;
  let closes = 0;
  let allowSave = true;
  const context = {
    isExecutionSettingsOpen: true,
    activeWorkflowDocumentId: 'test-workflow',
    activeExecutionSettings: { version: 1, mode: 'full_graph' },
    executionSettingsValidation: null,
    executionPreflight: { windowable: false, requiredResources: { requiredWorkerProfiles: { CPU: 1, GPU: 1 } } },
    isConnected: true, isExecuting: false, isPreflighting: false,
    closeExecutionSettings: () => { closes += 1; },
    refreshExecutionSettingsPreflight: async () => undefined,
    saveExecutionSettings: async () => {
      saves += 1;
      assert(resources.loadWorkerProfiles().find(p => p.name === 'CPU')?.physical_resources.cpu === 12,
        'preflight must see the edited Worker configuration');
      return allowSave;
    },
  } as unknown as import('../../context/FlowContextDef').FlowContextType;

  const renderDrawer = async () => {
    await act(async () => { root.render(<FlowContext.Provider value={context}><Drawer /></FlowContext.Provider>); });
  };
  const cpuInput = () => document.querySelector<HTMLInputElement>('fieldset input[type="number"]')!;
  const change = async (element: HTMLInputElement, value: string) => {
    await act(async () => {
      Object.getOwnPropertyDescriptor(dom.window.HTMLInputElement.prototype, 'value')!.set!.call(element, value);
      element.dispatchEvent(new dom.window.Event('input', { bubbles: true }));
    });
  };
  const submit = async () => {
    await act(async () => {
      document.querySelector('form')!.dispatchEvent(new dom.window.Event('submit', { bubbles: true, cancelable: true }));
    });
  };
  await renderDrawer();
  assert(!document.body.textContent?.includes('Save Worker Resources'), 'only one Save entry point');
  await change(cpuInput(), '');
  await submit();
  assert(saves === 0 && closes === 0, 'invalid Worker fields must prevent saving or closing');
  assert(document.body.textContent?.includes('Enter a positive whole number.'), 'invalid Worker fields explain how to fix them');
  await change(cpuInput(), '12');
  allowSave = false;
  await submit();
  assert(resources.loadWorkerProfiles().length === 0, 'failed settings validation restores old Worker configuration');
  assert(closes === 0 && cpuInput().value === '12', 'keep the drawer and edited values after a rejected save');
  allowSave = true;
  await submit();
  assert(Number(saves) === 2 && Number(closes) === 1, 'one Save saves settings and closes after success');
  assert(resources.loadWorkerProfiles().length === 2, 'save both Worker types');
  assert(resources.loadWorkerPools().length === 2, 'save the Job ranges with settings');
  await change(cpuInput(), '24');
  assert(resources.loadWorkerProfiles()[0].physical_resources.cpu === 12, 'editing alone must not persist');
  await act(async () => { root.unmount(); });
  assert(resources.loadWorkerProfiles()[0].physical_resources.cpu === 12, 'closing without Save discards Worker edits');
  const popupRoot = createRoot(document.getElementById('root')!);
  const path = '/data/' + 'a-long-microscopy-directory/'.repeat(12) + 'input.ome.zarr';
  let savedValue = '';
  let cancelled = false;
  await act(async () => { popupRoot.render(<ValuePopup name="input_path" initialValue={path}
    anchorRect={null} onSave={value => { savedValue = value; }} onClose={() => { cancelled = true; }} />); });
  const textarea = document.querySelector('textarea')!;
  assert(textarea.value === path && textarea.wrap === 'soft', 'wrap a long path without changing its contents');
  assert(document.activeElement === textarea, 'focus the editor on opening');
  await act(async () => { textarea.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter', isComposing: true, bubbles: true })); });
  assert(savedValue === '', 'IME composition must not save prematurely');
  await act(async () => { textarea.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter', bubbles: true })); });
  assert(savedValue === path, 'Enter preserves the exact path');
  await act(async () => { textarea.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true })); });
  assert(cancelled, 'Escape cancels editing');
  await act(async () => { popupRoot.unmount(); });

  const panelRoot = createRoot(document.getElementById('root')!);
  dom.window.HTMLElement.prototype.scrollIntoView = () => undefined;
  await act(async () => { panelRoot.render(<FlowContext.Provider value={{
    logs: [{ id: 'failure', timestamp: Date.now(), type: 'error', message: 'Cellpose [node-2]: invalid block', details: 'Traceback: algorithm failed' }],
    clearLogs: () => undefined, isConsoleOpen: true, toggleConsole: () => undefined,
    executionState: { phase: 'failed', executionId: 'test', lastError: 'invalid block', windowProgress: null },
    websocketStatus: 'connected', isExecuting: false, isCancelling: false, stopFlow: () => undefined,
  } as unknown as import('../../context/FlowContextDef').FlowContextType}><BottomPanel /></FlowContext.Provider>); });
  assert(document.body.textContent?.includes('Cellpose [node-2]'), 'console shows the failed node');
  assert(document.querySelector('details')?.open === false, 'stack traces are collapsed initially');
  assert(document.querySelector('pre')?.textContent?.includes('Traceback'), 'details retain stack traces');
  await act(async () => { panelRoot.unmount(); });
  dom.window.close();
  console.log('execution interaction tests passed');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
