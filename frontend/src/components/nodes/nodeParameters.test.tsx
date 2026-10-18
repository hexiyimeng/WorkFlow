import { JSDOM } from 'jsdom';

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

async function main() {
  const dom = new JSDOM('<!doctype html><div id="root"></div>', { url: 'http://localhost/' });
  Object.assign(globalThis, { window: dom.window, document: dom.window.document,
    HTMLElement: dom.window.HTMLElement, IS_REACT_ACT_ENVIRONMENT: true });
  const { act, useState } = await import('react');
  const { createRoot } = await import('react-dom/client');
  const { NodeParameter } = await import('./NodeParameter');
  const root = createRoot(document.getElementById('root')!);

  function Parameters({ disabled = false }: { disabled?: boolean }) {
    const [values, setValues] = useState<Record<string, unknown>>({ count: 3, gain: 0.4, enabled: false, mode: 'auto' });
    const change = (name: string, value: unknown) => setValues(previous => ({ ...previous, [name]: value }));
    return <>
      <NodeParameter name="count" config={['INT', { min: 1, max: 10 }]} value={values.count} onChange={change} disabled={disabled} />
      <NodeParameter name="gain" config={['FLOAT', { min: 0, max: 1, step: 0.1 }]} value={values.gain} onChange={change} disabled={disabled} />
      <NodeParameter name="enabled" config={['BOOLEAN']} value={values.enabled} onChange={change} disabled={disabled} />
      <NodeParameter name="mode" config={['auto manual'.split(' ')]} value={values.mode} onChange={change} disabled={disabled} />
    </>;
  }
  await act(async () => { root.render(<Parameters />); });
  const field = (name: string) => document.querySelector<HTMLInputElement>(`input[aria-label="${name}"]`)!;
  const edit = async (name: string, value: string, key = 'Enter') => {
    await act(async () => { field(name).focus(); });
    await act(async () => {
      Object.getOwnPropertyDescriptor(dom.window.HTMLInputElement.prototype, 'value')!.set!.call(field(name), value);
      field(name).dispatchEvent(new dom.window.Event('input', { bubbles: true }));
    });
    await act(async () => { field(name).dispatchEvent(new dom.window.KeyboardEvent('keydown', { key, bubbles: true })); });
  };
  await edit('gain', '0.75');
  assert(field('gain').value === '0.75', 'decimal editing preserves the entered value');
  await edit('gain', '0.25', 'Escape');
  assert(field('gain').value === '0.75', 'Escape discards a numeric draft');
  await edit('gain', '');
  assert(field('gain').value === '0.75', 'clearing a number must not silently save zero');
  await edit('gain', '9');
  assert(field('gain').value === '1', 'numeric input respects the declared maximum');
  await edit('count', '-5');
  assert(field('count').value === '1', 'numeric input respects the declared minimum');
  await edit('count', '2.7');
  assert(field('count').value === '3', 'integer parameters stay integers');
  await act(async () => { document.querySelector('[aria-label="Increase count"]')!
    .dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true, shiftKey: true })); });
  assert(field('count').value === '10', 'Shift stepping also respects bounds');
  const toggle = document.querySelector<HTMLButtonElement>('[role="switch"]')!;
  await act(async () => { toggle.click(); });
  assert(toggle.getAttribute('aria-checked') === 'true', 'switch changes a Boolean value');
  const select = document.querySelector('select')!;
  await act(async () => { select.value = 'manual'; select.dispatchEvent(new dom.window.Event('change', { bubbles: true })); });
  assert(select.value === 'manual', 'dropdown stores its original enum value');
  await act(async () => { root.render(<Parameters disabled />); });
  assert(field('count').disabled && select.disabled && toggle.disabled, 'all parameter types respect the execution lock');
  await act(async () => { toggle.click(); });
  assert(toggle.getAttribute('aria-checked') === 'true', 'locked controls cannot change values');
  await act(async () => { root.unmount(); });
  dom.window.close();
  console.log('node parameter interaction tests passed');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
