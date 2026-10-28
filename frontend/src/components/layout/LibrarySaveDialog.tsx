import { useEffect, useRef, useState } from 'react';
import { X } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';
import { Button } from '../ui/Button';

export default function LibrarySaveDialog() {
  const { librarySaveDialog: dialog, closeLibrarySave, saveLibraryDocument, libraryBusy, libraryError } = useFlow();
  return dialog ? <SaveForm key={`${dialog.kind}:${dialog.asCopy}:${dialog.name}`} dialog={dialog} close={closeLibrarySave} save={saveLibraryDocument} busy={libraryBusy} error={libraryError} /> : null;
}

function SaveForm({ dialog, close, save, busy, error }: {
  dialog: NonNullable<ReturnType<typeof useFlow>['librarySaveDialog']>; close: () => void;
  save: ReturnType<typeof useFlow>['saveLibraryDocument']; busy: boolean; error: string | null;
}) {
  const [name, setName] = useState(dialog.name);
  const [folder, setFolder] = useState(dialog.folder);
  const panel = useRef<HTMLFormElement>(null);
  useEffect(() => { const previous = document.activeElement as HTMLElement | null;
    panel.current?.querySelector<HTMLInputElement>('input')?.focus();
    return () => previous?.focus(); }, []);
  return <div className="library-modal-backdrop" onKeyDown={event => {
    if (event.key === 'Escape') close();
    if (event.key === 'Tab') {
      const controls = [...(panel.current?.querySelectorAll<HTMLElement>('button:not(:disabled),input:not(:disabled)') || [])];
      const next = controls[(controls.indexOf(document.activeElement as HTMLElement) + (event.shiftKey ? controls.length - 1 : 1)) % controls.length];
      if (next) { event.preventDefault(); next.focus(); }
    }
  }}>
    <form ref={panel} className="library-modal" role="dialog" aria-modal="true" aria-label="Save workflow" onSubmit={event => { event.preventDefault(); if (name.trim()) void save(dialog, name.trim(), folder.trim()); }}>
      <header><h2>{dialog.kind === 'subgraphs' ? '保存为可复用节点（Blueprint）' : dialog.asCopy ? 'Save workflow as' : 'Save workflow'}</h2><button type="button" aria-label="Close save dialog" disabled={busy} onClick={close}><X size={18} /></button></header>
      <label>Name<input aria-label="Saved workflow name" value={name} required maxLength={160} disabled={busy} onChange={event => setName(event.target.value)} /></label>
      <label>Folder <small>(optional)</small><input aria-label="Workflow folder" placeholder="For example: Experiments" value={folder} maxLength={320} disabled={busy} onChange={event => setFolder(event.target.value)} /></label>
      <p>{dialog.kind === 'subgraphs' ? '保存后可从节点库添加，实例之间独立编辑。' : '保存到后端服务所在的工作流库。'}</p>
      {error && <p className="library-error" role="alert">{error}</p>}
      <footer><Button type="button" variant="ghost" onClick={close} disabled={busy}>Cancel</Button><Button type="submit" variant="primary" disabled={!name.trim() || busy} loading={busy}>Save</Button></footer>
    </form>
  </div>;
}
