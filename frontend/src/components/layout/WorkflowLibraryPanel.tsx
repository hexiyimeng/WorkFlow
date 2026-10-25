import { useState } from 'react';
import { RefreshCw, Search, Workflow, Boxes, Trash2, Pencil, Folder } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';
import type { LibraryKind } from '../../utils/libraryApi';

export default function WorkflowLibraryPanel() {
  const { libraryItems, libraryBusy, libraryError, refreshLibrary, openLibraryItem, deleteLibraryItem, renameLibraryItem, isExecutionLocked } = useFlow();
  const [kind, setKind] = useState<LibraryKind>('workflows');
  const [search, setSearch] = useState('');
  const items = libraryItems[kind].filter(item => `${item.name} ${item.folder}`.toLowerCase().includes(search.toLowerCase()));
  return <>
    <header className="library-heading"><div><h2>Workflows</h2><span>Saved on this server</span></div>
      <button className="workspace-icon-button" aria-label="Refresh workflow library" title="Refresh" disabled={libraryBusy} onClick={() => { void refreshLibrary(); }}><RefreshCw size={16} /></button></header>
    <div className="library-kind-tabs" role="tablist" aria-label="Library type">{(['workflows', 'subgraphs'] as const).map(value => <button key={value} role="tab" aria-selected={kind === value} onClick={() => setKind(value)}>{value === 'workflows' ? 'Workflows' : 'Subgraphs'}</button>)}</div>
    <div className="library-search"><Search size={14} /><input aria-label="Search saved workflows" placeholder="Search name or folder…" value={search} onChange={event => setSearch(event.target.value)} /></div>
    {libraryError && <p className="library-error" role="alert">{libraryError}</p>}
    <div className="library-list custom-scrollbar">
      {items.map(item => <article className="saved-workflow" key={item.id}>
        <button className="saved-workflow__open" title={kind === 'workflows' ? 'Open workflow in a tab' : 'Insert a copy into the canvas'} disabled={libraryBusy || isExecutionLocked}
          onClick={() => { void openLibraryItem(kind, item.id); }}>{kind === 'workflows' ? <Workflow size={17} /> : <Boxes size={17} />}<span><strong>{item.name}</strong><small>{item.folder ? <><Folder size={11} /> {item.folder} · </> : null}{new Date(item.updatedAt).toLocaleDateString()}</small></span></button>
        <div className="saved-workflow__actions"><button className="workspace-icon-button" aria-label={`Rename ${item.name}`} disabled={libraryBusy || isExecutionLocked}
          onClick={() => { const name = prompt('Workflow name', item.name); if (name?.trim()) void renameLibraryItem(kind, item, name.trim()); }}><Pencil size={13} /></button>
          <button className="workspace-icon-button" aria-label={`Delete saved ${item.name}`} disabled={libraryBusy || isExecutionLocked}
            onClick={() => { if (confirm(`Delete saved ${kind === 'workflows' ? 'workflow' : 'subgraph'} "${item.name}"? Open tabs and output data are kept.`)) void deleteLibraryItem(kind, item); }}><Trash2 size={13} /></button></div>
      </article>)}
      {!items.length && <div className="library-empty"><Workflow size={24} /><strong>{search ? 'No matching workflows' : 'No saved items yet'}</strong><span>{kind === 'workflows' ? 'Use Save to keep your workflow here.' : 'Select a subgraph, then save it to the library.'}</span></div>}
    </div><footer className="library-footer">{kind === 'workflows' ? 'Save updates the original · Save as creates a copy' : 'Inserting creates an independent copy'}</footer>
  </>;
}
