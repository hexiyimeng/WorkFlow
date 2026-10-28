import { useState } from 'react';
import { RefreshCw, Search, Workflow, Trash2, Pencil, Folder } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';

export default function WorkflowLibraryPanel() {
  const { libraryItems, libraryBusy, libraryError, refreshLibrary, openLibraryItem, deleteLibraryItem, renameLibraryItem, isExecutionLocked } = useFlow();
  const [search, setSearch] = useState('');
  const items = libraryItems.workflows.filter(item => `${item.name} ${item.folder}`.toLowerCase().includes(search.toLowerCase()));
  return <>
    <header className="library-heading"><div><h2>Workflows</h2><span>Saved on this server</span></div>
      <button className="workspace-icon-button" aria-label="Refresh workflow library" title="Refresh" disabled={libraryBusy} onClick={() => { void refreshLibrary(); }}><RefreshCw size={16} /></button></header>
    <div className="library-search"><Search size={14} /><input aria-label="Search saved workflows" placeholder="Search name or folder…" value={search} onChange={event => setSearch(event.target.value)} /></div>
    {libraryError && <p className="library-error" role="alert">{libraryError}</p>}
    <div className="library-list custom-scrollbar">
      {items.map(item => <article className="saved-workflow" key={item.id}>
        <button className="saved-workflow__open" title="Open workflow in a tab" disabled={libraryBusy || isExecutionLocked}
          onClick={() => { void openLibraryItem('workflows', item.id); }}><Workflow size={17} /><span><strong>{item.name}</strong><small>{item.folder ? <><Folder size={11} /> {item.folder} · </> : null}{new Date(item.updatedAt).toLocaleDateString()}</small></span></button>
        <div className="saved-workflow__actions"><button className="workspace-icon-button" aria-label={`Rename ${item.name}`} disabled={libraryBusy || isExecutionLocked}
          onClick={() => { const name = prompt('Workflow name', item.name); if (name?.trim()) void renameLibraryItem('workflows', item, name.trim()); }}><Pencil size={13} /></button>
          <button className="workspace-icon-button" aria-label={`Delete saved ${item.name}`} disabled={libraryBusy || isExecutionLocked}
            onClick={() => { if (confirm(`Delete saved workflow "${item.name}"? Open tabs and output data are kept.`)) void deleteLibraryItem('workflows', item); }}><Trash2 size={13} /></button></div>
      </article>)}
      {!items.length && <div className="library-empty"><Workflow size={24} /><strong>{search ? 'No matching workflows' : 'No saved items yet'}</strong><span>Use Save to keep your workflow here.</span></div>}
    </div><footer className="library-footer">Save updates the original · Save as creates a copy</footer>
  </>;
}
