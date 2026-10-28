import { Boxes, ChevronDown, Folder, GripVertical, Pencil, Plus, RefreshCw, Trash2 } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';

export default function SubgraphLibraryGroup({ search, collapsed, toggle }: { search: string; collapsed: boolean; toggle: () => void }) {
  const { libraryItems, libraryBusy, libraryError, refreshLibrary, openLibraryItem, deleteLibraryItem, renameLibraryItem, isExecutionLocked } = useFlow();
  const terms = search.trim().toLowerCase().split(/\s+/).filter(Boolean);
  const items = libraryItems.subgraphs.filter(item => terms.every(term => `${item.name} ${item.folder} 子图 Blueprint`.toLowerCase().includes(term)));
  const locked = libraryBusy || isExecutionLocked;
  if (terms.length && !items.length) return null;
  return <section className="library-group subgraph-library-group" aria-label="子图节点库">
    <div className="subgraph-library-heading"><button className="library-group__heading" aria-expanded={!!terms.length || !collapsed} onClick={toggle}>
      <ChevronDown size={12} style={{ transform: !terms.length && collapsed ? 'rotate(-90deg)' : undefined }} /><span>子图节点（Blueprints）</span><small>{items.length}</small>
    </button><button className="workspace-icon-button" title="刷新子图节点库" aria-label="Refresh subgraph nodes" disabled={libraryBusy} onClick={() => { void refreshLibrary(); }}><RefreshCw size={13} /></button></div>
    {libraryError && <p className="library-error" role="alert">{libraryError}</p>}
    {(!!terms.length || !collapsed) && <>
      {items.map(item => <article className="saved-workflow subgraph-blueprint" key={item.id}>
        <button className="saved-workflow__open library-node" aria-label={`Add subgraph ${item.name}`} title={`${item.name}\n点击或拖入画布，创建独立的子图节点`}
          disabled={locked} draggable={!locked} onClick={() => { void openLibraryItem('subgraphs', item.id); }} onDragStart={event => {
            if (locked) { event.preventDefault(); return; }
            event.dataTransfer.setData('application/workflow-subgraph', item.id); event.dataTransfer.effectAllowed = 'copy';
          }}><GripVertical size={11} className="library-node__grip" /><Boxes size={15} /><span><strong>{item.name}</strong>{item.folder && <small><Folder size={11} />{item.folder}</small>}</span><Plus size={13} className="library-node__add" /></button>
        <div className="saved-workflow__actions"><button className="workspace-icon-button" aria-label={`Rename subgraph ${item.name}`} title="重命名已保存的子图节点" disabled={locked}
          onClick={() => { const name = prompt('子图节点名称', item.name); if (name?.trim()) void renameLibraryItem('subgraphs', item, name.trim()); }}><Pencil size={13} /></button>
          <button className="workspace-icon-button" aria-label={`Delete subgraph ${item.name}`} title="从节点库移除" disabled={locked}
            onClick={() => { if (confirm(`从节点库移除“${item.name}”？画布中已有的实例会保留。`)) void deleteLibraryItem('subgraphs', item); }}><Trash2 size={13} /></button></div>
      </article>)}
      {!items.length && <p className="subgraph-library-empty">选中子图，点击“保存为可复用节点”，即可在此添加使用。</p>}
    </>}
  </section>;
}
