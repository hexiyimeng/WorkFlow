import { useMemo, useState, type DragEvent } from 'react';
import { Boxes, ChevronDown, ChevronsDownUp, ChevronsUpDown, PanelLeftClose, Plus, Search, Terminal, X, Box, GripVertical } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';
import { usePanelResize } from '../../hooks/usePanelResize';
import type { NodeSpec } from '../../types';

type LibraryNode = NodeSpec & { type: string };

function NodeItem({ node, locked, add }: { node: LibraryNode; locked: boolean; add: (type: string) => void }) {
  const startDrag = (event: DragEvent) => {
    if (locked) { event.preventDefault(); return; }
    event.dataTransfer.setData('application/reactflow', node.type);
    event.dataTransfer.effectAllowed = 'move';
  };
  return <button type="button" className="library-node" draggable={!locked} disabled={locked}
    title={`${node.display_name}\n${node.description || node.type}\n${node.category}`}
    aria-label={`Add ${node.display_name}`} onDragStart={startDrag} onClick={() => add(node.type)}>
    <GripVertical size={11} className="library-node__grip" />
    <Box size={15} className="library-node__icon" />
    <span className="library-node__name">{node.display_name}</span>
    {node.required_worker_profile === 'GPU' && <span className="library-node__gpu">GPU</span>}
    <Plus size={13} className="library-node__add" />
  </button>;
}

export default function Sidebar() {
  const { addNode, nodeDefs, isExecutionLocked, isConsoleOpen, toggleConsole } = useFlow();
  const [open, setOpen] = useState(true);
  const [search, setSearch] = useState('');
  const [collapsed, setCollapsed] = useState<Set<string>>(() => new Set());
  const { size, separatorProps } = usePanelResize('x', 276, 240, 420);
  const entries = useMemo(() => Object.entries(nodeDefs).map(([type, node]) => ({ ...node, type })), [nodeDefs]);
  const groups = useMemo(() => {
    const result = new Map<string, LibraryNode[]>();
    for (const node of entries) {
      const category = node.category || 'Other';
      result.set(category, [...(result.get(category) ?? []), node]);
    }
    return [...result].sort(([a], [b]) => a.localeCompare(b)).map(([category, nodes]) => ({ category,
      label: category.replace(/^WorkFlow\//, '').replaceAll('/', ' / '), nodes: nodes.sort((a, b) => a.display_name.localeCompare(b.display_name)) }));
  }, [entries]);
  const terms = search.trim().toLowerCase().split(/\s+/).filter(Boolean);
  const results = entries.filter(node => {
    const text = `${node.display_name} ${node.type} ${node.category} ${node.description || ''} ${node.required_worker_profile || ''}`.toLowerCase();
    return terms.every(term => text.includes(term));
  });
  const toggleCategory = (category: string) => setCollapsed(previous => {
    const next = new Set(previous);
    if (next.has(category)) next.delete(category); else next.add(category);
    return next;
  });
  return (
    <aside className="workspace-sidebar" aria-label="Node library">
      <nav className="workspace-rail" aria-label="Workspace panels">
        <button title="Node Library" aria-label="Node Library" aria-expanded={open} data-active={open || undefined} onClick={() => setOpen(current => !current)}><Boxes size={19} /></button>
        <button title="Toggle output" aria-label="Toggle output" aria-pressed={isConsoleOpen} data-active={isConsoleOpen || undefined} onClick={toggleConsole}><Terminal size={18} /></button>
      </nav>
      {open && <section className="library-panel" style={{ width: size }}>
        <header className="library-heading"><div><h2>Node Library</h2><span>{entries.length} available nodes</span></div>
          <button className="workspace-icon-button" title="Collapse node library" aria-label="Collapse node library" onClick={() => setOpen(false)}><PanelLeftClose size={16} /></button>
        </header>
        <div className="library-search"><Search size={14} /><input type="search" aria-label="Search nodes" placeholder="Search nodes, types, categories…" value={search}
          onChange={event => setSearch(event.target.value)} />
          {search && <button aria-label="Clear node search" onClick={() => setSearch('')}><X size={13} /></button>}
        </div>
        <div className="library-list-heading"><span>{terms.length ? `${results.length} matching nodes` : 'All nodes'}</span>
          {!terms.length && <button className="workspace-icon-button" title={collapsed.size ? 'Expand all categories' : 'Collapse all categories'}
            aria-label={collapsed.size ? 'Expand all categories' : 'Collapse all categories'}
            onClick={() => setCollapsed(collapsed.size ? new Set() : new Set(groups.map(group => group.category)))}>
            {collapsed.size ? <ChevronsUpDown size={14} /> : <ChevronsDownUp size={14} />}</button>}
        </div>
        <div className="library-list custom-scrollbar">
          {terms.length ? results.map(node => <NodeItem key={node.type} node={node} locked={isExecutionLocked} add={addNode} />) : groups.map(group => <section key={group.category} className="library-group">
            <button className="library-group__heading" aria-expanded={!collapsed.has(group.category)} onClick={() => toggleCategory(group.category)}>
              <ChevronDown size={12} style={{ transform: collapsed.has(group.category) ? 'rotate(-90deg)' : undefined }} /><span>{group.label}</span><small>{group.nodes.length}</small>
            </button>
            {!collapsed.has(group.category) && group.nodes.map(node => <NodeItem key={node.type} node={node} locked={isExecutionLocked} add={addNode} />)}
          </section>)}
          {terms.length > 0 && results.length === 0 && <div className="library-empty"><Search size={24} /><strong>No matching nodes</strong><span>Try a node name, category or data type.</span><button onClick={() => setSearch('')}>Clear search</button></div>}
          {entries.length === 0 && <div className="library-empty"><Boxes size={24} /><span>Waiting for node definitions…</span></div>}
        </div>
        <footer className="library-footer"><GripVertical size={12} />Drag to the canvas or click to add</footer>
        <div {...separatorProps} aria-label="Resize node library" className="library-resize-handle" />
      </section>}
    </aside>
  );
}
