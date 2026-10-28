import { useMemo, useState, useEffect, useRef, useCallback } from 'react';
import { useFlow } from '../../hooks/useFlowContext';
import { createPortal } from 'react-dom';
import type { NodeSpec } from '../../types';

interface MenuItem {
  label: string;
  value?: string;
  children?: MenuItem[];
}

// ─────────────────────────────────────────────
// FloatingSubMenu — rendered via createPortal,
// can appear at any depth and open its own children
// ─────────────────────────────────────────────
const FloatingSubMenu = ({
  items,
  anchorRect,
  depth,
  onSelect,
  onSubmenuOpen,
  onSubmenuCloseFromDepth,
  onSubmenuMount,
}: {
  items: MenuItem[];
  anchorRect: DOMRect;
  depth: number;
  onSelect: (v: string) => void;
  onSubmenuOpen: (item: MenuItem, rect: DOMRect, depth: number) => void;
  onSubmenuCloseFromDepth: (depth: number) => void;
  onSubmenuMount: (el: HTMLDivElement | null) => void;
}) => {
  const submenuWidth = 220;

  // Determine horizontal position
  let left = anchorRect.right + 4;
  if (left + submenuWidth > window.innerWidth - 8) {
    left = anchorRect.left - submenuWidth - 4;
  }
  if (left < 8) left = 8;

  // Determine vertical position
  const estimatedHeight = Math.min(items.length * 32 + 16, 320);
  let top = anchorRect.top;
  if (top + estimatedHeight > window.innerHeight - 8) {
    top = Math.max(8, window.innerHeight - estimatedHeight - 8);
  }
  if (top < 8) top = 8;

  return createPortal(
    <div
      ref={onSubmenuMount}
      className="fixed z-[10000] flex flex-col rounded-[var(--radius-md)] overflow-hidden"
      style={{
        left,
        top,
        width: submenuWidth,
        backgroundColor: 'var(--color-bg-surface)',
        border: '1px solid var(--color-border-default)',
        boxShadow: 'var(--shadow-dropdown)',
        maxHeight: `${window.innerHeight - top - 16}px`,
        overflowY: 'auto',
      }}
      onClick={e => e.stopPropagation()}
      onMouseEnter={() => onSubmenuCloseFromDepth(depth + 1)}
      onMouseLeave={() => onSubmenuCloseFromDepth(depth + 1)}
    >
      {items.map(item => {
        const hasChildren = !!(item.children && item.children.length > 0);
        return (
          <div
            key={item.label}
            className="relative"
            onMouseEnter={(e) => {
              if (hasChildren) {
                const rect = e.currentTarget.getBoundingClientRect();
                onSubmenuOpen(item, rect, depth + 1);
              }
            }}
          >
            <div
              onClick={(e) => {
                e.stopPropagation();
                if (!hasChildren && item.value) {
                  onSelect(item.value);
                }
              }}
              className="px-3 py-1.5 flex items-center justify-between cursor-pointer select-none text-[12px] text-[var(--color-text-primary)] hover:bg-[var(--color-bg-field-hover)] transition-colors duration-75 whitespace-nowrap"
            >
              <span className="truncate mr-4">{item.label}</span>
              {hasChildren && (
                <span className="text-[10px] opacity-70 shrink-0 ml-1">▶</span>
              )}
            </div>
          </div>
        );
      })}
    </div>,
    document.body
  );
};

// ─────────────────────────────────────────────
// MenuList — renders current depth level items
// ─────────────────────────────────────────────
const MenuList = ({
  items,
  onSelect,
  depth,
  onSubmenuOpen,
  onSubmenuCloseFromDepth,
}: {
  items: MenuItem[];
  onSelect: (v: string) => void;
  depth: number;
  onSubmenuOpen: (item: MenuItem, rect: DOMRect, depth: number) => void;
  onSubmenuCloseFromDepth: (depth: number) => void;
}) => {
  return (
    <div className="flex flex-col">
      {items.map(item => {
        const hasChildren = !!(item.children && item.children.length > 0);
        return (
          <div
            key={item.label}
            className="relative"
            onMouseEnter={(e) => {
              if (hasChildren) {
                const rect = e.currentTarget.getBoundingClientRect();
                onSubmenuOpen(item, rect, depth);
              }
            }}
            onMouseLeave={() => onSubmenuCloseFromDepth(depth + 1)}
          >
            <div
              onClick={(e) => {
                e.stopPropagation();
                if (!hasChildren && item.value) {
                  onSelect(item.value);
                }
              }}
              className="px-3 py-1.5 flex items-center justify-between cursor-pointer select-none text-[12px] text-[var(--color-text-primary)] hover:bg-[var(--color-bg-field-hover)] transition-colors duration-75 whitespace-nowrap"
            >
              <span className="truncate mr-4">{item.label}</span>
              {hasChildren && (
                <span className="text-[10px] opacity-70 shrink-0 ml-1">▶</span>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
};

// ─────────────────────────────────────────────
// ContextMenu
// ─────────────────────────────────────────────
interface ContextMenuProps {
  top: number;
  left: number;
  onClose: () => void;
  onAddNode: (type: string) => void;
  onAddGroup?: () => void;
}

export default function ContextMenu({ top, left, onClose, onAddNode, onAddGroup }: ContextMenuProps) {
  const { nodeDefs } = useFlow();
  const [search, setSearch] = useState('');

  // Stack of open floating submenus: each entry = { item, rect, depth }
  const [submenuStack, setSubmenuStack] = useState<Array<{ item: MenuItem; rect: DOMRect; depth: number }>>([]);
  const submenuCloseTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Track the deepest active submenu element for backdrop click detection
  const activeSubmenuRef = useRef<HTMLDivElement | null>(null);

  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => { inputRef.current?.focus(); }, []);
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  // Keep menu within viewport
  const adjustedLeft = Math.min(left, window.innerWidth - 310);
  const adjustedTop = Math.min(top, window.innerHeight - 80);

  const menuTree = useMemo(() => {
    const root: MenuItem[] = [];
    Object.entries(nodeDefs).forEach(([type, def]: [string, NodeSpec]) => {
      const cat = def.category || 'Utils';
      const parts = cat.split('/');
      let cur = root;
      parts.forEach(p => {
        let ex = cur.find(i => i.label === p);
        if (!ex) { ex = { label: p, children: [] }; cur.push(ex); }
        cur = ex.children!;
      });
      cur.push({ label: def.display_name, value: type });
    });
    const sort = (ns: MenuItem[]) => {
      ns.sort((a, b) => {
        const af = !!a.children; const bf = !!b.children;
        if (af !== bf) return af ? -1 : 1;
        return a.label.localeCompare(b.label);
      });
      ns.forEach(n => n.children && sort(n.children));
    };
    sort(root);
    return root;
  }, [nodeDefs]);

  const flatResults = useMemo(() => {
    if (!search) return null;
    const q = search.toLowerCase();
    return Object.entries(nodeDefs)
      .filter(([, d]) => d.display_name.toLowerCase().includes(q))
      .map(([type, d]) => ({ label: d.display_name, value: type, category: d.category, output: d.output }));
  }, [search, nodeDefs]);

  // Open a submenu at a given depth (keeps items shallower than depth, replaces item at depth)
  const openSubmenu = useCallback((item: MenuItem, rect: DOMRect, depth: number) => {
    if (submenuCloseTimer.current) clearTimeout(submenuCloseTimer.current);
    setSubmenuStack(prev => [
      ...prev.slice(0, depth),
      { item, rect, depth },
    ]);
  }, []);

  // Close submenus from a given depth onward
  const closeSubmenusFromDepth = useCallback((depth: number) => {
    if (submenuCloseTimer.current) clearTimeout(submenuCloseTimer.current);
    submenuCloseTimer.current = setTimeout(() => {
      setSubmenuStack(prev => prev.filter(s => s.depth < depth));
    }, 120);
  }, []);

  // Backdrop click: only close if the click is NOT inside the deepest FloatingSubMenu
  const handleBackdropClick = useCallback((e: React.MouseEvent) => {
    if (activeSubmenuRef.current?.contains(e.target as Node)) return;
    onClose();
  }, [onClose]);

  useEffect(() => {
    return () => {
      if (submenuCloseTimer.current) clearTimeout(submenuCloseTimer.current);
    };
  }, []);

  return createPortal(
    // Backdrop
    <div
      className="fixed inset-0 z-[9998]"
      style={{ backgroundColor: 'rgba(0,0,0,0)' }}
      onClick={handleBackdropClick}
      onContextMenu={e => { e.preventDefault(); e.stopPropagation(); }}
    >
      {/* Menu panel */}
      <div
        className="fixed flex flex-col rounded-[var(--radius-lg)]"
        style={{
          top: adjustedTop,
          left: adjustedLeft,
          width: 300,
          backgroundColor: 'var(--color-bg-surface)',
          border: '1px solid var(--color-border-default)',
          boxShadow: 'var(--shadow-floating)',
          zIndex: 9999,
          overflow: 'visible',
          maxHeight: '75vh',
        }}
        onClick={e => e.stopPropagation()}
        onContextMenu={e => e.stopPropagation()}
      >
        {onAddGroup && <button className="px-4 py-3 text-left hover:bg-[var(--color-bg-hover)]" onClick={() => { onAddGroup(); onClose(); }}>创建组 · Group</button>}
        {/* Search header */}
        <div
          style={{
            backgroundColor: 'var(--color-bg-surface-2)',
            borderBottom: '1px solid var(--color-border-subtle)',
            borderRadius: 'var(--radius-lg) var(--radius-lg) 0 0',
            flexShrink: 0,
          }}
        >
          <div className="px-3 pt-2.5 pb-2">
            <div className="relative flex items-center">
              <svg
                className="absolute left-2.5 w-3.5 h-3.5 pointer-events-none"
                style={{ color: 'var(--color-text-muted)' }}
                fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
              >
                <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <input
                ref={inputRef}
                className="w-full h-8 rounded-[var(--radius-md)] pl-8 pr-3 text-[12px] outline-none transition-colors"
                style={{
                  backgroundColor: 'var(--color-bg-field)',
                  border: '1px solid var(--color-border-default)',
                  color: 'var(--color-text-primary)',
                }}
                placeholder="Search nodes..."
                value={search}
                onChange={e => setSearch(e.target.value)}
                onKeyDown={e => {
                  if (e.key === 'Enter' && flatResults && flatResults[0]?.value) {
                    onAddNode(flatResults[0].value); onClose();
                  }
                }}
                onFocus={e => (e.currentTarget.style.borderColor = 'var(--color-border-focus)')}
                onBlur={e => (e.currentTarget.style.borderColor = 'var(--color-border-default)')}
              />
            </div>
          </div>
        </div>

        {/* Content — scrollable, does NOT clip submenus */}
        <div
          className="flex-1 overflow-y-auto custom-scrollbar"
          style={{ overflowX: 'visible' }}
        >
          {search ? (
            <div>
              {flatResults && flatResults.length > 0 ? (
                <>
                  <div className="px-3 py-1.5 text-[9px] font-bold uppercase tracking-wider" style={{ color: 'var(--color-text-muted)' }}>
                    {flatResults.length} result{flatResults.length !== 1 ? 's' : ''}
                  </div>
                  {flatResults.map(item => {
                    const color = getCategoryColor(item.category);
                    return (
                      <div
                        key={item.value}
                        onClick={() => { if (item.value) { onAddNode(item.value); onClose(); } }}
                        className="flex items-center gap-2.5 px-3 py-2 cursor-pointer transition-colors"
                        style={{ color: 'var(--color-text-primary)' }}
                        onMouseEnter={e => (e.currentTarget.style.backgroundColor = 'var(--color-bg-field-hover)')}
                        onMouseLeave={e => (e.currentTarget.style.backgroundColor = 'transparent')}
                      >
                        <div className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: color }} />
                        <div className="flex-1 min-w-0">
                          <div className="text-[12px] truncate">{item.label}</div>
                          <div className="text-[10px]" style={{ color: 'var(--color-text-muted)' }}>{item.category}</div>
                        </div>
                        {item.output && item.output.length > 0 && (
                          <span
                            className="text-[9px] px-1 py-px rounded shrink-0"
                            style={{ backgroundColor: 'var(--color-bg-field)', color: 'var(--color-text-muted)' }}
                          >
                            {item.output[0]}
                          </span>
                        )}
                      </div>
                    );
                  })}
                </>
              ) : (
                <div className="px-4 py-6 text-center text-[11px] italic" style={{ color: 'var(--color-text-muted)' }}>
                  No nodes match "{search}"
                </div>
              )}
            </div>
          ) : (
            <div className="py-1">
              <MenuList
                items={menuTree}
                onSelect={(val) => { onAddNode(val); onClose(); }}
                depth={0}
                onSubmenuOpen={openSubmenu}
                onSubmenuCloseFromDepth={closeSubmenusFromDepth}
              />
            </div>
          )}
        </div>

        {/* Footer */}
        <div
          className="shrink-0 px-3 py-1.5 text-[9px]"
          style={{
            backgroundColor: 'var(--color-bg-surface-2)',
            borderTop: '1px solid var(--color-border-subtle)',
            borderRadius: '0 0 var(--radius-lg) var(--radius-lg)',
            color: 'var(--color-text-muted)',
          }}
        >
          <span>↵ Add node</span>
          <span className="mx-2">·</span>
          <span>Esc close</span>
          <span className="mx-2">·</span>
          <span>▶ Submenu</span>
        </div>
      </div>

      {/* Floating submenus — one per depth level, all via portal */}
      {submenuStack.map(entry => (
        entry.item.children && (
          <FloatingSubMenu
            key={`${entry.depth}-${entry.item.label}`}
            items={entry.item.children}
            anchorRect={entry.rect}
            depth={entry.depth}
            onSelect={(v) => { onAddNode(v); onClose(); }}
            onSubmenuOpen={openSubmenu}
            onSubmenuCloseFromDepth={closeSubmenusFromDepth}
            onSubmenuMount={(el) => { activeSubmenuRef.current = el; }}
          />
        )
      ))}
    </div>,
    document.body
  );
}

function getCategoryColor(cat?: string): string {
  const CATEGORY_COLORS: Record<string, string> = {
    'Image': '#3b82f6', 'Loader': '#22c55e', 'Output': '#a78bfa',
    'Processing': '#f59e0b', 'Transform': '#06b6d4', 'Utils': '#94a3b8',
    'WorkFlow/IO': '#22c55e', 'WorkFlow/Processing': '#3b82f6',
    'WorkFlow/Segmentation': '#f59e0b', 'WorkFlow/Output': '#a78bfa',
  };
  if (!cat) return '#94a3b8';
  return CATEGORY_COLORS[cat.split('/')[0]] ?? '#94a3b8';
}
