import { useLayoutEffect, useRef, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import './actionMenu.css';

export interface MenuAction {
  label: string; icon?: ReactNode; shortcut?: string; disabled?: boolean;
  action: () => void; separator?: boolean; keepOpen?: boolean;
}

export default function ActionMenu({ x, y, label, items, onClose, children }: {
  x: number; y: number; label: string; items: MenuAction[]; onClose: () => void; children?: ReactNode;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const close = useRef(onClose);
  useLayoutEffect(() => { close.current = onClose; });
  useLayoutEffect(() => {
    const menu = ref.current!;
    const place = () => {
      const rect = menu.getBoundingClientRect();
      menu.style.left = `${Math.max(8, Math.min(x, window.innerWidth - rect.width - 8))}px`;
      menu.style.top = `${Math.max(8, Math.min(y, window.innerHeight - rect.height - 8))}px`;
    };
    place();
    const observer = new ResizeObserver(place); observer.observe(menu);
    const previous = document.activeElement;
    menu.querySelector<HTMLButtonElement>('button:not(:disabled)')?.focus();
    const outside = (event: PointerEvent) => { if (!menu.contains(event.target as globalThis.Node)) close.current(); };
    const resize = () => close.current();
    document.addEventListener('pointerdown', outside, true);
    window.addEventListener('resize', resize);
    return () => {
      observer.disconnect(); document.removeEventListener('pointerdown', outside, true); window.removeEventListener('resize', resize);
      if (menu.contains(document.activeElement) && previous instanceof HTMLElement && previous.isConnected) previous.focus();
    };
  }, [x, y]);
  return createPortal(<div ref={ref} role="menu" aria-label={label} className="action-menu nodrag nopan nowheel" style={{ left: x, top: y }}
    onPointerDown={event => event.stopPropagation()} onClick={event => event.stopPropagation()}
    onContextMenu={event => { event.preventDefault(); event.stopPropagation(); }}
    onKeyDown={event => {
      event.stopPropagation();
      if (event.key === 'Escape' || event.key === 'Tab') { event.preventDefault(); onClose(); return; }
      if (!['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) return;
      event.preventDefault();
      const buttons = [...ref.current!.querySelectorAll<HTMLButtonElement>('button[role="menuitem"]:not(:disabled)')];
      const current = buttons.indexOf(document.activeElement as HTMLButtonElement);
      const index = event.key === 'Home' ? 0 : event.key === 'End' ? buttons.length - 1 : (current + (event.key === 'ArrowDown' ? 1 : -1) + buttons.length) % buttons.length;
      buttons[index]?.focus();
    }}>
    {items.map(item => <div key={item.label}>{item.separator && <hr role="separator" />}<button role="menuitem" disabled={item.disabled}
      onClick={() => { if (!item.keepOpen) onClose(); item.action(); }}>{item.icon}<span>{item.label}</span>{item.shortcut && <kbd>{item.shortcut}</kbd>}</button></div>)}
    {children}
  </div>, document.body);
}
