import { useEffect, useId, useMemo, useRef, useState, type CSSProperties } from 'react';
import { createPortal } from 'react-dom';

interface ValuePopupProps {
  name: string;
  initialValue: string;
  onSave: (value: string) => void;
  onClose: () => void;
  anchorRect: DOMRect | null;
}

export const ValuePopup = ({ name, initialValue, onSave, onClose, anchorRect }: ValuePopupProps) => {
  const [val, setVal] = useState(initialValue);
  const inputId = useId();
  const inputRef = useRef<HTMLTextAreaElement>(null);
  useEffect(() => {
    const previous = document.activeElement;
    inputRef.current?.focus();
    inputRef.current?.select();
    return () => { if (previous instanceof HTMLElement && previous.isConnected) previous.focus(); };
  }, []);

  const style: CSSProperties = useMemo(() => {
    if (!anchorRect) return { position: 'fixed', top: '50%', left: '50%', transform: 'translate(-50%, -50%)' };
    const width = Math.min(720, window.innerWidth - 24);
    let top = anchorRect.bottom + 8;
    const left = Math.max(12, Math.min(
      anchorRect.left + anchorRect.width / 2 - width / 2,
      window.innerWidth - width - 12,
    ));
    if (top + 220 > window.innerHeight) top = Math.max(12, anchorRect.top - 220);
    return { top, left, position: 'fixed' as const };
  }, [anchorRect]);

  return createPortal(
    <div className="fixed inset-0 z-[9999]" onClick={onClose}>
      <div
        role="dialog"
        aria-modal="true"
        aria-label={`Edit ${name}`}
        className="node-value-editor bg-[var(--color-bg-surface)] border border-[var(--color-border-default)] rounded-[var(--radius-lg)] shadow-[var(--shadow-floating)] p-4 space-y-3 w-[min(720px,calc(100vw-24px))] max-h-[calc(100vh-24px)] overflow-auto"
        style={style}
        onClick={e => e.stopPropagation()}
        onKeyDown={event => {
          event.stopPropagation();
          if (event.key === 'Escape' && !event.nativeEvent.isComposing) onClose();
          if (event.key !== 'Tab') return;
          const controls = event.currentTarget.querySelectorAll<HTMLElement>('textarea, button');
          const first = controls[0];
          const last = controls[controls.length - 1];
          if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
          if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
        }}
      >
        <label htmlFor={inputId} className="block text-[12px] text-[var(--color-text-secondary)] font-semibold">{name}</label>
        <textarea
          id={inputId}
          ref={inputRef}
          rows={4}
          wrap="soft"
          className="w-full min-h-[100px] resize-y bg-[var(--color-bg-field)] text-[var(--color-text-primary)] text-[13px] leading-6 font-mono px-3 py-2 rounded-[var(--radius-md)] border border-[var(--color-border-default)] focus:border-[var(--color-border-focus)] outline-none [overflow-wrap:anywhere]"
          value={val}
          onChange={e => setVal(e.target.value)}
          onKeyDown={e => {
            if (e.nativeEvent.isComposing) return;
            if (e.key === 'Enter') { e.preventDefault(); onSave(val); }
          }}
        />
        <div className="flex items-center justify-between gap-3">
          <span className="text-[10px] text-[var(--color-text-muted)]">Enter to save · Esc to cancel</span>
          <div className="flex gap-2">
            <button type="button" onClick={onClose} className="px-3 py-1.5 text-[11px] text-[var(--color-text-secondary)]">Cancel</button>
            <button
              type="button"
              onClick={() => onSave(val)}
              className="shrink-0 px-2.5 py-1.5 text-[10px] font-bold text-white bg-[var(--color-accent)] hover:bg-[var(--color-accent-hover)] rounded-[var(--radius-md)] transition-colors duration-[var(--motion-fast)]"
            >
              OK
            </button>
          </div>
        </div>
      </div>
    </div>, document.body
  );
};
