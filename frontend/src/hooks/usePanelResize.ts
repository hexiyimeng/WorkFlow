import { useEffect, useRef, useState, type PointerEvent, type KeyboardEvent } from 'react';

export function usePanelResize(axis: 'x' | 'y', initial: number, min: number, max: number, direction = 1) {
  const [size, setSize] = useState(initial);
  const drag = useRef<{ start: number; size: number; cursor: string; userSelect: string } | null>(null);
  const clamp = (value: number) => Math.max(min, Math.min(max, value,
    typeof window === 'undefined' ? max : axis === 'x' ? window.innerWidth * .42 : window.innerHeight * .65));
  const finish = () => {
    if (!drag.current) return;
    document.body.style.cursor = drag.current.cursor;
    document.body.style.userSelect = drag.current.userSelect;
    drag.current = null;
  };
  useEffect(() => () => {
    if (drag.current) {
      document.body.style.cursor = drag.current.cursor;
      document.body.style.userSelect = drag.current.userSelect;
    }
  }, []);
  useEffect(() => {
    const resize = () => setSize(current => Math.max(min, Math.min(current, max,
      axis === 'x' ? window.innerWidth * .42 : window.innerHeight * .65)));
    window.addEventListener('resize', resize);
    return () => window.removeEventListener('resize', resize);
  }, [axis, min, max]);
  return {
    size,
    separatorProps: {
      role: 'separator' as const,
      tabIndex: 0,
      'aria-orientation': axis === 'x' ? 'vertical' as const : 'horizontal' as const,
      'aria-valuemin': min, 'aria-valuemax': max, 'aria-valuenow': size,
      onPointerDown: (event: PointerEvent<HTMLElement>) => {
        if (event.button !== 0) return;
        event.preventDefault();
        event.currentTarget.setPointerCapture(event.pointerId);
        drag.current = { start: axis === 'x' ? event.clientX : event.clientY, size,
          cursor: document.body.style.cursor, userSelect: document.body.style.userSelect };
        document.body.style.cursor = axis === 'x' ? 'col-resize' : 'row-resize';
        document.body.style.userSelect = 'none';
      },
      onPointerMove: (event: PointerEvent<HTMLElement>) => {
        if (drag.current) setSize(clamp(drag.current.size + direction * ((axis === 'x' ? event.clientX : event.clientY) - drag.current.start)));
      },
      onPointerUp: finish,
      onPointerCancel: finish,
      onLostPointerCapture: finish,
      onKeyDown: (event: KeyboardEvent<HTMLElement>) => {
        const delta = axis === 'x'
          ? event.key === 'ArrowRight' ? 16 : event.key === 'ArrowLeft' ? -16 : 0
          : event.key === 'ArrowUp' ? 16 : event.key === 'ArrowDown' ? -16 : 0;
        if (delta) { event.preventDefault(); setSize(current => clamp(current + delta)); }
      },
    },
  };
}
