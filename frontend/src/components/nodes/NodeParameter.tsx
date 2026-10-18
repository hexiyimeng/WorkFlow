import { useId, useRef, useState, type KeyboardEvent } from 'react';
import { ChevronDown, ChevronLeft, ChevronRight, Expand } from 'lucide-react';
import { ValuePopup } from './NodeValueEditor';

interface NodeParameterProps {
  name: string;
  config: [string | string[], Record<string, unknown>?];
  value: unknown;
  onChange: (name: string, value: unknown) => void;
  disabled?: boolean;
}

function parameterLabel(name: string): string {
  const label = name.replaceAll('_', ' ').replace(/\b(gpu|cpu|roi|rgb|2d|3d)\b/gi, word => word.toUpperCase());
  return label.charAt(0).toUpperCase() + label.slice(1);
}

function NumberField({ name, config, value, onChange, disabled }: NodeParameterProps) {
  const [type, options] = config;
  const isFloat = type === 'FLOAT';
  const parsed = Number(value ?? options?.default ?? 0);
  const current = Number.isFinite(parsed) ? parsed : 0;
  const step = Number(options?.step) > 0 ? Number(options?.step) : isFloat ? 0.01 : 1;
  const min = typeof options?.min === 'number' ? options.min : undefined;
  const max = typeof options?.max === 'number' ? options.max : undefined;
  const [draft, setDraft] = useState(String(current));
  const [editing, setEditing] = useState(false);
  const skipBlurRef = useRef(false);

  const normalize = (number: number) => {
    let next = isFloat ? Number(number.toFixed(10)) : Math.round(number);
    if (min !== undefined) next = Math.max(min, next);
    if (max !== undefined) next = Math.min(max, next);
    return next;
  };
  const adjust = (direction: number, shift: boolean) => {
    const next = normalize(current + direction * step * (shift ? 10 : 1));
    setDraft(String(next));
    onChange(name, next);
  };
  const commit = () => {
    setEditing(false);
    if (skipBlurRef.current) { skipBlurRef.current = false; return; }
    const number = Number(draft);
    if (draft.trim() !== '' && Number.isFinite(number)) onChange(name, normalize(number));
  };

  return (
    <div className="node-number-field">
      <button type="button" aria-label={`Decrease ${name}`} title="Decrease (Shift: ×10)"
        disabled={disabled || (min !== undefined && current <= min)}
        onClick={event => adjust(-1, event.shiftKey)}><ChevronLeft size={12} /></button>
      <input aria-label={name} role="spinbutton" inputMode={isFloat ? 'decimal' : 'numeric'}
        aria-valuenow={current} aria-valuemin={min} aria-valuemax={max}
        value={editing ? draft : String(current)} disabled={disabled}
        onFocus={() => { setDraft(String(current)); setEditing(true); }}
        onChange={event => setDraft(event.target.value)} onBlur={commit}
        onKeyDown={event => {
          event.stopPropagation();
          if (event.nativeEvent.isComposing) return;
          if (event.key === 'Enter') event.currentTarget.blur();
          if (event.key === 'Escape') { skipBlurRef.current = true; event.currentTarget.blur(); }
          if (event.key === 'ArrowUp' || event.key === 'ArrowDown') {
            event.preventDefault(); adjust(event.key === 'ArrowUp' ? 1 : -1, event.shiftKey);
          }
        }} />
      <button type="button" aria-label={`Increase ${name}`} title="Increase (Shift: ×10)"
        disabled={disabled || (max !== undefined && current >= max)}
        onClick={event => adjust(1, event.shiftKey)}><ChevronRight size={12} /></button>
    </div>
  );
}

export function NodeParameter(props: NodeParameterProps) {
  const { name, config, value, onChange, disabled = false } = props;
  const [type, options] = config;
  const labelId = useId();
  const [editing, setEditing] = useState(false);
  const [anchorRect, setAnchorRect] = useState<DOMRect | null>(null);
  const label = typeof options?.label === 'string' ? options.label : parameterLabel(name);
  const tooltip = typeof options?.tooltip === 'string' ? options.tooltip : name;
  const stopKey = (event: KeyboardEvent) => event.stopPropagation();
  const isString = type === 'STRING';
  const bool = value === true || String(value).toLowerCase() === 'true';

  return (
    <div className={`node-parameter nodrag nowheel${isString ? ' node-parameter--text' : ''}`}
      data-parameter={name} data-disabled={disabled || undefined}
      onPointerDown={event => event.stopPropagation()} onDoubleClick={event => event.stopPropagation()}
      onKeyDown={stopKey}>
      <span id={labelId} className="node-parameter__label" title={`${label}\n${tooltip}`}>{label}</span>
      {isString ? (
        <>
          <button type="button" className="node-text-field" disabled={disabled}
            aria-label={`Edit ${name}`} title={String(value ?? '') || tooltip}
            onClick={event => { setAnchorRect(event.currentTarget.getBoundingClientRect()); setEditing(true); }}>
            <span className={`node-text-field__value${value == null || value === '' ? ' is-empty' : ''}`}>
              {String(value ?? '') || 'Click to enter a value…'}
            </span>
            <Expand size={12} aria-hidden="true" />
          </button>
          {editing && !disabled && <ValuePopup name={label} initialValue={String(value ?? '')}
            anchorRect={anchorRect}
            onSave={next => { onChange(name, next); setEditing(false); }} onClose={() => setEditing(false)} />}
        </>
      ) : type === 'BOOLEAN' ? (
        <button type="button" role="switch" aria-checked={bool} aria-label={name}
          className="node-toggle" disabled={disabled} onClick={() => onChange(name, !bool)}>
          <span className="node-toggle__label">{String(bool ? options?.on ?? 'On' : options?.off ?? 'Off')}</span>
          <span className="node-toggle__track" aria-hidden="true"><span /></span>
        </button>
      ) : Array.isArray(type) ? (
        <div className="node-select-field">
          <select aria-label={name} aria-describedby={labelId} disabled={disabled}
            value={String(value ?? type[0] ?? '')} title={String(value ?? type[0] ?? '')}
            onChange={event => onChange(name, event.target.value)}>
            {type.map(option => <option key={option} value={option}>{option}</option>)}
          </select>
          <ChevronDown size={12} aria-hidden="true" />
        </div>
      ) : ['INT', 'FLOAT', 'LONG'].includes(String(type)) ? <NumberField {...props} /> : null}
    </div>
  );
}
