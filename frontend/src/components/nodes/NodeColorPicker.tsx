import { NODE_COLORS } from '../../utils/nodePresentation';

export default function NodeColorPicker({ color, disabled, onChange, onPresetChosen }: {
  color?: string | null; disabled?: boolean; onChange: (color: string | undefined) => void; onPresetChosen?: () => void;
}) {
  return <div className="node-color-picker" role="group" aria-label="节点颜色">
    {NODE_COLORS.map(option => <button key={option.name} type="button" aria-label={`${option.name}颜色`}
      title={option.name} aria-pressed={color === option.value} disabled={disabled} onClick={() => { onChange(option.value); onPresetChosen?.(); }}>
      <span style={{ background: option.value || 'var(--node-base-heading)' }}>{option.value ? '' : '×'}</span>
    </button>)}
    <label className="node-custom-color" title="自定义颜色"><input type="color" aria-label="自定义节点颜色" value={color || '#779ac3'}
      disabled={disabled} onChange={event => onChange(event.target.value)} /><span>自定义</span></label>
  </div>;
}
