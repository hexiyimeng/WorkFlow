import { Hand, MousePointer2, Minus, Plus, Scan, Map } from 'lucide-react';
import { useReactFlow, useViewport } from '@xyflow/react';

export default function CanvasToolbar({ mode, setMode, showMap, toggleMap }: {
  mode: 'pan' | 'select'; setMode: (mode: 'pan' | 'select') => void; showMap: boolean; toggleMap: () => void;
}) {
  const { fitView, zoomIn, zoomOut, zoomTo } = useReactFlow();
  const { zoom } = useViewport();
  return <div className="canvas-toolbar" role="toolbar" aria-label="Canvas tools">
    <button title="Pan tool" aria-label="Pan tool" aria-pressed={mode === 'pan'} onClick={() => setMode('pan')}><Hand size={16} /></button>
    <button title="Selection tool" aria-label="Selection tool" aria-pressed={mode === 'select'} onClick={() => setMode('select')}><MousePointer2 size={16} /></button>
    <span />
    <button title="Zoom Out" aria-label="Zoom Out" onClick={() => { void zoomOut({ duration: 150 }); }}><Minus size={16} /></button>
    <button className="canvas-toolbar__zoom" title="Reset zoom to 100%" onClick={() => { void zoomTo(1, { duration: 150 }); }}>{Math.round(zoom * 100)}%</button>
    <button title="Zoom In" aria-label="Zoom In" onClick={() => { void zoomIn({ duration: 150 }); }}><Plus size={16} /></button>
    <span />
    <button title="Fit View" aria-label="Fit View" onClick={() => { void fitView({ padding: .18, maxZoom: 1.1, duration: 200 }); }}><Scan size={16} /></button>
    <button title="Toggle Minimap" aria-label="Toggle Minimap" aria-pressed={showMap} onClick={toggleMap}><Map size={16} /></button>
  </div>;
}
