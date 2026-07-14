import { useState, useCallback, type ComponentType } from 'react';
import {
  ReactFlow,
  Background,
  MiniMap,
  Panel,
  useReactFlow,
  useViewport,
  BackgroundVariant,
  ConnectionMode,
  type Node,
  type NodeProps,
  type NodeTypes
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { useFlow } from '../../hooks/useFlowContext';
import DynamicNode from '../DynamicNode';
import ContextMenu from './ContextMenu';
import type { NodeData } from '../../types';

const nodeTypes: NodeTypes = { dynamic: DynamicNode as ComponentType<NodeProps<Node<NodeData>>> };

// === Toolbar icons ===
const FitIcon = () => (
  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4" />
  </svg>
);
const MapIcon = () => (
  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7" />
  </svg>
);
const ZoomInIcon = () => (
  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
  </svg>
);
const ZoomOutIcon = () => (
  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M20 12H4" />
  </svg>
);

// === Bottom floating toolbar ===
function BottomToolbar() {
  const { fitView, zoomIn, zoomOut } = useReactFlow();
  const { zoom } = useViewport();
  const [showMap, setShowMap] = useState(false);

  return (
    <div className="flex flex-col items-end gap-2 pointer-events-auto">
      {/* Minimap */}
      <div
        className="rounded-[var(--radius-lg)] overflow-hidden border transition-all duration-300"
        style={{
          width: showMap ? 220 : 0,
          height: showMap ? 154 : 0,
          opacity: showMap ? 1 : 0,
          borderColor: 'var(--color-border-subtle)',
          boxShadow: 'var(--shadow-floating)',
          backgroundColor: 'var(--color-bg-surface)',
        }}
      >
        {showMap && (
          <MiniMap
            className="!relative !m-0 !w-full !h-full"
            nodeColor="var(--color-accent)"
            maskColor="rgba(0,0,0,0.4)"
            nodeStrokeWidth={0}
            zoomable
            pannable
          />
        )}
      </div>

      {/* Controls pill */}
      <div
        className="flex items-center rounded-full gap-0.5 border transition-all duration-200"
        style={{
          backgroundColor: 'var(--color-bg-surface)',
          borderColor: 'var(--color-border-subtle)',
          boxShadow: 'var(--shadow-floating)',
          padding: '4px',
          opacity: 0.85,
        }}
        onMouseEnter={e => (e.currentTarget.style.opacity = '1')}
        onMouseLeave={e => (e.currentTarget.style.opacity = '0.85')}
      >
        <button
          onClick={() => setShowMap(!showMap)}
          className="w-8 h-8 flex items-center justify-center rounded-full transition-colors duration-150"
          style={showMap ? { backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent)' } : { color: 'var(--color-text-secondary)' }}
          title="Toggle Minimap"
        >
          <MapIcon />
        </button>

        <div className="w-px h-4 mx-0.5" style={{ backgroundColor: 'var(--color-border-subtle)' }} />

        <button
          onClick={() => zoomOut()}
          className="w-8 h-8 flex items-center justify-center rounded-full transition-colors duration-150"
          style={{ color: 'var(--color-text-secondary)' }}
          title="Zoom Out"
        >
          <ZoomOutIcon />
        </button>

        <span
          className="text-[10px] font-mono font-medium min-w-[2.5rem] text-center cursor-pointer select-none transition-colors duration-150"
          style={{ color: 'var(--color-text-muted)' }}
          onClick={() => fitView({ duration: 300 })}
          title="Fit View"
        >
          {(zoom * 100).toFixed(0)}%
        </span>

        <button
          onClick={() => zoomIn()}
          className="w-8 h-8 flex items-center justify-center rounded-full transition-colors duration-150"
          style={{ color: 'var(--color-text-secondary)' }}
          title="Zoom In"
        >
          <ZoomInIcon />
        </button>

        <div className="w-px h-4 mx-0.5" style={{ backgroundColor: 'var(--color-border-subtle)' }} />

        <button
          onClick={() => fitView({ duration: 300 })}
          className="w-8 h-8 flex items-center justify-center rounded-full transition-colors duration-150"
          style={{ color: 'var(--color-text-secondary)' }}
          title="Fit View"
        >
          <FitIcon />
        </button>
      </div>
    </div>
  );
}

// === Empty canvas state ===
function EmptyCanvas() {
  return (
    <div
      className="absolute inset-0 flex items-center justify-center pointer-events-none"
      style={{ zIndex: 1 }}
    >
      <div className="flex flex-col items-center gap-4 text-center pointer-events-auto">
        {/* Logo */}
        <div
          className="w-16 h-16 rounded-2xl flex items-center justify-center text-white font-bold text-xl mb-2"
          style={{ backgroundColor: 'var(--color-accent)', boxShadow: 'var(--shadow-elevated)' }}
        >
          WF
        </div>
        <div>
          <div className="text-[14px] font-semibold mb-1" style={{ color: 'var(--color-text-primary)' }}>
            WorkFlow Editor
          </div>
          <div className="text-[12px]" style={{ color: 'var(--color-text-muted)' }}>
            Right-click on the canvas or drag nodes from the sidebar to start
          </div>
        </div>
        <div className="flex items-center gap-3 mt-2">
          {[
            { icon: '→', label: 'Right-click canvas' },
            { icon: '⊞', label: 'Drag from sidebar' },
          ].map(item => (
            <div
              key={item.label}
              className="flex items-center gap-2 px-3 py-1.5 rounded-full border text-[11px]"
              style={{
                backgroundColor: 'var(--color-bg-surface)',
                borderColor: 'var(--color-border-subtle)',
                color: 'var(--color-text-secondary)',
                boxShadow: 'var(--shadow-panel)',
              }}
            >
              <span>{item.icon}</span>
              <span>{item.label}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// === Main FlowEditor ===
export default function FlowEditor() {
  const {
    nodes, edges,
    onNodesChange, onEdgesChange,
    onConnect,
    theme,
    isConsoleOpen,
    addNodeAt,
    isValidConnection,
    onConnectStart, onConnectEnd,
    isExecutionLocked,
  } = useFlow();

  const { screenToFlowPosition } = useReactFlow();
  const [menu, setMenu] = useState<{ x: number; y: number; flowPos: { x: number; y: number } } | null>(null);

  const onPaneContextMenu = useCallback(
    (event: React.MouseEvent | globalThis.MouseEvent) => {
      event.preventDefault();
      const nativeEvent = (event as React.MouseEvent).nativeEvent || event;
      setMenu({ x: nativeEvent.clientX, y: nativeEvent.clientY, flowPos: screenToFlowPosition({ x: nativeEvent.clientX, y: nativeEvent.clientY }) });
    },
    [screenToFlowPosition]
  );

  const onPaneClick = useCallback(() => setMenu(null), []);

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    const type = event.dataTransfer.getData('application/reactflow');
    if (!type) return;
    const position = screenToFlowPosition({ x: event.clientX, y: event.clientY });
    addNodeAt(type, position);
  }, [screenToFlowPosition, addNodeAt]);

  const bgVariant = theme === 'dark' ? BackgroundVariant.Dots : BackgroundVariant.Lines;

  return (
    <div
      className="flex-1 h-full w-full relative"
      style={{ backgroundColor: 'var(--color-bg-canvas)' }}
      onDrop={onDrop}
      onDragOver={onDragOver}
      onContextMenu={onPaneContextMenu}
    >
      {/* Empty state */}
      {nodes.length === 0 && <EmptyCanvas />}

      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onConnectStart={onConnectStart}
        onConnectEnd={onConnectEnd}
        nodeTypes={nodeTypes}
        defaultEdgeOptions={{
          type: 'default',
          animated: false,
          style: { strokeWidth: 1.5, stroke: 'var(--color-edge)' },
        }}
        connectionMode={ConnectionMode.Loose}
        defaultViewport={{ x: 0, y: 0, zoom: 1.0 }}
        isValidConnection={isValidConnection}
        connectionLineStyle={{ stroke: 'var(--color-accent)', strokeWidth: 2, opacity: 0.8 }}
        selectNodesOnDrag={false}
        panOnScroll={true}
        zoomOnScroll={true}
        panOnDrag={[0, 1]}
        selectionOnDrag={false}
        minZoom={0.1}
        maxZoom={3}
        colorMode={theme === 'dark' ? 'dark' : 'light'}
        onPaneContextMenu={onPaneContextMenu}
        onPaneClick={onPaneClick}
        nodesDraggable={!isExecutionLocked}
        nodesConnectable={!isExecutionLocked}
        edgesReconnectable={!isExecutionLocked}
        elementsSelectable={!isExecutionLocked}
        deleteKeyCode={isExecutionLocked ? null : ['Backspace', 'Delete']}
      >
        <Background
          variant={bgVariant}
          gap={24}
          size={1}
          color={theme === 'dark' ? 'var(--color-canvas-grid-minor)' : 'var(--color-canvas-grid-minor)'}
          style={{ backgroundColor: 'var(--color-bg-canvas)' }}
        />

        <Panel
          position="bottom-right"
          className="mr-3 transition-all duration-300"
          style={{ marginBottom: isConsoleOpen ? 220 : 48 }}
        >
          <BottomToolbar />
        </Panel>
      </ReactFlow>

      {menu && (
        <ContextMenu
          top={menu.y}
          left={menu.x}
          onClose={() => setMenu(null)}
          onAddNode={(type) => {
            if (isExecutionLocked) return;
            addNodeAt(type, menu.flowPos);
          }}
        />
      )}
    </div>
  );
}
