import { useState, useCallback, useEffect, useRef, type ComponentType } from 'react';
import { ReactFlow, Background, MiniMap, Panel, useReactFlow, useNodesInitialized, BackgroundVariant,
  ConnectionMode, SelectionMode, type Node, type NodeProps, type NodeTypes } from '@xyflow/react';
import { Workflow, MousePointer2, MoveRight } from 'lucide-react';
import '@xyflow/react/dist/style.css';
import { useFlow } from '../../hooks/useFlowContext';
import DynamicNode from '../DynamicNode';
import ContextMenu from './ContextMenu';
import CanvasToolbar from './CanvasToolbar';
import type { NodeData } from '../../types';

const nodeTypes: NodeTypes = { dynamic: DynamicNode as ComponentType<NodeProps<Node<NodeData>>> };

export default function FlowEditor() {
  const { nodes, edges, onNodesChange, onEdgesChange, onConnect, theme, addNodeAt, isValidConnection,
    onConnectStart, onConnectEnd, isExecutionLocked, isRecoveryGraphReadOnly, recoveredGraphView, activeWorkflowDocumentId } = useFlow();
  const { screenToFlowPosition, fitView } = useReactFlow();
  const nodesInitialized = useNodesInitialized();
  const fittedDocumentRef = useRef<string | null>(null);
  const [mode, setMode] = useState<'pan' | 'select'>('pan');
  const [showMap, setShowMap] = useState(false);
  const [menu, setMenu] = useState<{ x: number; y: number; flowPos: { x: number; y: number } } | null>(null);
  const onPaneContextMenu = useCallback((event: React.MouseEvent | globalThis.MouseEvent) => {
    event.preventDefault();
    if (isExecutionLocked) return;
    const native = (event as React.MouseEvent).nativeEvent || event;
    setMenu({ x: native.clientX, y: native.clientY, flowPos: screenToFlowPosition({ x: native.clientX, y: native.clientY }) });
  }, [isExecutionLocked, screenToFlowPosition]);
  const onDrop = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    if (isExecutionLocked) return;
    const type = event.dataTransfer.getData('application/reactflow');
    if (type) addNodeAt(type, screenToFlowPosition({ x: event.clientX, y: event.clientY }));
  }, [screenToFlowPosition, addNodeAt, isExecutionLocked]);
  const documentKey = recoveredGraphView?.recoveryDirectory || activeWorkflowDocumentId;
  useEffect(() => {
    if (!nodesInitialized || nodes.length === 0 || fittedDocumentRef.current === documentKey) return;
    const frame = requestAnimationFrame(() => {
      fittedDocumentRef.current = documentKey;
      void fitView({ padding: .16, maxZoom: 1, duration: 200 });
    });
    return () => cancelAnimationFrame(frame);
  }, [nodesInitialized, nodes.length, documentKey, fitView]);

  return <div className="workspace-canvas" data-tool={mode}
    onDrop={onDrop} onDragOver={event => { event.preventDefault(); event.dataTransfer.dropEffect = 'move'; }} onContextMenu={onPaneContextMenu}>
    {nodes.length === 0 && <div className="canvas-empty"><span className="canvas-empty__icon"><Workflow size={30} /></span>
      <h1>Build your workflow</h1><p>Add a node from the library, connect the steps,<br />and run your image processing pipeline.</p>
      <div><span><MousePointer2 size={13} />Right-click to add nodes</span><MoveRight size={14} /><span>Connect & run</span></div>
    </div>}
    {isRecoveryGraphReadOnly && recoveredGraphView && <div className="canvas-recovery-note"><strong>Recovery graph · Read only</strong><span>Nodes can move; parameters and connections are locked.</span><code title={recoveredGraphView.recoveryDirectory}>{recoveredGraphView.recoveryDirectory}</code></div>}
    <ReactFlow nodes={nodes} edges={edges} onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
      onConnect={onConnect} onConnectStart={onConnectStart} onConnectEnd={onConnectEnd} nodeTypes={nodeTypes}
      defaultEdgeOptions={{ type: 'default', animated: false, style: { strokeWidth: 1.5, stroke: 'var(--color-edge)' } }}
      connectionMode={ConnectionMode.Loose} defaultViewport={{ x: 0, y: 0, zoom: 1 }} isValidConnection={isValidConnection}
      connectionLineStyle={{ stroke: 'var(--color-accent)', strokeWidth: 2 }}
      selectNodesOnDrag={false} panOnScroll={false} zoomOnScroll panOnDrag={mode === 'pan' ? [0, 1] : [1]}
      selectionOnDrag={mode === 'select'} selectionMode={SelectionMode.Partial} panActivationKeyCode="Space"
      minZoom={.1} maxZoom={3} colorMode={theme} onPaneContextMenu={onPaneContextMenu} onPaneClick={() => setMenu(null)}
      nodesDraggable nodesConnectable={!isExecutionLocked} edgesReconnectable={!isExecutionLocked} elementsSelectable
      deleteKeyCode={isExecutionLocked ? null : ['Backspace', 'Delete']}>
      <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="var(--color-canvas-grid-minor)" />
      {showMap && <MiniMap position="bottom-right" style={{ width: 192, height: 128, bottom: 66, right: 16, margin: 0 }}
        nodeColor="var(--color-border-strong)" maskColor="var(--color-bg-overlay)" zoomable pannable />}
      <Panel position="bottom-left" className="canvas-hint">{mode === 'pan' ? 'Drag to pan' : 'Drag to select'}<span>·</span>Scroll to zoom<span>·</span>Space to pan</Panel>
      <Panel position="bottom-right" className="canvas-tools-panel"><CanvasToolbar mode={mode} setMode={setMode} showMap={showMap} toggleMap={() => setShowMap(current => !current)} /></Panel>
    </ReactFlow>
    {menu && <ContextMenu top={menu.y} left={menu.x} onClose={() => setMenu(null)} onAddNode={type => {
      if (!isExecutionLocked) addNodeAt(type, menu.flowPos);
    }} />}
  </div>;
}
