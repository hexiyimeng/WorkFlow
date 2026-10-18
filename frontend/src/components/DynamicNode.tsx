import { memo, useCallback, useMemo, useState, useRef, useLayoutEffect, type CSSProperties } from 'react';
import { Handle, Position, NodeResizer, useNodeConnections, useUpdateNodeInternals, type NodeProps, type Node } from '@xyflow/react';
import { ChevronDown, Database, ScanLine, SlidersHorizontal, Save } from 'lucide-react';
import type { NodeData } from '../types';
import { useFlow } from '../hooks/useFlowContext';
import { parsePortType, resolveNodeOutputTypes } from '../utils/portTypes';
import { inputMeta, isInputVisible, valuesWithInputDefaults } from '../utils/inputVisibility';
import { NodeParameter } from './nodes/NodeParameter';

const TYPE_COLORS: Record<string, string> = {
  ZARR_HANDLE: '#d7ad66', IMAGE: '#78a9e2', INT: '#88bc9a', FLOAT: '#cea0cd',
  STRING: '#a4afc0', DATA_STREAM: '#af9cdb', METADATA: '#d59ba0', DASK_ARRAY: '#68bfc4',
};
function getTypeColor(type: string): string {
  return TYPE_COLORS[type] ?? TYPE_COLORS[parsePortType(type).container] ?? '#9aa5b5';
}
function categoryStyle(category = '', output = false) {
  if (output || /Output|Writer/.test(category)) return { color: '#a08bd5', Icon: Save };
  if (/Segmentation|Models/.test(category)) return { color: '#c59b62', Icon: ScanLine };
  if (/IO|Loader/.test(category)) return { color: '#6fa795', Icon: Database };
  return { color: '#779ac3', Icon: SlidersHorizontal };
}
const STATUS_LABELS: Record<string, string> = {
  ready: 'Ready', submitted: 'Queued', running: 'Running', done: 'Completed',
  failed: 'Failed', cancelled: 'Cancelled',
};

const DynamicNode = ({ id, data, selected }: NodeProps<Node<NodeData>>) => {
  const { nodeSpec, values = {}, message, _invalid, _warning, runState, waitingFor } = data;
  const { isExecutionLocked, updateNodeData } = useFlow();
  const locked = isExecutionLocked ?? false;
  const [collapsed, setCollapsed] = useState(false);
  const valuesRef = useRef(values);
  useLayoutEffect(() => { valuesRef.current = values; }, [values]);
  const effectiveValues = useMemo(() => valuesWithInputDefaults(nodeSpec, values), [nodeSpec, values]);
  const handleUpdate = useCallback((key: string, value: unknown) => {
    updateNodeData(id, { values: { ...valuesRef.current, [key]: value } });
  }, [id, updateNodeData]);
  const connections = useNodeConnections({ id });
  const updateNodeInternals = useUpdateNodeInternals();
  const connectedInputs = new Set(connections.filter(c => c.target === id).map(c => c.targetHandle));
  const connectedOutputs = new Set(connections.filter(c => c.source === id).map(c => c.sourceHandle));
  const { color, Icon } = categoryStyle(nodeSpec?.category, nodeSpec?.output_node);
  const title = nodeSpec?.display_name || data.opType || 'Unknown node';
  const status = runState ?? 'idle';
  const hasStatus = status !== 'idle' && status !== 'ready';
  const footerMessage = status === 'running' && waitingFor?.length
    ? `Waiting for ${waitingFor.join(', ')}`
    : status === 'done' ? '' : message || '';

  const { inputs, widgets, outputs } = useMemo(() => {
    const inputs: { name: string; type: string; color: string }[] = [];
    const widgets: { name: string; config: [string | string[], Record<string, unknown>?] }[] = [];
    if (nodeSpec) {
      const definitions = { ...nodeSpec.input?.required, ...nodeSpec.input?.optional };
      for (const [name, config] of Object.entries(definitions)) {
        if (!config || !isInputVisible(inputMeta(config), effectiveValues)) continue;
        const [type, options] = config as [string | string[], Record<string, unknown>?];
        if (Array.isArray(type) || ['INT', 'FLOAT', 'STRING', 'BOOLEAN', 'LONG'].includes(type)) {
          widgets.push({ name, config: [type, options] });
        } else if (typeof type === 'string') inputs.push({ name, type, color: getTypeColor(type) });
      }
    }
    const outputs = nodeSpec ? resolveNodeOutputTypes(nodeSpec, effectiveValues).map((type, index) => ({
      name: nodeSpec.output_name?.[index] ?? type, type, color: getTypeColor(type),
    })) : [];
    return { inputs, widgets, outputs };
  }, [nodeSpec, effectiveValues]);
  const portLayout = [...inputs.map(p => `${p.name}:${p.type}`), '|', ...outputs.map(p => p.type)].join(',');
  useLayoutEffect(() => { updateNodeInternals(id); }, [id, portLayout, collapsed, updateNodeInternals]);

  return (
    <>
      <NodeResizer color="var(--color-accent)" isVisible={Boolean(selected) && !collapsed && !locked}
        minWidth={300} minHeight={96} handleStyle={{ width: 7, height: 7 }} />
      <article className="workflow-node node-wrapper" data-node-id={id} data-status={status}
        data-selected={selected || undefined} data-invalid={_invalid || undefined}
        data-collapsed={collapsed || undefined} style={{ '--node-category': color } as CSSProperties}>
        <header className="workflow-node__header" title={nodeSpec?.description || `${title} · ${nodeSpec?.category || data.opType}`}
          onDoubleClick={event => { event.stopPropagation(); setCollapsed(current => !current); }}>
          <button type="button" className="workflow-node__collapse nodrag" aria-label={collapsed ? 'Expand node' : 'Collapse node'}
            aria-expanded={!collapsed} onDoubleClick={event => event.stopPropagation()}
            onClick={event => { event.stopPropagation(); setCollapsed(current => !current); }}>
            <ChevronDown size={14} />
          </button>
          <span className="workflow-node__category" aria-hidden="true"><Icon size={14} /></span>
          <h3 className="workflow-node__title">{title}</h3>
          {nodeSpec?.output_node && <span className="workflow-node__output" title="Output node" aria-label="Output node"><Save size={13} /></span>}
          {collapsed && hasStatus && <span className="node-status-dot" title={STATUS_LABELS[status]} />}
        </header>
        {_invalid && <div className="workflow-node__warning" role="alert">{_warning || 'Node type unavailable. Replace this node.'}</div>}
        {(inputs.length > 0 || outputs.length > 0) && (
          <div className="node-ports" aria-label="Node connections">
            <div className="node-ports__column">
              {inputs.map(input => <div key={input.name} className="node-port" title={`${input.name} · ${input.type}`}>
                <Handle type="target" position={Position.Left} id={input.name}
                  aria-label={`Input ${input.name} (${input.type})`}
                  className="node-port__handle node-port__handle--input"
                  data-connected={connectedInputs.has(input.name) || undefined}
                  style={{ '--port-color': input.color } as CSSProperties} />
                <span className="node-port__name">{input.name}</span>
              </div>)}
            </div>
            <div className="node-ports__column node-ports__column--outputs">
              {outputs.map((output, index) => <div key={index} className="node-port" title={`${output.name} · ${output.type}`}>
                <span className="node-port__name">{output.name}</span>
                <Handle type="source" position={Position.Right} id={String(index)}
                  aria-label={`Output ${output.name} (${output.type})`}
                  className="node-port__handle node-port__handle--output"
                  data-connected={connectedOutputs.has(String(index)) || undefined}
                  style={{ '--port-color': output.color } as CSSProperties} />
              </div>)}
            </div>
          </div>
        )}
        {!collapsed && widgets.length > 0 && (
          <div className="node-parameters" aria-label="Node parameters">
            {widgets.map(widget => <NodeParameter key={widget.name} {...widget}
              value={effectiveValues[widget.name]} onChange={handleUpdate} disabled={locked} />)}
          </div>
        )}
        {hasStatus && <footer className="workflow-node__status" role="status" title={message}>
          <span className="node-status-dot" />
          <span className="workflow-node__status-label">{STATUS_LABELS[status] || status}</span>
          {footerMessage && footerMessage !== STATUS_LABELS[status] && <span className="workflow-node__status-message">{footerMessage}</span>}
        </footer>}
      </article>
    </>
  );
};

export default memo(DynamicNode);
