import { useState } from 'react';
import type { Node, Edge } from '@xyflow/react';
import { X, RotateCcw, Unplug, Plug, ArrowUp, ArrowDown, Trash2, LogIn } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';
import { usePanelResize } from '../../hooks/usePanelResize';
import type { NodeData, SubgraphPort } from '../../types';
import { bypassMapping, editSubgraphPort, exposePort, graphAtPath, updateGraphAtPath } from '../../utils/subgraphs';
import { inputDefault, inputPortType, isWidgetInput, nodeInputValues, withNodeInputValue } from '../../utils/nodeInputs';
import { resolveNodeOutputTypes } from '../../utils/portTypes';
import { inputMeta, isInputVisible } from '../../utils/inputVisibility';
import { NodeParameter } from './NodeParameter';
import NodeColorPicker from './NodeColorPicker';
import './inspector.css';
import { isGroup, groupContents, fitGroup } from '../../utils/groups';

function EditableLabel({ value, label, disabled, onChange }: { value: string; label: string; disabled: boolean; onChange: (name: string) => void }) {
  return <input key={value} className="inspector-name" aria-label={label} title="修改显示名称，参数标识保持不变" defaultValue={value} disabled={disabled}
    onBlur={event => { const next = event.target.value.trim(); if (next && next !== value) onChange(next); else event.target.value = value; }}
    onKeyDown={event => { if (event.key === 'Enter') event.currentTarget.blur(); if (event.key === 'Escape') { event.currentTarget.value = value; event.currentTarget.blur(); } }} />;
}

function FieldName({ value, label }: { value: string; label: string }) {
  return <span className="inspector-field-name" aria-label={label} title={`${value} · 字段名称暂不支持修改`}>{value}</span>;
}

function InputFields({ node, edges, path = [], promote, search = '' }: { node: Node<NodeData>; edges: Edge[]; path?: string[]; promote: boolean; search?: string }) {
  const { nodes, editGraph, isExecutionLocked: locked, addLog } = useFlow();
  const change = (update: (node: Node<NodeData>) => Node<NodeData>) => editGraph(graph => updateGraphAtPath(graph, path, inner => ({ ...inner, nodes: inner.nodes.map(n => n.id === node.id ? update(n) : n) })));
  const definitions = { ...node.data.nodeSpec.input.required, ...node.data.nodeSpec.input.optional };
  const values = nodeInputValues(node, { nodes: graphAtPath({ nodes, edges }, path).nodes, edges });
  return <>{Object.entries(definitions).filter(([name, config]) => isInputVisible(inputMeta(config), values) && `${name} ${node.data.inputLabels?.[name] || ''}`.toLowerCase().includes(search.toLowerCase())).map(([name, config]) => {
    const connection = edges.find(e => e.target === node.id && e.targetHandle === name);
    const display = node.data.inputLabels?.[name] || name;
    return <section className="inspector-field" key={name} data-input={name}>
      <div className="inspector-field__heading"><FieldName value={display} label={`输入名称 ${name}`} />
        <code>{inputPortType(config)}</code></div>
      {isWidgetInput(config) && <NodeParameter name={name} label={display} config={config} value={values[name]} disabled={locked || !!connection} linked={!!connection}
        onChange={(_, value) => change(n => withNodeInputValue(n, name, value))} />}
      <div className="inspector-field__actions"><small>{connection ? '已连接 · 使用上游值' : isWidgetInput(config) ? '未连接时使用此值' : '等待上游连接'}{node.data.nodeSpec.input.optional?.[name] ? ' · 高级输入' : ''}</small>
        {isWidgetInput(config) && <button title="恢复默认值" aria-label={`重置 ${name}`} disabled={locked || !!connection} onClick={() => change(n => withNodeInputValue(n, name, inputDefault(config)))}><RotateCcw size={14} /></button>}
        {connection && <button title="断开输入连接" aria-label={`断开 ${name}`} disabled={locked} onClick={() => editGraph(graph => updateGraphAtPath(graph, path, inner => ({ ...inner, edges: inner.edges.filter(e => e.id !== connection.id) })))}><Unplug size={14} /></button>}
        {promote && <button title={connection ? '请先断开此输入，再公开为子图参数' : '公开为子图输入，父图也可编辑或连线'} aria-label={`公开输入 ${name}`} disabled={locked || !!connection}
          onClick={() => { try { editGraph(graph => updateGraphAtPath(graph, path, inner => exposePort(inner, node.id, 'input', name))); } catch (error) { addLog((error as Error).message, 'error'); } }}><Plug size={14} /></button>}
      </div>
    </section>;
  })}</>;
}

export default function NodeInspector() {
  const { nodes, edges, subgraphPath, isNodeInspectorOpen, closeNodeInspector, isExecutionLocked: locked, updateNodeData, editGraph, enterSubgraph, addLog } = useFlow();
  const [tab, setTab] = useState<'parameters' | 'info' | 'settings'>('parameters');
  const [search, setSearch] = useState('');
  const resize = usePanelResize('x', 360, 300, 560, -1);
  const selected = nodes.filter(n => n.selected);
  const node = selected.length === 1 ? selected[0] : undefined;
  const sub = node?.data.subgraph;
  if (!isNodeInspectorOpen) return null;
  const safe = (action: () => void) => { try { action(); } catch (error) { addLog((error as Error).message, 'error'); } };
  const matches = (text: string) => text.toLowerCase().includes(search.toLowerCase());
  const portEditor = (port: SubgraphPort, kind: 'input' | 'output', index: number, count: number) => {
    if (!node || !sub || !matches(port.name)) return null;
    const action = (change: Parameters<typeof editSubgraphPort>[4]) => safe(() => editGraph(graph => editSubgraphPort(graph, node.id, kind, port.id, change)));
    const linked = kind === 'input' && edges.some(e => e.target === node.id && e.targetHandle === port.id);
    return <section className="inspector-field" key={port.id} data-port={port.id}>
      <div className="inspector-field__heading"><FieldName value={port.name} label={`${kind === 'input' ? '输入' : '输出'}名称 ${port.id}`} /><code>{port.type}</code></div>
      {kind === 'input' && isWidgetInput(port.config) && <NodeParameter name={port.id} label={port.name} config={port.config!} value={nodeInputValues(node, { nodes, edges })[port.id]} disabled={locked || linked} linked={linked}
        onChange={(_, value) => action({ value })} />}
      <div className="inspector-field__actions"><small>{linked ? '已连接 · 使用上游值' : kind === 'input' && port.config ? '父图与子图共享此默认值' : '子图接口'}</small>
        {linked && <button title="断开外部连接" disabled={locked} onClick={() => editGraph(graph => ({ ...graph, edges: graph.edges.filter(e => e.target !== node.id || e.targetHandle !== port.id) }))}><Unplug size={14} /></button>}
        <button title="上移接口" disabled={locked || index === 0} onClick={() => action({ move: -1 })}><ArrowUp size={14} /></button>
        <button title="下移接口" disabled={locked || index === count - 1} onClick={() => action({ move: 1 })}><ArrowDown size={14} /></button>
        <button title="移除接口及相关连线" aria-label={`移除接口 ${port.name}`} disabled={locked} onClick={() => action({ remove: true })}><Trash2 size={14} /></button>
      </div>
    </section>;
  };
  return <aside className="node-inspector" aria-label="节点详情" style={{ width: resize.size }}>
    <div className="node-inspector__resize" {...resize.separatorProps} aria-label="调整节点面板宽度" />
    <header><h2>{node?.data.title || node?.data.nodeSpec.display_name || (selected.length ? `已选择 ${selected.length} 个节点` : '节点详情')}</h2><button aria-label="关闭节点面板" onClick={closeNodeInspector}><X size={18} /></button></header>
    <nav aria-label="节点面板标签">{([['parameters', '参数'], ['info', '信息'], ['settings', '设置']] as const).map(([id, title]) => <button role="tab" aria-selected={tab === id} key={id} onClick={() => setTab(id)}>{title}</button>)}</nav>
    <div className="node-inspector__body">
      {!selected.length ? <p className="inspector-empty">选择画布上的节点，查看参数、输入输出和设置。</p> : <>
        {locked && <p className="inspector-note">当前为只读，可查看本次配置。</p>}
        {tab === 'parameters' && (!node ? <p className="inspector-empty">选择一个节点以编辑参数。可在设置中批量调整颜色。</p> : <>
          <input className="inspector-search" type="search" aria-label="搜索节点参数" placeholder="搜索参数或接口…" value={search} onChange={e => setSearch(e.target.value)} />
          {isGroup(node) ? <>
            <p className="inspector-note">组只整理画布，不改变依赖和任务执行。拖动标题移动组内节点，按住 Alt 仅移动框。删除组会保留内部节点。</p>
            <p>{groupContents(node, nodes).filter(n => !isGroup(n)).length} 个成员 · {Math.round(node.data.group!.width)} × {Math.round(node.data.group!.height)}</p>
            <button className="inspector-enter" disabled={locked} onClick={() => editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => n.id === node.id ? fitGroup(n, graph.nodes) : n) }))}>组框适应内部节点</button>
          </> : sub ? <>
            <button className="inspector-enter" onClick={() => enterSubgraph(node.id)}><LogIn size={15} />进入子图</button>
            <h3>输入</h3>{sub.inputs.length ? sub.inputs.map((p, i) => portEditor(p, 'input', i, sub.inputs.length)) : <p className="inspector-note">进入子图，将“新增输入”的圆点连接到内部参数。</p>}
            <h3>输出</h3>{sub.outputs.map((p, i) => portEditor(p, 'output', i, sub.outputs.length))}
            <details className="inspector-advanced"><summary>高级参数 · 内部节点</summary>{sub.nodes.filter(n => !n.data.boundary && !isGroup(n) && matches(`${n.data.title || n.data.nodeSpec.display_name} ${Object.keys({ ...n.data.nodeSpec.input.required, ...n.data.nodeSpec.input.optional }).join(' ')}`)).map(child => <details key={child.id}><summary>{child.data.title || child.data.nodeSpec.display_name}</summary><InputFields node={child} edges={sub.edges} path={[node.id]} promote /></details>)}</details>
          </> : <>
            <h3>{node.data.boundary ? '子图接口' : '输入'}</h3>
            {node.data.boundary && <FieldName value={node.data.title || '接口'} label="接口名称" />}
            <div className="inspector-inputs"><InputFields node={node} edges={edges} search={search} promote={subgraphPath.length > 0 && !node.data.boundary} /></div>
            {node.data.boundary === 'input' && !node.data.boundaryConfig && <p className="inspector-note">此接口传递 {String(node.data.values.portType)}，请从父图连接数据。</p>}
            <h3>输出</h3>{resolveNodeOutputTypes(node.data.nodeSpec, nodeInputValues(node, { nodes, edges })).map((type, index) => matches(node.data.outputLabels?.[index] || node.data.nodeSpec.output_name?.[index] || type) && <section className="inspector-field" key={index}>
              <div className="inspector-field__heading"><FieldName value={node.data.outputLabels?.[index] || node.data.nodeSpec.output_name?.[index] || type} label={`输出名称 ${index}`} /><code>{type}</code></div>
              {subgraphPath.length > 0 && node.data.boundary !== 'output' && <button className="inspector-link" disabled={locked} onClick={() => safe(() => editGraph(graph => exposePort(graph, node.id, 'output', String(index))))}><Plug size={14} />公开为子图输出</button>}
            </section>)}
          </>}
        </>)}
        {tab === 'info' && selected.map(n => <section className="inspector-info" key={n.id}><h3>{n.data.title || n.data.nodeSpec.display_name}</h3><p>{n.data.nodeSpec.description || (n.data.subgraph ? '可进入编辑的子图，接口连接到内部节点。' : '此节点由插件提供。')}</p>
          <dl><dt>类型</dt><dd>{n.data.opType}</dd><dt>分类</dt><dd>{n.data.nodeSpec.category || '—'}</dd><dt>ID</dt><dd>{n.id}</dd><dt>运行状态</dt><dd>{n.data.runState || '未运行'}</dd>{n.data.nodeSpec.required_worker_profile && <><dt>Worker 类型</dt><dd>{n.data.nodeSpec.required_worker_profile}</dd></>}</dl>
          <p className="inspector-note">输入输出类型由节点接口声明；字段名称暂不支持修改。</p></section>)}
        {tab === 'settings' && <>
          {node && <><h3>{node.data.boundary ? '接口名称' : '节点名称'}</h3>{node.data.boundary
            ? <FieldName value={node.data.title || node.data.nodeSpec.display_name} label="接口名称" />
            : <EditableLabel value={node.data.title || node.data.nodeSpec.display_name} label="节点名称" disabled={locked} onChange={title => updateNodeData(node.id, { title })} />}</>}
          <h3>节点颜色</h3><NodeColorPicker color={node?.data.color} disabled={locked} onChange={color => editGraph(graph => ({ ...graph, nodes: graph.nodes.map(n => n.selected ? { ...n, data: { ...n.data, color } } : n) }))} />
          {node && !sub && !node.data.boundary && !isGroup(node) && <label className="inspector-checkbox"><input type="checkbox" checked={!!node.data.showAdvanced} disabled={locked} onChange={event => updateNodeData(node.id, { showAdvanced: event.target.checked })} />显示高级输入</label>}
          {node && !node.data.boundary && !isGroup(node) && <label className="inspector-checkbox"><input type="checkbox" checked={!!node.data.bypassed} disabled={locked || !bypassMapping(node)} onChange={event => updateNodeData(node.id, { bypassed: event.target.checked })} />忽略节点（Bypass）</label>}
          {node && !bypassMapping(node) && !node.data.boundary && !isGroup(node) && <p className="inspector-note">只有具备明确输入输出对应关系的节点支持忽略。</p>}
        </>}
      </>}
    </div>
  </aside>;
}
