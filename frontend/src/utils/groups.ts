import type { Node, NodeChange } from '@xyflow/react';
import type { NodeData, NodeSpec } from '../types';

export const GROUP_SPEC: NodeSpec = { type: '__group__', display_name: '组', category: '布局',
  description: '组用于整理画布，移动标题会带动组内节点；删除组只移除框，不影响节点和执行。',
  input: { required: {} }, output: [] };
export const isGroup = (node: Pick<Node<NodeData>, 'data'>) => !!node.data.group;
export function nodeRect(node: Node<NodeData>) {
  return { ...node.position, width: node.data.group?.width ?? node.measured?.width ?? node.width ?? 330,
    height: node.data.group?.height ?? node.measured?.height ?? node.height ?? 200 };
}

/** Spatial membership, like ComfyUI: node centres inside; nested frames fully inside. */
export function groupContents(group: Node<NodeData>, nodes: Node<NodeData>[]): Node<NodeData>[] {
  const box = nodeRect(group);
  return nodes.filter(node => {
    if (node.id === group.id) return false;
    const r = nodeRect(node);
    if (isGroup(node)) return r.x >= box.x && r.y >= box.y && r.x + r.width <= box.x + box.width
      && r.y + r.height <= box.y + box.height && (r.width < box.width || r.height < box.height);
    return r.x + r.width / 2 >= box.x && r.x + r.width / 2 <= box.x + box.width
      && r.y + r.height / 2 >= box.y && r.y + r.height / 2 <= box.y + box.height;
  });
}
export function selectionWithContents(nodes: Node<NodeData>[]): Node<NodeData>[] {
  const ids = new Set(nodes.filter(n => n.selected).map(n => n.id));
  for (const group of nodes.filter(n => n.selected && isGroup(n))) {
    for (const member of groupContents(group, nodes)) ids.add(member.id);
  }
  return nodes.filter(n => ids.has(n.id));
}
function bounds(items: Node<NodeData>[]) {
  const rects = items.map(nodeRect);
  const x = Math.min(...rects.map(r => r.x)) - 30, y = Math.min(...rects.map(r => r.y)) - 60;
  return { x, y, width: Math.max(240, Math.max(...rects.map(r => r.x + r.width)) - x + 30),
    height: Math.max(160, Math.max(...rects.map(r => r.y + r.height)) - y + 30) };
}
export function createGroup(nodes: Node<NodeData>[], position = { x: 0, y: 0 }): Node<NodeData>[] {
  const selected = selectionWithContents(nodes);
  const box = selected.length ? bounds(selected) : { ...position, width: 640, height: 400 };
  return [...nodes.map(n => ({ ...n, selected: false })), {
    id: `group-${crypto.randomUUID()}`, type: 'groupFrame', position: { x: box.x, y: box.y }, selected: true,
    data: { opType: '__group__', nodeSpec: GROUP_SPEC, title: '新建组', color: '#597da3', values: {},
      group: { width: box.width, height: box.height } },
  }];
}
export function fitGroup(group: Node<NodeData>, nodes: Node<NodeData>[]): Node<NodeData> {
  const contents = groupContents(group, nodes);
  if (!contents.length) return group;
  const box = bounds(contents);
  return { ...group, position: { x: box.x, y: box.y }, data: { ...group.data, group: { width: box.width, height: box.height } } };
}

/** Freeze membership at drag start; moving across a node must not pick it up. */
export function groupDragSnapshot(nodes: Node<NodeData>[], draggedId: string, frameOnly: boolean) {
  const dragged = nodes.find(n => n.id === draggedId);
  const moving = nodes.filter(n => n.id === draggedId || (dragged?.selected && n.selected));
  return { nodes, groups: frameOnly ? [] : moving.filter(isGroup).map(group => ({
    id: group.id, members: groupContents(group, nodes).map(n => n.id),
  })) };
}
export type GroupDrag = ReturnType<typeof groupDragSnapshot>;
export function expandGroupDrag(changes: NodeChange<Node<NodeData>>[], snapshot: GroupDrag | null): NodeChange<Node<NodeData>>[] {
  if (!snapshot?.groups.length) return changes;
  const result = [...changes];
  const moved = new Set(changes.filter(c => c.type === 'position').map(c => c.id));
  for (const group of snapshot.groups) {
    const change = changes.find(c => c.type === 'position' && c.id === group.id);
    const start = snapshot.nodes.find(n => n.id === group.id);
    if (change?.type !== 'position' || !change.position || !start) continue;
    for (const id of group.members) {
      if (moved.has(id)) continue;
      const member = snapshot.nodes.find(n => n.id === id)!;
      moved.add(id);
      result.push({ id, type: 'position', dragging: change.dragging,
        position: { x: member.position.x + change.position.x - start.position.x,
          y: member.position.y + change.position.y - start.position.y } });
    }
  }
  return result;
}
