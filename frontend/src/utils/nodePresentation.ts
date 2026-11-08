import type { CSSProperties } from 'react';

export const NODE_COLORS = [
  { name: '默认', value: undefined }, { name: '蓝色', value: '#779ac3' },
  { name: '绿色', value: '#6fa795' }, { name: '黄色', value: '#c59b62' },
  { name: '紫色', value: '#a08bd5' }, { name: '红色', value: '#cd7f85' },
  { name: '灰色', value: '#888888' },
] as const;

/** Shared by ordinary, subgraph and interface nodes, in both themes. */
export function nodeColorStyle(color?: string, category = '#779ac3'): CSSProperties {
  const valid = color && /^#[0-9a-fA-F]{6}$/.test(color) ? color : undefined;
  return {
    '--node-category': valid || category,
    ...(valid ? {
      '--node-heading': `color-mix(in srgb, ${valid} 38%, var(--node-base-heading))`,
      '--node-surface': `color-mix(in srgb, ${valid} 12%, var(--node-base-surface))`,
      '--node-outline': valid,
    } : {}),
  } as CSSProperties;
}
