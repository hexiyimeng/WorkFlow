import type { NodeSpec } from '../types';

type InputMeta = Record<string, unknown> | undefined;
type InputConfig = [string | string[], InputMeta?];

export function inputMeta(config: unknown): InputMeta {
  if (!Array.isArray(config)) return undefined;
  const maybeMeta = config[1];
  return maybeMeta && typeof maybeMeta === 'object' && !Array.isArray(maybeMeta)
    ? maybeMeta as InputMeta
    : undefined;
}

export function valuesWithInputDefaults(
  nodeSpec: NodeSpec | undefined,
  values: Record<string, unknown>,
): Record<string, unknown> {
  const resolved: Record<string, unknown> = {};
  for (const [name, config] of nodeInputEntries(nodeSpec)) {
    const [rawType, meta] = config;
    if (meta?.default !== undefined) {
      resolved[name] = meta.default;
    } else if (Array.isArray(rawType)) {
      resolved[name] = rawType[0];
    }
  }
  return { ...resolved, ...values };
}

export function isInputVisible(inputMetaValue: InputMeta, values: Record<string, unknown>): boolean {
  const visibleWhen = inputMetaValue?.visible_when;
  if (!visibleWhen || typeof visibleWhen !== 'object' || Array.isArray(visibleWhen)) return true;

  for (const [key, expected] of Object.entries(visibleWhen as Record<string, unknown>)) {
    const current = values[key];
    if (Array.isArray(expected)) {
      if (!expected.includes(current)) return false;
    } else if (current !== expected) {
      return false;
    }
  }
  return true;
}

export function nodeInputEntries(nodeSpec: NodeSpec | undefined): [string, InputConfig][] {
  if (!nodeSpec) return [];
  const allInputs = { ...(nodeSpec.input?.required || {}), ...(nodeSpec.input?.optional || {}) };
  return Object.entries(allInputs) as [string, InputConfig][];
}

export function visibleNodeInputNames(nodeSpec: NodeSpec | undefined, values: Record<string, unknown>): Set<string> {
  const resolvedValues = valuesWithInputDefaults(nodeSpec, values);
  return new Set(
    nodeInputEntries(nodeSpec)
      .filter(([, config]) => isInputVisible(inputMeta(config), resolvedValues))
      .map(([name]) => name),
  );
}
