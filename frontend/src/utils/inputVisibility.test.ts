import type { NodeSpec } from '../types';
import { visibleNodeInputNames } from './inputVisibility.ts';

function input(type: string | string[], meta?: Record<string, unknown>): [string | string[], Record<string, unknown>?] {
  return meta === undefined ? [type] : [type, meta];
}

const rechunkSpec: NodeSpec = {
  type: 'DaskRechunk',
  display_name: 'Dask Rechunk',
  category: 'WorkFlow/Dask',
  input: {
    required: {
      dask_arr: input('DASK_ARRAY[any]'),
      mode: input(['explicit', 'axis_index', 'axis_name', 'match_reference'], { default: 'explicit' }),
    },
    optional: {
      chunks: input('STRING', { default: '', visible_when: { mode: 'explicit' } }),
      axis_chunks: input('STRING', { default: '', visible_when: { mode: ['axis_index', 'axis_name'] } }),
      reference_arr: input('DASK_ARRAY[any]', { visible_when: { mode: 'match_reference' } }),
      axes: input('STRING', { default: '', visible_when: { mode: 'axis_name' } }),
    },
  },
  output: ['DASK_ARRAY[any]'],
};

function assertVisible(mode: string, expected: string[]) {
  const actual = [...visibleNodeInputNames(rechunkSpec, { mode })].sort();
  const wanted = [...expected].sort();
  if (actual.join('|') !== wanted.join('|')) {
    throw new Error(`mode=${mode} visible inputs ${actual.join(',')} did not match ${wanted.join(',')}`);
  }
}

assertVisible('explicit', ['dask_arr', 'mode', 'chunks']);
assertVisible('axis_index', ['dask_arr', 'mode', 'axis_chunks']);
assertVisible('axis_name', ['dask_arr', 'mode', 'axis_chunks', 'axes']);
assertVisible('match_reference', ['dask_arr', 'mode', 'reference_arr']);

const defaultModeVisible = [...visibleNodeInputNames(rechunkSpec, {})].sort();
if (defaultModeVisible.join('|') !== ['chunks', 'dask_arr', 'mode'].join('|')) {
  throw new Error(`default mode visible inputs ${defaultModeVisible.join(',')} did not match explicit mode`);
}
