import type { ExecutionPhase, ExecutionRuntimeState } from '../types.ts';

const LIVE_EXECUTION_PHASES: readonly ExecutionPhase[] = [
  'graph_building',
  'submitted',
  'running',
  'cancelling',
];

export const isLiveExecutionPhase = (phase: ExecutionPhase): boolean =>
  LIVE_EXECUTION_PHASES.includes(phase);

export const isExecutionStateUnresolved = (phase: ExecutionPhase): boolean =>
  phase === 'disconnected';

export const blocksExecutionChanges = (phase: ExecutionPhase): boolean =>
  isLiveExecutionPhase(phase) || isExecutionStateUnresolved(phase);

const LOCKED_NODE_LAYOUT_CHANGE_TYPES = new Set([
  'position',
  'dimensions',
  'select',
]);

/** Keep visual layout interactive while rejecting semantic graph mutations. */
export const filterLockedNodeChanges = <T extends { type: string }>(
  changes: readonly T[],
): T[] => changes.filter(change => LOCKED_NODE_LAYOUT_CHANGE_TYPES.has(change.type));

/** Edge selection is visual; every other edge change can mutate the graph. */
export const filterLockedEdgeChanges = <T extends { type: string }>(
  changes: readonly T[],
): T[] => changes.filter(change => change.type === 'select');

export const markExecutionConnectionLost = (
  state: ExecutionRuntimeState,
  executionId?: string | null,
): ExecutionRuntimeState => {
  const retainedExecutionId = executionId || state.executionId;
  if (
    !retainedExecutionId
    || state.phase === 'succeeded'
    || state.phase === 'failed'
    || state.phase === 'cancelled'
    || state.phase === 'interrupted'
  ) {
    return state;
  }
  return {
    ...state,
    phase: 'disconnected',
    executionId: retainedExecutionId,
    finishedAt: null,
    lastError: 'Backend connection lost; waiting to reconcile execution status.',
  };
};

export const markExecutionInterrupted = (
  state: ExecutionRuntimeState,
  executionId?: string | null,
): ExecutionRuntimeState => ({
  ...state,
  phase: 'interrupted',
  executionId: executionId || state.executionId,
  finishedAt: Date.now(),
  lastError: 'The backend no longer has this execution. Window runs can be resumed from their recovery checkpoint.',
});
