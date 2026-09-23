import type { ExecutionRuntimeState } from '../types.ts';
import {
  blocksExecutionChanges,
  filterLockedEdgeChanges,
  filterLockedNodeChanges,
  isLiveExecutionPhase,
  markExecutionConnectionLost,
  markExecutionInterrupted,
  websocketDisconnectMessage,
  websocketReconnectDelayMs,
} from './executionRuntime.ts';

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

const running: ExecutionRuntimeState = {
  phase: 'running',
  executionId: 'run-123',
  startedAt: 1,
  finishedAt: null,
  totalNodes: 2,
  lastError: null,
  windowProgress: {
    currentWindow: 4,
    completedWindows: 3,
    totalWindows: 10,
    progress: 30,
    windowStatus: 'running',
    message: 'Window 4 / 10',
  },
};

const disconnected = markExecutionConnectionLost(running);
assert(disconnected.phase === 'disconnected', 'an active execution should become disconnected');
assert(disconnected.executionId === 'run-123', 'disconnect should retain the execution id');
assert(disconnected.windowProgress?.completedWindows === 3, 'disconnect should retain last reported progress');
assert(!isLiveExecutionPhase(disconnected.phase), 'disconnected must not render live execution controls');
assert(blocksExecutionChanges(disconnected.phase), 'unresolved execution state should remain locked');

const restoredAfterReload = markExecutionConnectionLost({
  ...running,
  phase: 'idle',
  executionId: null,
  windowProgress: null,
}, 'stored-run');
assert(restoredAfterReload.phase === 'disconnected', 'stored execution should reconcile after reload');
assert(restoredAfterReload.executionId === 'stored-run', 'stored execution id should be restored');

const interrupted = markExecutionInterrupted(disconnected, 'run-123');
assert(interrupted.phase === 'interrupted', 'missing backend execution should become interrupted');
assert(interrupted.windowProgress?.completedWindows === 3, 'interrupted state should retain diagnostic progress');
assert(!blocksExecutionChanges(interrupted.phase), 'confirmed interruption should allow recovery actions');

const idle: ExecutionRuntimeState = {
  ...running,
  phase: 'idle',
  executionId: null,
  windowProgress: null,
};
assert(markExecutionConnectionLost(idle) === idle, 'an idle disconnect should not invent an execution');

assert(websocketReconnectDelayMs(1) === 1_000, 'first reconnect should be prompt');
assert(websocketReconnectDelayMs(6) === 30_000, 'reconnect delay should be capped');
assert(websocketReconnectDelayMs(50) === 30_000, 'large retries should remain capped');
assert(
  websocketDisconnectMessage(1006, true, 2_000).includes('SSH/VPN/network'),
  'abnormal closure should explain the likely transport layer',
);
assert(
  websocketDisconnectMessage(1006, true, 2_000).includes('may still be running'),
  'disconnect should not claim that the backend execution failed',
);

const permittedNodeChanges = filterLockedNodeChanges([
  { type: 'position', id: 'node', position: { x: 10, y: 20 } },
  { type: 'dimensions', id: 'node', dimensions: { width: 260, height: 180 } },
  { type: 'select', id: 'node', selected: true },
  { type: 'remove', id: 'node' },
  { type: 'add', item: { id: 'new-node' } },
  { type: 'replace', id: 'node', item: { id: 'replacement' } },
]);
assert(
  permittedNodeChanges.map(change => change.type).join(',') === 'position,dimensions,select',
  'execution lock should allow layout changes but reject semantic node changes',
);

const permittedEdgeChanges = filterLockedEdgeChanges([
  { type: 'select', id: 'edge', selected: true },
  { type: 'remove', id: 'edge' },
  { type: 'add', item: { id: 'new-edge' } },
  { type: 'replace', id: 'edge', item: { id: 'replacement' } },
]);
assert(
  permittedEdgeChanges.length === 1 && permittedEdgeChanges[0]?.type === 'select',
  'execution lock should reject edge mutations while retaining visual selection',
);
