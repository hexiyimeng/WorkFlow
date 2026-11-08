import { executionDiagnostic } from './executionDiagnostics.ts';

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

const diagnostic = executionDiagnostic({
  type: 'execution_finished', status: 'failed',
  executionId: 'execution-1', taskId: 'algorithm-node', nodeType: 'Cellpose',
  stage: 'Dask task execution', errorType: 'ValueError',
  taskKey: 'cellpose-block-3', message: 'Invalid image dimensions\nAdditional context',
  traceback: 'Traceback: failing block',
});
assert(diagnostic.message.includes('Cellpose [algorithm-node]'), 'show the failed node');
assert(diagnostic.message.includes('Invalid image dimensions'), 'show the actual cause');
assert(diagnostic.message.includes('Dask task execution'), 'show the execution stage');
assert(!diagnostic.message.includes('Additional context'), 'keep the summary short');
assert(diagnostic.details?.includes('Additional context') === true, 'retain full error text');
assert(diagnostic.details?.includes('cellpose-block-3') === true, 'retain the failed task key');
assert(diagnostic.details?.includes('Traceback') === true, 'retain stack traces');
const long = executionDiagnostic({ type: 'error', message: 'x'.repeat(20000) });
assert(long.message.length <= 501, 'long exceptions must not fill the console');
assert(long.details?.length === 16000, 'bound retained error details');
assert(executionDiagnostic({ type: 'error' }).message === 'Unknown error', 'handle old error events');
console.log('execution diagnostics tests passed');
