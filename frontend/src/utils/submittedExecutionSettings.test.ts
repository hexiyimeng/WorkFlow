import { copySubmittedExecutionSettings, rememberSubmittedExecutionSettings, readSubmittedExecutionSettings } from './submittedExecutionSettings.ts';
import type { SubmittedExecutionSettings } from '../types.ts';

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
const values = new Map<string, string>();
Object.assign(globalThis, { sessionStorage: {
  getItem: (key: string) => values.get(key) ?? null,
  setItem: (key: string, value: string) => values.set(key, value),
} });
const source: SubmittedExecutionSettings = {
  executionId: 'execution-a',
  config: { mode: 'window', windowShape: [32, 256, 256], maxInFlightWindows: 3, resumeAction: 'new', recoveryLocation: { mode: 'custom', directory: '/data/recovery' } },
  workerProfiles: [{ name: 'GPU', threads: 4, capabilities: ['GPU'], logical_resources: { GPU: 1 }, physical_resources: { cpu: 4, memory: '16GB', gpu: 1 } }],
  workerPools: [{ profile: 'GPU', minimum_jobs: 1, maximum_jobs: 2, processes: 1 }],
  preflight: { windowable: true, totalWindows: 3, outputShape: [96, 256, 256] },
};
const snapshot = copySubmittedExecutionSettings(source);
source.workerProfiles[0].physical_resources.cpu = 20;
if (source.config.mode === 'window') source.config.windowShape![0] = 99;
assert(snapshot.workerProfiles[0].physical_resources.cpu === 4, 'later browser resource edits must not alter the submitted snapshot');
assert(snapshot.config.mode === 'window' && snapshot.config.windowShape![0] === 32, 'Window dimensions must be copied');
sessionStorage.setItem('WorkFlow_execution_id', snapshot.executionId);
rememberSubmittedExecutionSettings(snapshot);
assert(readSubmittedExecutionSettings()?.workerPools[0].maximum_jobs === 2, 'reload restores the submitted Job range');
const invalid = copySubmittedExecutionSettings(snapshot);
Object.assign(invalid.config, { maxInFlightWindows: {} });
rememberSubmittedExecutionSettings(invalid);
assert(readSubmittedExecutionSettings() === null, 'malformed settings must not reach the read-only renderer');
rememberSubmittedExecutionSettings(snapshot);
sessionStorage.setItem('WorkFlow_execution_id', 'other-execution');
assert(readSubmittedExecutionSettings() === null, 'never show an old execution snapshot for a different execution');
values.set('WorkFlow_submitted_execution_settings', '{invalid json');
assert(readSubmittedExecutionSettings() === null, 'corrupt browser data must not break initialization');
Object.assign(globalThis, { sessionStorage: { getItem: () => { throw new Error('storage unavailable'); }, setItem: () => { throw new Error('storage unavailable'); } } });
rememberSubmittedExecutionSettings(snapshot);
assert(readSubmittedExecutionSettings() === null, 'restricted storage must not break execution');
console.log('submitted execution settings tests passed');
