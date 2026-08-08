import type {
  ExecutionPreflightResponse,
  WorkflowExecutionSettings,
} from '../types.ts';
import {
  buildNewRunExecutionConfig,
  commitWorkflowExecutionSettings,
  createDefaultWorkflowExecutionSettings,
  createWorkflowId,
  formatWorkflowExecutionSettingsSummary,
  lastPreflightSummaryFromResponse,
  loadLocalWorkflowExecutionSettings,
  mirrorWorkflowExecutionSettingsIntoAutosave,
  resolveWorkflowExecutionSettings,
  sanitizeWorkflowExecutionSettings,
  saveLocalWorkflowExecutionSettings,
  validateWorkflowExecutionSettings,
  withLastPreflightSummary,
  workflowExecutionSettingsStorageKey,
  type WorkflowExecutionSettingsStorage,
} from './workflowExecutionSettings.ts';

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

class MemoryStorage implements WorkflowExecutionSettingsStorage {
  readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }

  removeItem(key: string): void {
    this.values.delete(key);
  }
}

const storage = new MemoryStorage();
const windowSettings: WorkflowExecutionSettings = {
  version: 1,
  mode: 'window',
  windowShape: [64, 256, 256],
  maxInFlightWindows: 8,
  newRunRecoveryLocation: {
    mode: 'output_sidecar',
    anchorNodeId: 'writer-123',
  },
};

assert(
  workflowExecutionSettingsStorageKey('workflow-a')
    === 'workflow.executionSettings.workflow-a',
  'settings must be keyed by stable workflowId',
);

saveLocalWorkflowExecutionSettings('workflow-a', windowSettings, storage);
const afterRefresh = resolveWorkflowExecutionSettings({
  workflowId: 'workflow-a',
  storage,
});
assert(afterRefresh.source === 'local', 'local settings should survive a fresh resolution');
assert(afterRefresh.isConfigured, 'local settings should count as configured');
assert(
  afterRefresh.settings.windowShape?.join(',') === '64,256,256',
  'local storage should preserve Window shape',
);

const defaultResolution = resolveWorkflowExecutionSettings({
  workflowId: 'never-configured',
  storage,
});
assert(defaultResolution.source === 'default', 'missing settings should use defaults');
assert(!defaultResolution.isConfigured, 'application defaults must not masquerade as saved settings');
assert(defaultResolution.settings.mode === 'full_graph', 'application default should remain Full Graph');

saveLocalWorkflowExecutionSettings('workflow-b', {
  version: 1,
  mode: 'full_graph',
}, storage);
const workflowA = resolveWorkflowExecutionSettings({ workflowId: 'workflow-a', storage });
const workflowB = resolveWorkflowExecutionSettings({ workflowId: 'workflow-b', storage });
assert(workflowA.settings.mode === 'window', 'workflow A must retain its own settings');
assert(workflowB.settings.mode === 'full_graph', 'workflow B must retain independent settings');

const authoritativeMetadata = resolveWorkflowExecutionSettings({
  workflowId: 'workflow-a',
  storage,
  metadata: {
    executionSettings: {
      version: 1,
      mode: 'window',
      windowShape: [32, 32],
      maxInFlightWindows: 3,
      // Intentionally incomplete: metadata must still win over stale local data.
    },
  },
});
assert(authoritativeMetadata.source === 'metadata', 'workflow metadata must outrank local storage');
assert(authoritativeMetadata.settings.windowShape?.join(',') === '32,32', 'valid metadata values must survive');
assert(authoritativeMetadata.settings.maxInFlightWindows === 3, 'unaffected metadata fields must survive');
assert(!authoritativeMetadata.validation.isValid, 'incomplete authoritative metadata must require correction');
assert(
  Boolean(authoritativeMetadata.validation.fieldErrors.newRunRecoveryLocation),
  'only the missing recovery-location setting should be identified',
);

const malformedAuthoritativeMetadata = resolveWorkflowExecutionSettings({
  workflowId: 'workflow-a',
  storage,
  metadata: { executionSettings: 'broken' },
});
assert(
  malformedAuthoritativeMetadata.source === 'metadata',
  'malformed metadata must not silently fall back to stale local values',
);
assert(
  !malformedAuthoritativeMetadata.validation.isValid,
  'malformed metadata must open settings for correction',
);

const futureVersionMetadata = resolveWorkflowExecutionSettings({
  workflowId: 'workflow-a',
  storage,
  metadata: {
    executionSettings: { version: 2, mode: 'full_graph' },
  },
});
assert(futureVersionMetadata.source === 'metadata', 'future metadata must remain authoritative');
assert(
  !futureVersionMetadata.validation.isValid
    && Boolean(futureVersionMetadata.validation.fieldErrors.version),
  'an unsupported settings schema must require explicit migration instead of executing',
);

const invalidAnchorSettings = {
  ...windowSettings,
  windowShape: [...(windowSettings.windowShape ?? [])],
};
const invalidAnchor = validateWorkflowExecutionSettings(invalidAnchorSettings, {
  outputShape: [293, 1077, 1050],
  availableAnchorNodeIds: ['another-writer'],
});
assert(!invalidAnchor.isValid, 'a deleted output anchor must invalidate Window settings');
assert(
  Object.keys(invalidAnchor.fieldErrors).join(',') === 'anchorNodeId',
  'a deleted anchor must highlight only the anchor field',
);
assert(
  invalidAnchorSettings.windowShape?.join(',') === '64,256,256'
    && invalidAnchorSettings.maxInFlightWindows === 8,
  'anchor validation must not erase unrelated settings',
);

const rankMismatch = validateWorkflowExecutionSettings(windowSettings, {
  outputShape: [293, 1077],
  availableAnchorNodeIds: ['writer-123'],
});
assert(
  Object.keys(rankMismatch.fieldErrors).join(',') === 'windowShape',
  'output-rank changes must invalidate only Window shape',
);
assert(
  rankMismatch.fieldErrors.windowShape
    === 'The saved Window shape has rank 3, but the current output has rank 2.',
  'rank mismatch should be actionable',
);

const customSettings: WorkflowExecutionSettings = {
  ...windowSettings,
  newRunRecoveryLocation: { mode: 'custom', directory: 'C:\\Recovery\\run' },
};
const customUnavailable = validateWorkflowExecutionSettings(customSettings, {
  outputShape: [293, 1077, 1050],
  customDirectoryError: 'Access denied by the recovery server.',
});
assert(
  Object.keys(customUnavailable.fieldErrors).join(',') === 'directory',
  'a backend directory error must mark only the custom-directory field',
);
assert(
  customSettings.newRunRecoveryLocation?.mode === 'custom'
    && customSettings.newRunRecoveryLocation.directory === 'C:\\Recovery\\run',
  'directory validation must preserve the path text',
);

const unsafeCachedSettings = {
  ...windowSettings,
  resumeAction: 'restart',
  executionId: 'must-not-persist',
  selectedRecoveryRecord: 'old-run',
};
const sanitized = sanitizeWorkflowExecutionSettings(unsafeCachedSettings);
assert(!Object.hasOwn(sanitized.settings, 'resumeAction'), 'resumeAction must be removed while loading settings');
assert(!Object.hasOwn(sanitized.settings, 'executionId'), 'executionId must be removed while loading settings');
assert(!Object.hasOwn(sanitized.settings, 'selectedRecoveryRecord'), 'selected recovery state must be removed');

saveLocalWorkflowExecutionSettings(
  'unsafe',
  unsafeCachedSettings as unknown as WorkflowExecutionSettings,
  storage,
);
const rawUnsafe = JSON.parse(storage.getItem(workflowExecutionSettingsStorageKey('unsafe')) ?? '{}');
assert(!Object.hasOwn(rawUnsafe, 'resumeAction'), 'local persistence must whitelist settings fields');
assert(!Object.hasOwn(rawUnsafe, 'executionId'), 'runtime IDs must not enter local settings');

const newRunConfig = buildNewRunExecutionConfig(unsafeCachedSettings, {
  outputShape: [293, 1077, 1050],
  availableAnchorNodeIds: ['writer-123'],
});
assert(newRunConfig.mode === 'window', 'Window settings should build a Window execution config');
assert(
  newRunConfig.mode === 'window' && newRunConfig.resumeAction === 'new',
  'normal Run must always force resumeAction=new',
);
assert(
  newRunConfig.mode === 'window'
    && newRunConfig.recoveryLocation.mode === 'output_sidecar'
    && newRunConfig.recoveryLocation.anchorNodeId === 'writer-123',
  'new-run recovery location should be copied explicitly',
);

const fullGraphConfig = buildNewRunExecutionConfig({
  ...createDefaultWorkflowExecutionSettings(),
  resumeAction: 'resume',
});
assert(
  fullGraphConfig.mode === 'full_graph' && !Object.hasOwn(fullGraphConfig, 'resumeAction'),
  'Full Graph payloads must remain backward-compatible and recovery-free',
);

const preflight: ExecutionPreflightResponse = {
  windowable: true,
  outputShape: [293, 1077, 1050],
  totalWindows: 125,
  requiredResources: {
    requiresCpu: true,
    requiresGpu: true,
    cpuWorkers: 1,
    gpuWorkers: 8,
    cpuNodes: [],
    gpuNodes: [],
  },
};
const summary = lastPreflightSummaryFromResponse(preflight, 1234);
assert(summary.outputShape?.join(',') === '293,1077,1050', 'preflight summary should retain output shape');
assert(summary.totalWindows === 125, 'preflight summary should retain total Windows');
assert(summary.cpuWorkers === 1 && summary.gpuWorkers === 8, 'preflight summary should retain resource counts');
assert(summary.validatedAt === 1234, 'preflight summary should retain validation time');

const settingsWithSummary = withLastPreflightSummary(windowSettings, preflight, 1234);
assert(settingsWithSummary.lastPreflight?.totalWindows === 125, 'last valid preflight should be cacheable');
assert(!Object.hasOwn(windowSettings, 'lastPreflight'), 'summary attachment must not mutate existing settings');
assert(
  formatWorkflowExecutionSettingsSummary(windowSettings)
    === 'Window · 64×256×256 · In-flight 8',
  'Window header summary should expose mode, shape, and concurrency',
);
assert(
  formatWorkflowExecutionSettingsSummary(createDefaultWorkflowExecutionSettings()) === 'Full Graph',
  'Full Graph header summary should be concise',
);

const loaded = loadLocalWorkflowExecutionSettings('workflow-a', storage);
assert(loaded?.settings.mode === 'window', 'direct local load should reconstruct settings');

const firstId = createWorkflowId();
const secondId = createWorkflowId();
assert(firstId !== secondId, 'new workflows must receive distinct stable IDs');
assert(
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(firstId),
  'generated workflow IDs should be UUID v4 values',
);

const primaryModes: WorkflowExecutionSettings['mode'][] = [];
let laterSecondaryRan = false;
let secondaryFailures = 0;
commitWorkflowExecutionSettings(
  windowSettings,
  settings => { primaryModes.push(settings.mode); },
  [
    () => { throw new Error('quota exceeded'); },
    () => { laterSecondaryRan = true; },
  ],
  () => { secondaryFailures += 1; },
);
assert(
  primaryModes[0] === 'window',
  'primary workflow metadata must commit before a secondary storage failure',
);
assert(
  laterSecondaryRan && secondaryFailures === 1,
  'one failed secondary cache must neither throw nor skip other mirrors',
);

const autosaveStorage = new MemoryStorage();
autosaveStorage.setItem('WorkFlow_AUTOSAVE', JSON.stringify({
  workflowId: 'workflow-a',
  metadata: {
    description: 'preserve me',
    executionSettings: { version: 1, mode: 'full_graph' },
  },
  nodes: [{ id: 'reader' }],
  edges: [],
}));
assert(
  mirrorWorkflowExecutionSettingsIntoAutosave({
    storage: autosaveStorage,
    autosaveKey: 'WorkFlow_AUTOSAVE',
    workflowId: 'workflow-a',
    settings: windowSettings,
  }),
  'fresh settings should synchronously patch the active autosave document',
);
const mirroredAutosave = JSON.parse(
  autosaveStorage.getItem('WorkFlow_AUTOSAVE') ?? '{}',
) as Record<string, unknown>;
const mirroredMetadata = mirroredAutosave.metadata as Record<string, unknown>;
const mirroredSettings = mirroredMetadata.executionSettings as Record<string, unknown>;
assert(mirroredSettings.mode === 'window', 'fresh settings must replace stale autosave metadata');
assert(mirroredMetadata.description === 'preserve me', 'unrelated workflow metadata must survive');
assert(
  Array.isArray(mirroredAutosave.nodes)
    && (mirroredAutosave.nodes[0] as Record<string, unknown>).id === 'reader',
  'synchronous settings persistence must not rewrite or drop the autosaved graph',
);
