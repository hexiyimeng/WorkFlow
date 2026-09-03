import type {
  ExecutionPreflightResponse,
  ResolvedWorkflowExecutionSettings,
  WorkflowExecutionSettings,
} from '../types.ts';
import {
  decideNormalRun,
  normalRunRecoveryFailureValidation,
  preflightConfigForSettings,
} from './executionExperience.ts';
import { sanitizeWorkflowExecutionSettings } from './workflowExecutionSettings.ts';

const assert = (condition: boolean, message: string) => {
  if (!condition) throw new Error(message);
};

const preflight: ExecutionPreflightResponse = {
  windowable: true,
  outputShape: [64, 512, 512],
  outputs: [{
    nodeId: 'writer',
    nodeType: 'ZarrWriter',
    displayName: 'Zarr Writer',
    pathInput: 'output_path',
    path: 'D:\\results\\image.zarr',
  }],
  resourcesSatisfied: true,
};

const resolved = (
  settings: WorkflowExecutionSettings,
  isConfigured = true,
): ResolvedWorkflowExecutionSettings => ({
  settings,
  source: isConfigured ? 'metadata' : 'default',
  isConfigured,
  validation: { isValid: true, fieldErrors: {} },
});

const fullGraph = decideNormalRun(
  resolved({ version: 1, mode: 'full_graph' }),
  preflight,
);
assert(fullGraph.kind === 'execute', 'saved Full Graph settings should execute directly');
assert(
  fullGraph.kind === 'execute' && fullGraph.config.mode === 'full_graph',
  'Full Graph execution must use the compatible wire payload',
);

const failedReaderPreflight = decideNormalRun(
  resolved({ version: 1, mode: 'full_graph' }),
  {
    ...preflight,
    windowable: false,
    outputShape: null,
    totalWindows: null,
    resourcesSatisfied: true,
    preflightError: {
      type: 'ValueError',
      message: "Set the Reader array_path explicitly, for example 's0'.",
    },
  },
);
assert(
  failedReaderPreflight.kind === 'open_settings'
    && failedReaderPreflight.validation.generalError?.includes("array_path explicitly") === true,
  'a graph metadata preflight failure must block Run even if resources are available',
);

const windowSettings: WorkflowExecutionSettings = {
  version: 1,
  mode: 'window',
  windowShape: [64, 256, 256],
  maxInFlightWindows: 8,
  newRunRecoveryLocation: { mode: 'output_sidecar', anchorNodeId: 'writer' },
};
const windowRun = decideNormalRun(resolved(windowSettings), preflight);
assert(windowRun.kind === 'execute', 'valid Window settings should execute directly');
assert(
  windowRun.kind === 'execute'
    && windowRun.config.mode === 'window'
    && windowRun.config.resumeAction === 'new',
  'ordinary Window Run must always force resumeAction=new',
);

const firstRun = decideNormalRun(
  resolved({ version: 1, mode: 'full_graph' }, false),
  preflight,
);
assert(firstRun.kind === 'open_settings', 'an unconfigured workflow must open settings');

const wrongRankSettings = {
  ...windowSettings,
  windowShape: [64, 256],
};
const wrongRank = decideNormalRun(resolved(wrongRankSettings), preflight);
assert(wrongRank.kind === 'open_settings', 'rank mismatch must open settings');
assert(
  wrongRank.validation.fieldErrors.windowShape?.includes('rank 2') === true,
  'rank mismatch should identify only the Window shape field',
);
assert(
  wrongRankSettings.maxInFlightWindows === 8
    && wrongRankSettings.newRunRecoveryLocation?.mode === 'output_sidecar'
    && wrongRankSettings.newRunRecoveryLocation.anchorNodeId === 'writer',
  'rank validation must preserve unaffected values',
);

const deletedAnchorSettings = {
  ...windowSettings,
  newRunRecoveryLocation: {
    mode: 'output_sidecar' as const,
    anchorNodeId: 'deleted-writer',
  },
};
const deletedAnchor = decideNormalRun(resolved(deletedAnchorSettings), preflight);
assert(deletedAnchor.kind === 'open_settings', 'a deleted anchor must open settings');
assert(
  deletedAnchor.validation.fieldErrors.anchorNodeId?.includes('no longer exists') === true,
  'only the deleted anchor should be highlighted',
);
assert(
  deletedAnchor.validation.fieldErrors.windowShape === undefined
    && deletedAnchorSettings.windowShape?.join(',') === '64,256,256',
  'deleted-anchor validation must preserve the Window shape',
);

const hostileCache = sanitizeWorkflowExecutionSettings({
  ...windowSettings,
  resumeAction: 'resume',
  executionId: 'cached-run',
  selectedRecoveryRecord: 'D:\\old.workflow',
});
const hostileDecision = decideNormalRun({
  settings: hostileCache.settings,
  source: 'local',
  isConfigured: true,
  validation: hostileCache.validation,
}, preflight);
assert(
  hostileDecision.kind === 'execute'
    && hostileDecision.config.mode === 'window'
    && hostileDecision.config.resumeAction === 'new',
  'cached recovery/runtime state must never affect ordinary Run',
);
assert(
  preflightConfigForSettings({ version: 1, mode: 'window' }).mode === 'full_graph',
  'an incomplete Window draft should still allow read-only metadata preflight',
);

const customSettings: WorkflowExecutionSettings = {
  ...windowSettings,
  newRunRecoveryLocation: {
    mode: 'custom',
    directory: 'D:\\recoveries\\new-run',
  },
};
const conflictValidation = normalRunRecoveryFailureValidation(
  'Recovery directory is not empty; choose Resume or Restart for a current recovery record.',
  customSettings,
);
assert(
  conflictValidation?.fieldErrors.directory?.includes('delete the old record in Recovery') === true
    && conflictValidation.fieldErrors.directory.includes('current edited workflow'),
  'a new-run conflict must explain how to discard recovery and run the edited graph',
);
const accessValidation = normalRunRecoveryFailureValidation(
  'Permission denied: D:\\recoveries\\new-run',
  customSettings,
);
assert(
  accessValidation?.fieldErrors.directory?.includes('Permission denied') === true,
  'an inaccessible saved directory must be mapped back to that field',
);
assert(
  normalRunRecoveryFailureValidation('Model inference failed', customSettings) === null,
  'ordinary node failures must not force the Execution Settings drawer open',
);
