/// <reference types="node" />

import { readFileSync } from 'node:fs';
import { join } from 'node:path';

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

const source = (relativePath: string): string => readFileSync(
  join(process.cwd(), 'src', relativePath),
  'utf8',
);

const app = source('App.tsx');
const header = source('components/layout/Header.tsx');
const settingsDrawer = source('components/execution/ExecutionSettingsDrawer.tsx');
const recoveryDrawer = source('components/execution/RecoveryBrowserDialog.tsx');
const flowContext = source('context/FlowContext.tsx');
const settingsStore = source('hooks/useWorkflowExecutionSettingsStore.ts');
const flowEngine = source('hooks/useFlowEngine.ts');

assert(
  !app.includes('ExecutionConfigDialog'),
  'the retired combined Run configuration dialog must not be mounted',
);
assert(
  app.includes('<ExecutionSettingsDrawer />') && app.includes('<RecoveryBrowserDialog />'),
  'normal settings and recovery must be mounted as separate interfaces',
);

const settingsButton = header.indexOf('<span>Execution Settings</span>');
const recoveryButton = header.indexOf('>\n          Recovery\n        </Button>');
const runButton = header.indexOf("{isPreflighting ? 'Checking...' : 'Run'}");
assert(
  settingsButton >= 0 && recoveryButton > settingsButton && runButton > recoveryButton,
  'the header must present Execution Settings, Recovery, then Run',
);
assert(
  header.includes('executionSettingsSummary')
    && header.includes('activeExecutionSettingsConfigured'),
  'the header must always expose the saved execution-mode summary',
);

assert(
  !/>\s*(Resume|Restart)\s*</.test(settingsDrawer),
  'Execution Settings must not render Resume or Restart controls',
);
assert(
  !settingsDrawer.includes('Delete Recovery'),
  'Execution Settings must not render recovery deletion controls',
);
assert(
  !settingsDrawer.includes('inspectRecoveryDirectory')
    && !settingsDrawer.includes('openRecoveryDirectory')
    && !settingsDrawer.includes('deleteRecoveryDirectory')
    && !settingsDrawer.includes('executeRecoveryDirectory'),
  'Execution Settings must not perform recovery-record operations',
);
assert(
  />\s*Resume\s*</.test(recoveryDrawer)
    && />\s*Restart\s*</.test(recoveryDrawer)
    && recoveryDrawer.includes('Open Saved Workflow')
    && recoveryDrawer.includes('Delete Recovery Record')
    && recoveryDrawer.includes('Inspect'),
  'Recovery must expose inspect, saved-graph, deletion, Resume, and Restart actions',
);
assert(
  recoveryDrawer.includes('inspectRecoveryDirectory(recoveredDirectory)'),
  'reopening Recovery must refresh progress rather than reuse a stale summary',
);
assert(
  recoveryDrawer.includes('summary.executionId')
    && recoveryDrawer.includes('outputs are not deleted by this action')
    && recoveryDrawer.includes('overwrite those outputs'),
  'recovery deletion must confirm identity and explain later Writer overwrites',
);

const runFlowStart = flowContext.indexOf('const runFlow = useCallback');
const runFlowEnd = flowContext.indexOf('// ===========================================', runFlowStart);
const runFlow = flowContext.slice(runFlowStart, runFlowEnd);
assert(runFlowStart >= 0 && runFlowEnd > runFlowStart, 'normal Run orchestration must exist');
assert(
  runFlow.indexOf('await preflightExecutionSettings(') >= 0
    && runFlow.indexOf('submitPreparedExecution(')
      > runFlow.indexOf('await preflightExecutionSettings('),
  'normal Run must finish silent preflight before formal execution submission',
);
assert(
  !runFlow.includes('inspectRecoveryDirectory(')
    && !runFlow.includes('openRecoveryDirectory(')
    && !runFlow.includes('deleteRecoveryDirectory(')
    && !runFlow.includes('executeRecoveryDirectory('),
  'normal Run must never inspect, open, or execute an old recovery record',
);

const settingsPreflightStart = flowContext.indexOf('const preflightExecutionSettings');
const settingsPreflightEnd = flowContext.indexOf(
  'const refreshExecutionSettingsPreflight',
  settingsPreflightStart,
);
const settingsPreflight = flowContext.slice(settingsPreflightStart, settingsPreflightEnd);
assert(
  settingsPreflight.indexOf("preflightFlow({ mode: 'full_graph' })") >= 0
    && settingsPreflight.indexOf('buildNewRunExecutionConfig(')
      > settingsPreflight.indexOf("preflightFlow({ mode: 'full_graph' })"),
  'Window validation must discover rank/anchors before submitting a saved Window plan',
);
assert(
  flowContext.includes('resetExecutionSettingsUi();\n    _switchWorkflow(id);')
    && flowContext.includes('resetExecutionSettingsUi();\n    _loadWorkflowDocument('),
  'workflow switching and importing must discard another workflow draft/preflight',
);
assert(
  settingsStore.indexOf('updateWorkflowMetadata(')
    < settingsStore.indexOf('saveLocalWorkflowExecutionSettings(')
    && settingsStore.includes('serializeWorkflowDocument({'),
  'settings must update primary metadata first and synchronously refresh the active workflow document',
);

const deleteRecoveryStart = flowEngine.indexOf('const deleteRecoveryDirectory = useCallback');
const deleteRecoveryEnd = flowEngine.indexOf('const submitExecution = useCallback', deleteRecoveryStart);
const deleteRecovery = flowEngine.slice(deleteRecoveryStart, deleteRecoveryEnd);
assert(
  deleteRecoveryStart >= 0
    && deleteRecovery.includes("'/execution/recovery/delete'")
    && deleteRecovery.includes("method: 'POST'")
    && deleteRecovery.includes('expectedExecutionId')
    && deleteRecovery.includes('closeRecoveredGraph()'),
  'recovery deletion must use the dedicated API, identity check, and close a deleted saved graph',
);

const submitStart = flowEngine.indexOf('const submitExecution = useCallback');
const submitEnd = flowEngine.indexOf('const executeRecoveryDirectory', submitStart);
const submission = flowEngine.slice(submitStart, submitEnd);
assert(
  submission.indexOf('crypto.randomUUID()') >= 0
    && submission.indexOf("sessionStorage.setItem('WorkFlow_execution_id', executionId)")
      > submission.indexOf('crypto.randomUUID()')
    && submission.indexOf('ws.send(JSON.stringify({')
      > submission.indexOf("sessionStorage.setItem('WorkFlow_execution_id', executionId)"),
  'the frontend execution ID must be generated and persisted before WebSocket submission',
);
