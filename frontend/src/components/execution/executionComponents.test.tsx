import { renderToStaticMarkup } from 'react-dom/server';
import { ReactFlowProvider } from '@xyflow/react';
import ExecutionSettingsDrawer from './ExecutionSettingsDrawer';
import RecoveryBrowserDialog from './RecoveryBrowserDialog';
import Header from '../layout/Header';
import { FlowContext, type FlowContextType } from '../../context/FlowContextDef';
import type { ExecutionPreflightResponse, WorkflowExecutionSettings } from '../../types';

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

const settings: WorkflowExecutionSettings = {
  version: 1,
  mode: 'window',
  windowShape: [64, 256, 256],
  maxInFlightWindows: 8,
  newRunRecoveryLocation: {
    mode: 'output_sidecar',
    anchorNodeId: 'deleted-writer',
  },
};

const preflight: ExecutionPreflightResponse = {
  windowable: true,
  outputShape: [293, 1077, 1050],
  totalWindows: 125,
  outputs: [{
    nodeId: 'current-writer',
    nodeType: 'ZarrWriter',
    displayName: 'Current Zarr Writer',
    pathInput: 'output_path',
    path: 'D:\\results\\cells.zarr',
  }],
  resourcesSatisfied: true,
};

const baseContext = {
  isExecutionSettingsOpen: true,
  activeWorkflowDocumentId: 'workflow-a',
  activeExecutionSettings: settings,
  activeExecutionSettingsConfigured: true,
  executionSettingsValidation: {
    isValid: false,
    fieldErrors: {
      anchorNodeId: 'The saved output anchor no longer exists. Select another terminal output node.',
    },
  },
  executionPreflight: preflight,
  closeExecutionSettings: () => undefined,
  saveExecutionSettings: async () => true,
  refreshExecutionSettingsPreflight: async () => undefined,
  isConnected: true,
  isExecuting: false,
  isPreflighting: false,
} as unknown as FlowContextType;

const executionSettingsMarkup = renderToStaticMarkup(
  <FlowContext.Provider value={baseContext}>
    <ExecutionSettingsDrawer />
  </FlowContext.Provider>,
);
assert(executionSettingsMarkup.includes('Execution Settings'), 'settings drawer must render');
assert(executionSettingsMarkup.includes('Window Shape'), 'Window settings must render');
assert(
  executionSettingsMarkup.includes('value="64"')
    && executionSettingsMarkup.includes('value="8"'),
  'invalid graph references must not erase saved Window or concurrency values',
);
assert(
  executionSettingsMarkup.includes('saved output anchor no longer exists'),
  'the deleted anchor must receive an actionable field error',
);
assert(
  !/>\s*Resume\s*</.test(executionSettingsMarkup)
    && !/>\s*Restart\s*</.test(executionSettingsMarkup),
  'rendered Execution Settings must contain no recovery actions',
);

const failedPreflightMarkup = renderToStaticMarkup(
  <FlowContext.Provider value={{
    ...baseContext,
    activeExecutionSettings: {
      ...settings,
      lastPreflight: {
        outputShape: [293, 1077, 1050],
        totalWindows: 0,
        validatedAt: 1,
      },
    },
    executionSettingsValidation: null,
    executionPreflight: {
      windowable: false,
      outputShape: null,
      totalWindows: null,
      resourcesSatisfied: false,
      resourceError: "Set the Reader array_path explicitly, for example 's0'.",
      preflightError: {
        type: 'ValueError',
        message: "Set the Reader array_path explicitly, for example 's0'.",
      },
    },
  } as unknown as FlowContextType}>
    <ExecutionSettingsDrawer />
  </FlowContext.Provider>,
);
assert(
  failedPreflightMarkup.includes("Preflight failed: Set the Reader array_path explicitly, for example &#x27;s0&#x27;."),
  'the drawer must prominently display the current Reader preflight failure',
);
assert(
  failedPreflightMarkup.includes('>Preflight failed</dd>')
    && !failedPreflightMarkup.includes('>0</dd>'),
  'a current failed preflight must not display a stale cached zero-window summary',
);

const recoveryContext = {
  isRecoveryBrowserOpen: true,
  recoveredGraphView: null,
  closeRecoveryBrowser: () => undefined,
  browseServerDirectories: async () => ({ path: '', parent: null, directories: [] }),
  inspectRecoveryDirectory: async () => { throw new Error('not invoked during render'); },
  openRecoveryDirectory: async () => { throw new Error('not invoked during render'); },
  deleteRecoveryDirectory: async () => { throw new Error('not invoked during render'); },
  executeRecoveryDirectory: async () => true,
  isConnected: true,
  isExecuting: false,
} as unknown as FlowContextType;
const recoveryMarkup = renderToStaticMarkup(
  <FlowContext.Provider value={recoveryContext}>
    <RecoveryBrowserDialog />
  </FlowContext.Provider>,
);
assert(recoveryMarkup.includes('Recovery directory'), 'Recovery drawer must render');
assert(recoveryMarkup.includes('>Inspect</button>'), 'Recovery must render Inspect');
assert(recoveryMarkup.includes('>Open Saved Workflow</button>'), 'Recovery must render saved graph opening');
assert(recoveryMarkup.includes('>Restart</button>'), 'Recovery must render Restart');
assert(recoveryMarkup.includes('>Resume</button>'), 'Recovery must render Resume');
assert(
  recoveryMarkup.includes('>Delete Recovery Record</button>'),
  'Recovery must render explicit record deletion',
);

const headerContext = {
  theme: 'light',
  toggleTheme: () => undefined,
  workflows: [{
    id: 'tab-a',
    workflowId: 'workflow-a',
    metadata: { executionSettings: settings },
    name: 'Cells',
    nodes: [],
    edges: [],
    timestamp: 1,
  }],
  activeWorkflow: {
    id: 'tab-a',
    workflowId: 'workflow-a',
    metadata: { executionSettings: settings },
    name: 'Cells',
    nodes: [],
    edges: [],
    timestamp: 1,
  },
  activeWorkflowId: 'tab-a',
  createWorkflow: () => undefined,
  switchWorkflow: () => undefined,
  deleteWorkflow: () => undefined,
  renameWorkflow: () => undefined,
  loadWorkflowDocument: () => undefined,
  activeExecutionSettings: settings,
  activeExecutionSettingsConfigured: true,
  openExecutionSettings: () => undefined,
  runFlow: async () => undefined,
  stopFlow: () => undefined,
  reloadNodes: async () => undefined,
  nodeDefs: {},
  dashboardUrl: '',
  isReloadingNodes: false,
  executionState: {
    phase: 'idle',
    executionId: null,
    startedAt: null,
    finishedAt: null,
    totalNodes: 0,
    lastError: null,
    windowProgress: null,
  },
  isConnected: true,
  isExecuting: false,
  isPreflighting: false,
  isExecutionLocked: false,
  addLog: () => undefined,
  recoveredGraphView: null,
  openRecoveryBrowser: () => undefined,
  closeRecoveredGraph: () => undefined,
} as unknown as FlowContextType;
const headerMarkup = renderToStaticMarkup(
  <ReactFlowProvider>
    <FlowContext.Provider value={headerContext}>
      <Header />
    </FlowContext.Provider>
  </ReactFlowProvider>,
);
const settingsIndex = headerMarkup.indexOf('Execution Settings');
const recoveryIndex = headerMarkup.indexOf('>Recovery</button>');
const runIndex = headerMarkup.indexOf('>Run</button>');
assert(
  settingsIndex >= 0 && recoveryIndex > settingsIndex && runIndex > recoveryIndex,
  'rendered header must present Execution Settings, Recovery, then primary Run',
);
assert(
  headerMarkup.includes('Window · 64×256×256 · In-flight 8'),
  'rendered header must expose the configuration used by the next Run',
);
