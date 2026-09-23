import type {
  ExecutionConfig,
  ExecutionPreflightResponse,
  ResolvedWorkflowExecutionSettings,
  WorkflowExecutionSettings,
  WorkflowExecutionSettingsValidation,
} from '../types';
import {
  buildNewRunExecutionConfig,
  validateWorkflowExecutionSettings,
  type WorkflowExecutionSettingsValidationContext,
} from './workflowExecutionSettings.ts';
import { preflightOutputShape } from './executionConfig.ts';

export type NormalRunDecision =
  | {
      kind: 'execute';
      config: ExecutionConfig;
      validation: WorkflowExecutionSettingsValidation;
    }
  | {
      kind: 'open_settings';
      validation: WorkflowExecutionSettingsValidation;
    };

const mergeValidation = (
  current: WorkflowExecutionSettingsValidation,
  stored?: WorkflowExecutionSettingsValidation,
): WorkflowExecutionSettingsValidation => {
  const fieldErrors = {
    ...(stored?.fieldErrors ?? {}),
    ...current.fieldErrors,
  };
  const generalError = current.generalError ?? stored?.generalError;
  return {
    isValid: Object.keys(fieldErrors).length === 0 && generalError === undefined,
    fieldErrors,
    ...(generalError === undefined ? {} : { generalError }),
  };
};

export const validationContextFromPreflight = (
  preflight: ExecutionPreflightResponse,
): WorkflowExecutionSettingsValidationContext => {
  const preflightError = preflight.preflightError?.message?.trim();
  return {
    outputShape: preflightOutputShape(preflight),
    availableAnchorNodeIds: (preflight.outputs ?? []).map(output => output.nodeId),
    windowable: preflightError ? false : preflight.windowable,
    windowUnavailableReason: preflightError
      ? `Preflight failed: ${preflightError}`
      : preflight.reason,
    resourcesSatisfied: preflightError ? false : preflight.resourcesSatisfied,
    resourceError: preflightError
      ? `Preflight failed: ${preflightError}`
      : preflight.resourceError,
  };
};

/**
 * Select a side-effect-free preflight payload. Incomplete Window drafts use
 * Full Graph metadata preflight so the drawer can still discover output rank
 * and terminal anchors without inventing missing recovery settings.
 */
export const preflightConfigForSettings = (
  settings: WorkflowExecutionSettings,
): ExecutionConfig => {
  if (settings.mode === 'full_graph') return { mode: 'full_graph' };
  try {
    return buildNewRunExecutionConfig(settings);
  } catch {
    return { mode: 'full_graph' };
  }
};

export const decideNormalRun = (
  resolved: ResolvedWorkflowExecutionSettings,
  preflight: ExecutionPreflightResponse,
): NormalRunDecision => {
  if (!resolved.isConfigured) {
    return {
      kind: 'open_settings',
      validation: {
        isValid: false,
        fieldErrors: {},
        generalError: 'Configure and save Execution Settings before the first run.',
      },
    };
  }

  const currentValidation = validateWorkflowExecutionSettings(
    resolved.settings,
    validationContextFromPreflight(preflight),
  );
  const validation = mergeValidation(currentValidation, resolved.validation);
  if (!validation.isValid) {
    return { kind: 'open_settings', validation };
  }

  try {
    return {
      kind: 'execute',
      config: buildNewRunExecutionConfig(
        resolved.settings,
        validationContextFromPreflight(preflight),
      ),
      validation,
    };
  } catch (error) {
    return {
      kind: 'open_settings',
      validation: {
        ...validation,
        isValid: false,
        generalError: (error as Error).message,
      },
    };
  }
};

export const preflightFailureValidation = (
  error: unknown,
): WorkflowExecutionSettingsValidation => ({
  isValid: false,
  fieldErrors: {},
  generalError: error instanceof Error ? error.message : String(error),
});

/** Map formal new-run recovery setup failures back to the saved field. */
export const normalRunRecoveryFailureValidation = (
  message: string,
  settings: WorkflowExecutionSettings,
): WorkflowExecutionSettingsValidation | null => {
  if (settings.mode !== 'window' || !settings.newRunRecoveryLocation) return null;
  const normalized = message.toLowerCase();
  const location = settings.newRunRecoveryLocation;
  const customDirectoryMentioned = location.mode === 'custom'
    && location.directory.trim() !== ''
    && normalized.includes(location.directory.trim().toLowerCase());
  const isConflict = normalized.includes('recovery directory is not empty')
    || normalized.includes('choose resume or restart')
    || (
      normalized.includes('recovery')
      && normalized.includes('already exists')
    );
  const isAnchorFailure = normalized.includes('recovery')
    && normalized.includes('anchor');
  const isAccessFailure = (
    normalized.includes('recovery') || customDirectoryMentioned
  ) && [
    'permission denied',
    'access is denied',
    'not accessible',
    'not a directory',
    'read-only file system',
    'cannot create',
    'failed to create',
  ].some(fragment => normalized.includes(fragment));

  if (!isConflict && !isAnchorFailure && !isAccessFailure) return null;

  const fieldErrors: WorkflowExecutionSettingsValidation['fieldErrors'] = {};
  if (isConflict) {
    const conflictMessage = (
      'This new-run recovery location already contains a recovery record. '
      + 'To run the current edited workflow, choose another location or delete the old record in Recovery. '
      + 'Resume and Restart continue using the old saved workflow.'
    );
    if (location.mode === 'custom') fieldErrors.directory = conflictMessage;
    else fieldErrors.newRunRecoveryLocation = conflictMessage;
  } else if (isAnchorFailure && location.mode === 'output_sidecar') {
    fieldErrors.anchorNodeId = message;
  } else if (location.mode === 'custom') {
    fieldErrors.directory = message;
  } else {
    fieldErrors.newRunRecoveryLocation = message;
  }
  return { isValid: false, fieldErrors };
};
