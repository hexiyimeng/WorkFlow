import type {
  ExecutionConfig,
  ExecutionPreflightResponse,
  LastPreflightSummary,
  RecoveryLocation,
  ResolvedWorkflowExecutionSettings,
  WorkflowExecutionSettings,
  WorkflowExecutionSettingsField,
  WorkflowExecutionSettingsValidation,
} from '../types.ts';
import {
  isAbsoluteServerPath,
  isValidMaxInFlightWindows,
  isValidOutputShape,
  isValidWindowShape,
  preflightOutputShape,
} from './executionConfig.ts';

export const WORKFLOW_EXECUTION_SETTINGS_VERSION = 1 as const;
export const WORKFLOW_EXECUTION_SETTINGS_STORAGE_PREFIX = 'workflow.executionSettings.';

export interface WorkflowExecutionSettingsStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem?(key: string): void;
}

export type WorkflowExecutionSettingsWriter = (
  settings: WorkflowExecutionSettings,
) => void;

export interface SanitizedWorkflowExecutionSettings {
  settings: WorkflowExecutionSettings;
  validation: WorkflowExecutionSettingsValidation;
}

export interface WorkflowExecutionSettingsValidationContext {
  outputShape?: readonly number[] | null;
  availableAnchorNodeIds?: readonly string[];
  windowable?: boolean;
  windowUnavailableReason?: string | null;
  resourcesSatisfied?: boolean | null;
  resourceError?: string | null;
  customDirectoryError?: string | null;
}

const isRecord = (value: unknown): value is Record<string, unknown> => (
  value !== null && typeof value === 'object' && !Array.isArray(value)
);

const defaultSettings = (): WorkflowExecutionSettings => ({
  version: WORKFLOW_EXECUTION_SETTINGS_VERSION,
  mode: 'full_graph',
});

export const createDefaultWorkflowExecutionSettings = (): WorkflowExecutionSettings => (
  defaultSettings()
);

export const cloneWorkflowExecutionSettings = (
  settings: WorkflowExecutionSettings,
): WorkflowExecutionSettings => ({
  version: WORKFLOW_EXECUTION_SETTINGS_VERSION,
  mode: settings.mode,
  ...(settings.windowShape === undefined
    ? {}
    : { windowShape: [...settings.windowShape] }),
  ...(settings.maxInFlightWindows === undefined
    ? {}
    : { maxInFlightWindows: settings.maxInFlightWindows }),
  ...(settings.newRunRecoveryLocation === undefined
    ? {}
    : {
        newRunRecoveryLocation: settings.newRunRecoveryLocation.mode === 'output_sidecar'
          ? {
              mode: 'output_sidecar' as const,
              anchorNodeId: settings.newRunRecoveryLocation.anchorNodeId,
            }
          : {
              mode: 'custom' as const,
              directory: settings.newRunRecoveryLocation.directory,
            },
      }),
  ...(settings.lastPreflight === undefined
    ? {}
    : {
        lastPreflight: {
          ...settings.lastPreflight,
          ...(settings.lastPreflight.outputShape === undefined
            ? {}
            : { outputShape: [...settings.lastPreflight.outputShape] }),
        },
      }),
});

const firstValidationMessage = (
  validation: WorkflowExecutionSettingsValidation,
): string => {
  if (validation.generalError) return validation.generalError;
  const fieldOrder: WorkflowExecutionSettingsField[] = [
    'version',
    'mode',
    'windowShape',
    'maxInFlightWindows',
    'newRunRecoveryLocation',
    'anchorNodeId',
    'directory',
  ];
  for (const field of fieldOrder) {
    const message = validation.fieldErrors[field];
    if (message) return message;
  }
  return 'Execution settings are invalid.';
};

export const validateWorkflowExecutionSettings = (
  settings: WorkflowExecutionSettings,
  context: WorkflowExecutionSettingsValidationContext = {},
): WorkflowExecutionSettingsValidation => {
  const fieldErrors: WorkflowExecutionSettingsValidation['fieldErrors'] = {};
  let generalError: string | undefined;

  if (settings.version !== WORKFLOW_EXECUTION_SETTINGS_VERSION) {
    fieldErrors.version = `Unsupported execution settings version ${String(settings.version)}.`;
  }
  if (settings.mode !== 'full_graph' && settings.mode !== 'window') {
    fieldErrors.mode = 'Execution mode must be Full Graph or Window.';
  }

  if (settings.mode === 'window') {
    if (context.windowable === false) {
      generalError = context.windowUnavailableReason
        || 'Window execution is unavailable for this workflow.';
    }

    const windowShape = settings.windowShape;
    if (
      !Array.isArray(windowShape)
      || windowShape.length === 0
      || !windowShape.every(size => Number.isSafeInteger(size) && size > 0)
    ) {
      fieldErrors.windowShape = 'Window shape must contain positive integers.';
    } else if (context.outputShape !== undefined && context.outputShape !== null) {
      const outputShape = [...context.outputShape];
      if (!isValidOutputShape(outputShape)) {
        fieldErrors.windowShape = 'The current output shape is invalid.';
      } else if (!isValidWindowShape(outputShape, windowShape)) {
        fieldErrors.windowShape = (
          `The saved Window shape has rank ${windowShape.length}, `
          + `but the current output has rank ${outputShape.length}.`
        );
      }
    }

    if (
      settings.maxInFlightWindows !== undefined
      && !isValidMaxInFlightWindows(settings.maxInFlightWindows)
    ) {
      fieldErrors.maxInFlightWindows = (
        'Maximum in-flight Windows must be a positive integer.'
      );
    }

    const location = settings.newRunRecoveryLocation;
    if (!location) {
      fieldErrors.newRunRecoveryLocation = (
        'Choose Output Sidecar or a custom recovery directory for new Window runs.'
      );
    } else if (location.mode === 'output_sidecar') {
      if (!location.anchorNodeId.trim()) {
        fieldErrors.anchorNodeId = 'Select a terminal output for the recovery sidecar.';
      } else if (
        context.availableAnchorNodeIds !== undefined
        && !context.availableAnchorNodeIds.includes(location.anchorNodeId)
      ) {
        fieldErrors.anchorNodeId = (
          'The saved output anchor no longer exists. Select another terminal output node.'
        );
      }
    } else if (location.mode === 'custom') {
      if (!location.directory.trim()) {
        fieldErrors.directory = 'Enter an absolute custom recovery directory.';
      } else if (!isAbsoluteServerPath(location.directory)) {
        fieldErrors.directory = 'The custom recovery directory must be an absolute server path.';
      } else if (context.customDirectoryError) {
        fieldErrors.directory = context.customDirectoryError;
      }
    } else {
      fieldErrors.newRunRecoveryLocation = (
        'Choose Output Sidecar or a custom recovery directory for new Window runs.'
      );
    }

  }

  if (context.resourcesSatisfied === false) {
    generalError = context.resourceError
      || 'The active Dask cluster cannot satisfy this workflow.';
  }

  return {
    isValid: Object.keys(fieldErrors).length === 0 && generalError === undefined,
    fieldErrors,
    ...(generalError === undefined ? {} : { generalError }),
  };
};

const sanitizeLastPreflight = (value: unknown): LastPreflightSummary | undefined => {
  if (!isRecord(value)) return undefined;
  const summary: LastPreflightSummary = {};
  if (isValidOutputShape(value.outputShape)) {
    summary.outputShape = [...value.outputShape];
  }
  if (Number.isSafeInteger(value.totalWindows) && Number(value.totalWindows) >= 0) {
    summary.totalWindows = Number(value.totalWindows);
  }
  if (Number.isSafeInteger(value.cpuWorkers) && Number(value.cpuWorkers) >= 0) {
    summary.cpuWorkers = Number(value.cpuWorkers);
  }
  if (Number.isSafeInteger(value.gpuWorkers) && Number(value.gpuWorkers) >= 0) {
    summary.gpuWorkers = Number(value.gpuWorkers);
  }
  if (typeof value.validatedAt === 'number' && Number.isFinite(value.validatedAt)) {
    summary.validatedAt = value.validatedAt;
  }
  return Object.keys(summary).length === 0 ? undefined : summary;
};

const sanitizeRecoveryLocation = (
  value: unknown,
): { location?: RecoveryLocation; error?: string } => {
  if (!isRecord(value)) {
    return { error: 'Choose a recovery storage location for new Window runs.' };
  }
  if (value.mode === 'output_sidecar') {
    return typeof value.anchorNodeId === 'string'
      ? { location: { mode: 'output_sidecar', anchorNodeId: value.anchorNodeId } }
      : {
          location: { mode: 'output_sidecar', anchorNodeId: '' },
          error: 'Select a terminal output for the recovery sidecar.',
        };
  }
  if (value.mode === 'custom') {
    return typeof value.directory === 'string'
      ? { location: { mode: 'custom', directory: value.directory } }
      : {
          location: { mode: 'custom', directory: '' },
          error: 'Enter an absolute custom recovery directory.',
        };
  }
  return { error: 'Choose Output Sidecar or a custom recovery directory.' };
};

const mergeValidation = (
  base: WorkflowExecutionSettingsValidation,
  extraFieldErrors: WorkflowExecutionSettingsValidation['fieldErrors'],
  extraGeneralError?: string,
): WorkflowExecutionSettingsValidation => {
  const fieldErrors = { ...base.fieldErrors, ...extraFieldErrors };
  const generalError = extraGeneralError ?? base.generalError;
  return {
    isValid: Object.keys(fieldErrors).length === 0 && generalError === undefined,
    fieldErrors,
    ...(generalError === undefined ? {} : { generalError }),
  };
};

/**
 * Whitelist and normalize persisted settings. Unknown keys (including old
 * resume/restart state) are intentionally discarded.
 */
export const sanitizeWorkflowExecutionSettings = (
  value: unknown,
): SanitizedWorkflowExecutionSettings => {
  if (!isRecord(value)) {
    const settings = defaultSettings();
    return {
      settings,
      validation: {
        isValid: false,
        fieldErrors: {},
        generalError: 'Saved execution settings are not a valid object.',
      },
    };
  }

  const fieldErrors: WorkflowExecutionSettingsValidation['fieldErrors'] = {};
  if (value.version !== WORKFLOW_EXECUTION_SETTINGS_VERSION) {
    fieldErrors.version = value.version === undefined
      ? 'Saved execution settings have no schema version.'
      : `Unsupported execution settings version ${String(value.version)}.`;
  }

  const mode = value.mode === 'window' || value.mode === 'full_graph'
    ? value.mode
    : 'full_graph';
  if (value.mode !== 'window' && value.mode !== 'full_graph') {
    fieldErrors.mode = 'Execution mode must be Full Graph or Window.';
  }

  const settings: WorkflowExecutionSettings = {
    version: WORKFLOW_EXECUTION_SETTINGS_VERSION,
    mode,
  };

  if (Array.isArray(value.windowShape)) {
    if (value.windowShape.every(size => typeof size === 'number' && Number.isFinite(size))) {
      settings.windowShape = [...value.windowShape] as number[];
    } else if (mode === 'window') {
      fieldErrors.windowShape = 'Window shape must contain only numbers.';
    }
  } else if (value.windowShape !== undefined && mode === 'window') {
    fieldErrors.windowShape = 'Window shape must be an array.';
  }

  if (typeof value.maxInFlightWindows === 'number' && Number.isFinite(value.maxInFlightWindows)) {
    settings.maxInFlightWindows = value.maxInFlightWindows;
  } else if (value.maxInFlightWindows !== undefined && mode === 'window') {
    fieldErrors.maxInFlightWindows = 'Maximum in-flight Windows must be a number.';
  }

  if (value.newRunRecoveryLocation !== undefined) {
    const sanitizedLocation = sanitizeRecoveryLocation(value.newRunRecoveryLocation);
    if (sanitizedLocation.location) {
      settings.newRunRecoveryLocation = sanitizedLocation.location;
    }
    if (sanitizedLocation.error && mode === 'window') {
      const field = sanitizedLocation.location?.mode === 'output_sidecar'
        ? 'anchorNodeId'
        : sanitizedLocation.location?.mode === 'custom'
          ? 'directory'
          : 'newRunRecoveryLocation';
      fieldErrors[field] = sanitizedLocation.error;
    }
  }

  const lastPreflight = sanitizeLastPreflight(value.lastPreflight);
  if (lastPreflight) settings.lastPreflight = lastPreflight;

  const semanticValidation = validateWorkflowExecutionSettings(settings);
  return {
    settings,
    validation: mergeValidation(semanticValidation, fieldErrors),
  };
};

export const workflowExecutionSettingsStorageKey = (workflowId: string): string => {
  const normalized = workflowId.trim();
  if (!normalized) throw new Error('workflowId must be a non-empty string.');
  return `${WORKFLOW_EXECUTION_SETTINGS_STORAGE_PREFIX}${normalized}`;
};

const browserStorage = (): WorkflowExecutionSettingsStorage | null => {
  try {
    return typeof localStorage === 'undefined' ? null : localStorage;
  } catch {
    return null;
  }
};

export const loadLocalWorkflowExecutionSettings = (
  workflowId: string,
  storage: WorkflowExecutionSettingsStorage | null = browserStorage(),
): SanitizedWorkflowExecutionSettings | null => {
  if (!storage) return null;
  let raw: string | null;
  try {
    raw = storage.getItem(workflowExecutionSettingsStorageKey(workflowId));
  } catch (error) {
    return {
      settings: defaultSettings(),
      validation: {
        isValid: false,
        fieldErrors: {},
        generalError: `Unable to read locally saved execution settings: ${(error as Error).message}`,
      },
    };
  }
  if (raw === null) return null;
  try {
    return sanitizeWorkflowExecutionSettings(JSON.parse(raw));
  } catch {
    return {
      settings: defaultSettings(),
      validation: {
        isValid: false,
        fieldErrors: {},
        generalError: 'Locally saved execution settings contain invalid JSON.',
      },
    };
  }
};

export const saveLocalWorkflowExecutionSettings = (
  workflowId: string,
  settings: WorkflowExecutionSettings,
  storage: WorkflowExecutionSettingsStorage | null = browserStorage(),
): void => {
  if (!storage) return;
  const sanitized = sanitizeWorkflowExecutionSettings(settings).settings;
  storage.setItem(
    workflowExecutionSettingsStorageKey(workflowId),
    JSON.stringify(sanitized),
  );
};

/**
 * Commit primary workflow metadata first, then mirror it to best-effort
 * secondary stores. A browser quota/security failure must never block either
 * the primary state update or an otherwise valid Run submission.
 */
export const commitWorkflowExecutionSettings = (
  value: unknown,
  writePrimary: WorkflowExecutionSettingsWriter,
  secondaryWriters: readonly WorkflowExecutionSettingsWriter[] = [],
  onSecondaryError: (error: unknown) => void = () => undefined,
): WorkflowExecutionSettings => {
  const settings = sanitizeWorkflowExecutionSettings(value).settings;
  writePrimary(settings);
  secondaryWriters.forEach(writer => {
    try {
      writer(settings);
    } catch (error) {
      onSecondaryError(error);
    }
  });
  return settings;
};

/**
 * Patch only execution metadata in the active autosave document, preserving
 * its graph and unrelated metadata. This closes the debounce window in which
 * stale autosave metadata could outrank a freshly written per-workflow cache
 * after an immediate browser refresh.
 */
export const mirrorWorkflowExecutionSettingsIntoAutosave = ({
  storage,
  autosaveKey,
  workflowId,
  settings,
}: {
  storage: WorkflowExecutionSettingsStorage;
  autosaveKey: string;
  workflowId: string;
  settings: WorkflowExecutionSettings;
}): boolean => {
  const serialized = storage.getItem(autosaveKey);
  if (!serialized) return false;

  const parsed: unknown = JSON.parse(serialized);
  if (!isRecord(parsed)) return false;
  const savedWorkflowId = typeof parsed.workflowId === 'string'
    ? parsed.workflowId.trim()
    : '';
  if (savedWorkflowId && savedWorkflowId !== workflowId) return false;
  const metadata = isRecord(parsed.metadata) ? parsed.metadata : {};
  const sanitized = sanitizeWorkflowExecutionSettings(settings).settings;
  storage.setItem(autosaveKey, JSON.stringify({
    ...parsed,
    workflowId,
    metadata: {
      ...metadata,
      executionSettings: sanitized,
    },
  }));
  return true;
};

export const removeLocalWorkflowExecutionSettings = (
  workflowId: string,
  storage: WorkflowExecutionSettingsStorage | null = browserStorage(),
): void => {
  storage?.removeItem?.(workflowExecutionSettingsStorageKey(workflowId));
};

/** Resolve metadata -> local storage -> defaults without fingerprint keys. */
export const resolveWorkflowExecutionSettings = ({
  workflowId,
  metadata,
  storage = browserStorage(),
  defaults = defaultSettings(),
}: {
  workflowId: string;
  metadata?: unknown;
  storage?: WorkflowExecutionSettingsStorage | null;
  defaults?: WorkflowExecutionSettings;
}): ResolvedWorkflowExecutionSettings => {
  if (isRecord(metadata) && Object.hasOwn(metadata, 'executionSettings')) {
    const resolved = sanitizeWorkflowExecutionSettings(metadata.executionSettings);
    return {
      settings: resolved.settings,
      source: 'metadata',
      isConfigured: true,
      validation: resolved.validation,
    };
  }

  const local = loadLocalWorkflowExecutionSettings(workflowId, storage);
  if (local) {
    return {
      settings: local.settings,
      source: 'local',
      isConfigured: true,
      validation: local.validation,
    };
  }

  const normalizedDefaults = sanitizeWorkflowExecutionSettings(defaults);
  return {
    settings: normalizedDefaults.settings,
    source: 'default',
    isConfigured: false,
    validation: normalizedDefaults.validation,
  };
};

/**
 * Convert persistent ordinary-run settings to the WebSocket wire payload.
 * Recovery actions cannot leak through because every field is reconstructed
 * and Window execution always receives resumeAction="new".
 */
export const buildNewRunExecutionConfig = (
  value: unknown,
  context: WorkflowExecutionSettingsValidationContext = {},
): ExecutionConfig => {
  const sanitized = sanitizeWorkflowExecutionSettings(value);
  const validation = mergeValidation(
    validateWorkflowExecutionSettings(sanitized.settings, context),
    sanitized.validation.fieldErrors,
    sanitized.validation.generalError,
  );
  if (!validation.isValid) throw new Error(firstValidationMessage(validation));

  const settings = sanitized.settings;
  if (settings.mode === 'full_graph') return { mode: 'full_graph' };

  const location = settings.newRunRecoveryLocation;
  if (!settings.windowShape || !location) {
    throw new Error('Window execution settings are incomplete.');
  }
  return {
    mode: 'window',
    windowShape: [...settings.windowShape],
    ...(settings.maxInFlightWindows === undefined
      ? {}
      : { maxInFlightWindows: settings.maxInFlightWindows }),
    resumeAction: 'new',
    recoveryLocation: location.mode === 'output_sidecar'
      ? { mode: 'output_sidecar', anchorNodeId: location.anchorNodeId }
      : { mode: 'custom', directory: location.directory },
  };
};

export const lastPreflightSummaryFromResponse = (
  preflight: ExecutionPreflightResponse,
  validatedAt = Date.now(),
): LastPreflightSummary => {
  const summary: LastPreflightSummary = {};
  const outputShape = preflightOutputShape(preflight);
  if (outputShape.length > 0) summary.outputShape = [...outputShape];
  if (Number.isSafeInteger(preflight.totalWindows) && Number(preflight.totalWindows) >= 0) {
    summary.totalWindows = Number(preflight.totalWindows);
  }
  const cpuWorkers = preflight.requiredResources?.cpuWorkers;
  const gpuWorkers = preflight.requiredResources?.gpuWorkers;
  if (Number.isSafeInteger(cpuWorkers) && Number(cpuWorkers) >= 0) {
    summary.cpuWorkers = Number(cpuWorkers);
  }
  if (Number.isSafeInteger(gpuWorkers) && Number(gpuWorkers) >= 0) {
    summary.gpuWorkers = Number(gpuWorkers);
  }
  if (Number.isFinite(validatedAt)) summary.validatedAt = validatedAt;
  return summary;
};

export const withLastPreflightSummary = (
  settings: WorkflowExecutionSettings,
  preflight: ExecutionPreflightResponse,
  validatedAt = Date.now(),
): WorkflowExecutionSettings => ({
  ...cloneWorkflowExecutionSettings(settings),
  lastPreflight: lastPreflightSummaryFromResponse(preflight, validatedAt),
});

export const formatWorkflowExecutionSettingsSummary = (
  settings: WorkflowExecutionSettings,
): string => {
  if (settings.mode === 'full_graph') return 'Full Graph';
  const shape = settings.windowShape?.length
    ? ` · ${settings.windowShape.join('×')}`
    : '';
  const inFlight = settings.maxInFlightWindows === undefined
    ? ''
    : ` · In-flight ${settings.maxInFlightWindows}`;
  return `Window${shape}${inFlight}`;
};

/** Generate a stable UUID for a genuinely new or migrated legacy workflow. */
export const createWorkflowId = (): string => {
  const cryptoObject = globalThis.crypto;
  if (typeof cryptoObject?.randomUUID === 'function') {
    return cryptoObject.randomUUID();
  }

  const bytes = new Uint8Array(16);
  if (typeof cryptoObject?.getRandomValues === 'function') {
    cryptoObject.getRandomValues(bytes);
  } else {
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Math.floor(Math.random() * 256);
    }
  }
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map(byte => byte.toString(16).padStart(2, '0'));
  return [
    hex.slice(0, 4).join(''),
    hex.slice(4, 6).join(''),
    hex.slice(6, 8).join(''),
    hex.slice(8, 10).join(''),
    hex.slice(10, 16).join(''),
  ].join('-');
};
