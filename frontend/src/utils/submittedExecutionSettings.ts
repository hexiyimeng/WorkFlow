import type { SubmittedExecutionSettings } from '../types.ts';

const STORAGE_KEY = 'WorkFlow_submitted_execution_settings';

export const copySubmittedExecutionSettings = (
  settings: SubmittedExecutionSettings,
): SubmittedExecutionSettings => JSON.parse(JSON.stringify(settings));

export const rememberSubmittedExecutionSettings = (settings: SubmittedExecutionSettings): void => {
  try { sessionStorage.setItem(STORAGE_KEY, JSON.stringify(settings)); }
  catch { /* A storage restriction must not prevent execution. */ }
};

export const readSubmittedExecutionSettings = (): SubmittedExecutionSettings | null => {
  try {
    const settings = JSON.parse(sessionStorage.getItem(STORAGE_KEY) ?? 'null');
    if (!settings || typeof settings.executionId !== 'string' || !settings.executionId
      || settings.executionId !== sessionStorage.getItem('WorkFlow_execution_id')
      || !['full_graph', 'window'].includes(settings.config?.mode)
      || !Array.isArray(settings.workerProfiles) || !Array.isArray(settings.workerPools)) return null;
    if (settings.config.mode === 'window' && (
      (settings.config.windowShape !== undefined && (!Array.isArray(settings.config.windowShape)
        || !settings.config.windowShape.every((n: unknown) => Number.isSafeInteger(n) && Number(n) > 0)))
      || !['new', 'resume', 'restart'].includes(settings.config.resumeAction)
      || !['custom', 'output_sidecar'].includes(settings.config.recoveryLocation?.mode)
      || (settings.config.maxInFlightWindows !== undefined && (!Number.isSafeInteger(settings.config.maxInFlightWindows) || settings.config.maxInFlightWindows < 1))
      || (settings.config.recoveryLocation.mode === 'custom'
        ? typeof settings.config.recoveryLocation.directory !== 'string'
        : typeof settings.config.recoveryLocation.anchorNodeId !== 'string')
    )) return null;
    if (!settings.workerProfiles.every((profile: SubmittedExecutionSettings['workerProfiles'][number]) => (
      profile && typeof profile.name === 'string' && typeof profile.physical_resources?.cpu === 'number'
      && typeof profile.physical_resources.memory === 'string' && typeof profile.physical_resources.gpu === 'number'
      && typeof profile.threads === 'number'
    )) || !settings.workerPools.every((pool: SubmittedExecutionSettings['workerPools'][number]) => (
      pool && typeof pool.profile === 'string' && typeof pool.processes === 'number'
      && typeof pool.minimum_jobs === 'number' && typeof pool.maximum_jobs === 'number'
    ))) return null;
    if (settings.preflight != null) {
      if (typeof settings.preflight !== 'object' || Array.isArray(settings.preflight)) return null;
      for (const key of ['outputShape', 'output_shape']) {
        const shape = settings.preflight[key];
        if (shape != null && (!Array.isArray(shape) || !shape.every((n: unknown) => Number.isSafeInteger(n) && Number(n) >= 0))) return null;
      }
    }
    return settings;
  } catch { return null; }
};
