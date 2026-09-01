import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
} from 'react';
import { useFlow } from '../../hooks/useFlowContext';
import type {
  ExecutionMode,
  ExecutionPreflightResponse,
  WorkflowExecutionSettings,
  WorkflowExecutionSettingsField,
  WorkerPool,
  WorkerProfile,
} from '../../types';
import { isAbsoluteServerPath } from '../../utils/executionConfig';
import {
  defaultWorkerPool,
  defaultWorkerProfile,
  fixedGpuForWorkerProfile,
  loadWorkerPools,
  loadWorkerProfiles,
  saveRequiredWorkerResources,
  synchronizeLogicalResources,
} from '../../utils/workerResources';
import { Button } from '../ui/Button';

const DEFAULT_SETTINGS: WorkflowExecutionSettings = {
  version: 1,
  mode: 'full_graph',
};

type CheckpointMode = 'output_sidecar' | 'custom';

interface SettingsDraft {
  mode: ExecutionMode;
  windowShapeInputs: string[];
  maxInFlightInput: string;
  checkpointMode: CheckpointMode;
  hasCheckpointLocation: boolean;
  anchorNodeId: string;
  customDirectory: string;
}

const preflightOutputShape = (
  preflight: ExecutionPreflightResponse | null,
): number[] => {
  const shape = preflight?.outputShape ?? preflight?.output_shape;
  return Array.isArray(shape) ? [...shape] : [];
};

const settingsToDraft = (
  settings: WorkflowExecutionSettings,
  preflight: ExecutionPreflightResponse | null,
): SettingsDraft => {
  const location = settings.newRunRecoveryLocation;
  const shape = settings.windowShape
    ?? (settings.mode === 'window'
      ? preflightOutputShape(preflight).map(() => 1)
      : []);
  return {
    mode: settings.mode,
    windowShapeInputs: shape.map(size => String(size)),
    maxInFlightInput: settings.maxInFlightWindows === undefined
      ? (settings.mode === 'window' ? '1' : '')
      : String(settings.maxInFlightWindows),
    checkpointMode: location?.mode ?? 'output_sidecar',
    hasCheckpointLocation: location !== undefined,
    anchorNodeId: location?.mode === 'output_sidecar' ? location.anchorNodeId : '',
    customDirectory: location?.mode === 'custom' ? location.directory : '',
  };
};

const draftToSettings = (
  draft: SettingsDraft,
  previous: WorkflowExecutionSettings,
): WorkflowExecutionSettings => {
  const settings: WorkflowExecutionSettings = {
    version: 1,
    mode: draft.mode,
    ...(previous.lastPreflight === undefined
      ? {}
      : {
          lastPreflight: {
            ...previous.lastPreflight,
            ...(previous.lastPreflight.outputShape === undefined
              ? {}
              : { outputShape: [...previous.lastPreflight.outputShape] }),
          },
        }),
  };
  if (draft.windowShapeInputs.length > 0) {
    settings.windowShape = draft.windowShapeInputs.map(value => Number(value));
  }
  if (draft.maxInFlightInput.trim() !== '') {
    settings.maxInFlightWindows = Number(draft.maxInFlightInput);
  }
  if (draft.hasCheckpointLocation) {
    settings.newRunRecoveryLocation = draft.checkpointMode === 'output_sidecar'
      ? { mode: 'output_sidecar', anchorNodeId: draft.anchorNodeId }
      : { mode: 'custom', directory: draft.customDirectory };
  }
  return settings;
};

const axisLabel = (index: number, rank: number): string => {
  const spatialAxes = ['Z', 'Y', 'X'];
  if (rank > 0 && rank <= spatialAxes.length) {
    return spatialAxes[spatialAxes.length - rank + index];
  }
  return `Axis ${index + 1}`;
};

const FieldError = ({ message }: { message?: string }) => (
  message ? (
    <p className="mt-1.5 text-[10px]" style={{ color: 'var(--color-danger)' }}>
      {message}
    </p>
  ) : null
);

type WorkerDraftField = 'cpu' | 'memoryGB' | 'gpu' | 'processes' | 'scale';
type WorkerDraftErrors = Partial<Record<WorkerDraftField, string>>;

const WORKER_FIELD_LABELS: Record<WorkerDraftField, string> = {
  cpu: 'CPU / Worker',
  memoryGB: 'Memory / Worker',
  gpu: 'GPU / Worker',
  processes: 'Processes / Job',
  scale: 'Scale (Slurm Jobs)',
};

interface WorkerResourceDraft {
  cpu: string;
  memoryGB: string;
  gpu: string;
  processes: string;
  scale: string;
}

const memoryAmount = (value: string): string => (
  value.trim().match(/^([0-9]+(?:\.[0-9]+)?)\s*(?:GB|GiB)$/i)?.[1] ?? ''
);

const positiveIntegerDraft = (value: string): number | null => {
  const parsed = Number(value);
  return value.trim() !== '' && Number.isSafeInteger(parsed) && parsed > 0
    ? parsed
    : null;
};

const positiveNumberDraft = (value: string): number | null => {
  const parsed = Number(value);
  return value.trim() !== '' && Number.isFinite(parsed) && parsed > 0
    ? parsed
    : null;
};

const nonnegativeIntegerDraft = (value: string): number | null => {
  const parsed = Number(value);
  return value.trim() !== '' && Number.isSafeInteger(parsed) && parsed >= 0
    ? parsed
    : null;
};

const WorkerResourcesSection = ({
  requiredProfiles,
  disabled,
  onSaved,
}: {
  requiredProfiles: string[];
  disabled: boolean;
  onSaved: () => Promise<void>;
}) => {
  const [profiles, setProfiles] = useState<WorkerProfile[]>([]);
  const [pools, setPools] = useState<WorkerPool[]>([]);
  const [drafts, setDrafts] = useState<Record<string, WorkerResourceDraft>>({});
  const [fieldErrors, setFieldErrors] = useState<Record<string, WorkerDraftErrors>>({});
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const requirementKey = requiredProfiles.join('|');
  useEffect(() => {
    const profileNames = requirementKey ? requirementKey.split('|') : [];
    const savedProfiles = new Map(loadWorkerProfiles().map(profile => [profile.name, profile]));
    const savedPools = new Map(loadWorkerPools().map(pool => [pool.profile, pool]));
    const nextProfiles = profileNames.map(
      name => savedProfiles.get(name) ?? defaultWorkerProfile(name),
    );
    const nextPools = profileNames.map(
      name => savedPools.get(name) ?? defaultWorkerPool(name),
    );
    const poolByName = new Map(nextPools.map(pool => [pool.profile, pool]));
    setProfiles(nextProfiles);
    setPools(nextPools);
    setDrafts(Object.fromEntries(nextProfiles.map(profile => {
      const pool = poolByName.get(profile.name) ?? defaultWorkerPool(profile.name);
      return [profile.name, {
        cpu: String(profile.physical_resources.cpu),
        memoryGB: memoryAmount(profile.physical_resources.memory),
        gpu: String(profile.physical_resources.gpu),
        processes: String(pool.processes),
        scale: String(pool.scale),
      }];
    })));
    setFieldErrors({});
    setError(null);
  }, [requirementKey]);

  const updateDraft = (name: string, field: WorkerDraftField, value: string) => {
    setDrafts(current => ({
      ...current,
      [name]: {
        ...(current[name] ?? {
          cpu: '', memoryGB: '', gpu: '', processes: '', scale: '',
        }),
        [field]: value,
      },
    }));
    setFieldErrors(current => ({
      ...current,
      [name]: { ...current[name], [field]: undefined },
    }));
    setError(null);
  };
  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      const nextErrors: Record<string, WorkerDraftErrors> = {};
      const nextProfiles = profiles.map(profile => {
        const draft = drafts[profile.name];
        const errors: WorkerDraftErrors = {};
        const cpu = positiveIntegerDraft(draft?.cpu ?? '');
        const memoryGB = positiveNumberDraft(draft?.memoryGB ?? '');
        const fixedGpu = fixedGpuForWorkerProfile(profile.name);
        const gpu = fixedGpu ?? nonnegativeIntegerDraft(draft?.gpu ?? '');
        if (cpu === null) errors.cpu = 'Enter a positive whole number.';
        if (memoryGB === null) errors.memoryGB = 'Enter a positive number.';
        if (gpu === null || gpu > 1) errors.gpu = 'Enter 0 or 1.';
        if (Object.keys(errors).length > 0) nextErrors[profile.name] = errors;
        return synchronizeLogicalResources({
          ...profile,
          physical_resources: {
            cpu: cpu ?? 0,
            memory: `${memoryGB ?? 0}GB`,
            gpu: gpu ?? 0,
          },
          threads: cpu ?? 0,
        });
      });
      const nextPools = pools.map(pool => {
        const draft = drafts[pool.profile];
        const profile = nextProfiles.find(item => item.name === pool.profile);
        const processes = positiveIntegerDraft(draft?.processes ?? '');
        const scale = positiveIntegerDraft(draft?.scale ?? '');
        const errors = nextErrors[pool.profile] ?? {};
        if (processes === null) errors.processes = 'Enter a positive whole number.';
        if (scale === null) errors.scale = 'Enter a positive whole number.';
        if (profile?.physical_resources.gpu && processes !== 1) {
          errors.processes = 'GPU Pools require exactly 1 process per Job.';
        }
        if (Object.keys(errors).length > 0) nextErrors[pool.profile] = errors;
        return {
          ...pool,
          processes: profile?.physical_resources.gpu ? 1 : processes ?? 0,
          scale: scale ?? 0,
        };
      });
      setFieldErrors(nextErrors);
      const firstInvalid = Object.entries(nextErrors)[0];
      if (firstInvalid) {
        const [profileName, errors] = firstInvalid;
        const firstError = Object.entries(errors).find(([, message]) => Boolean(message));
        const field = firstError?.[0] as WorkerDraftField | undefined;
        const message = firstError?.[1];
        throw new Error(
          `Worker Profile "${profileName}" — ${field ? WORKER_FIELD_LABELS[field] : 'invalid'}: ${message}`,
        );
      }
      saveRequiredWorkerResources(nextProfiles, nextPools);
      setProfiles(nextProfiles);
      setPools(nextPools);
      await onSaved();
    } catch (caught) {
      setError((caught as Error).message);
    } finally {
      setSaving(false);
    }
  };

  if (requiredProfiles.length === 0) {
    return (
      <section className="rounded-[var(--radius-md)] border px-3 py-3 text-[10px]"
        style={{ borderColor: 'var(--color-border-subtle)' }}>
        Run preflight to discover the Worker Profiles required by this workflow.
      </section>
    );
  }

  return (
    <section className="space-y-3 rounded-[var(--radius-md)] border px-3 py-3"
      style={{ borderColor: 'var(--color-border-subtle)' }}>
      <div>
        <h3 className="text-[11px] font-semibold">Worker Profiles and Pools</h3>
        <p className="mt-1 text-[9px]" style={{ color: 'var(--color-text-muted)' }}>
          Required by the current workflow. Saved only in this browser.
        </p>
      </div>
      {profiles.map(profile => {
        const draft = drafts[profile.name] ?? {
          cpu: '', memoryGB: '', gpu: '', processes: '', scale: '',
        };
        const errors = fieldErrors[profile.name] ?? {};
        const fixedGpu = fixedGpuForWorkerProfile(profile.name);
        const gpuProfile = fixedGpu === 1 || (fixedGpu === undefined && draft.gpu === '1');
        const processes = positiveIntegerDraft(draft.processes);
        const scale = positiveIntegerDraft(draft.scale);
        const totalWorkers = processes !== null && scale !== null
          ? scale * (gpuProfile ? 1 : processes)
          : null;
        return (
          <fieldset key={profile.name} disabled={disabled || saving}
            className="rounded border p-2" style={{ borderColor: 'var(--color-border-subtle)' }}>
            <legend className="px-1 font-mono text-[10px] font-semibold">{profile.name}</legend>
            <div className="grid grid-cols-2 gap-2 text-[9px]">
              <label>CPU / Worker
                <input type="number" min="1" step="1" value={draft.cpu}
                  aria-invalid={Boolean(errors.cpu)}
                  onChange={event => updateDraft(profile.name, 'cpu', event.target.value)}
                  className="mt-1 h-8 w-full rounded border bg-[var(--color-bg-field)] px-2" />
                <FieldError message={errors.cpu} />
              </label>
              <label>Memory / Worker
                <span className="mt-1 flex h-8 overflow-hidden rounded border bg-[var(--color-bg-field)]">
                  <input type="number" min="0" step="any" value={draft.memoryGB}
                    aria-invalid={Boolean(errors.memoryGB)}
                    placeholder="32"
                    onChange={event => updateDraft(profile.name, 'memoryGB', event.target.value)}
                    className="min-w-0 flex-1 bg-transparent px-2 outline-none" />
                  <span className="flex items-center border-l px-2 font-mono"
                    style={{ borderColor: 'var(--color-border-subtle)', color: 'var(--color-text-muted)' }}>
                    GB
                  </span>
                </span>
                <FieldError message={errors.memoryGB} />
              </label>
              <label>GPU / Worker{fixedGpu === undefined ? '' : ' (fixed)'}
                <input type="number" min="0" max="1" step="1"
                  value={fixedGpu === undefined ? draft.gpu : String(fixedGpu)}
                  aria-invalid={Boolean(errors.gpu)}
                  disabled={disabled || saving || fixedGpu !== undefined}
                  onChange={event => {
                    const gpu = event.target.value;
                    updateDraft(profile.name, 'gpu', gpu);
                    if (gpu === '1') updateDraft(profile.name, 'processes', '1');
                  }}
                  className="mt-1 h-8 w-full rounded border bg-[var(--color-bg-field)] px-2" />
                <FieldError message={errors.gpu} />
              </label>
              <div>Threads / Worker
                <div className="mt-1 flex h-8 items-center rounded border px-2 font-mono"
                  style={{ borderColor: 'var(--color-border-subtle)', color: 'var(--color-text-muted)' }}>
                  {positiveIntegerDraft(draft.cpu) ?? '—'} (derived from CPU / Worker)
                </div>
              </div>
              <label>Processes / Job
                <input type="number" min="1" step="1" value={gpuProfile ? '1' : draft.processes}
                  aria-invalid={Boolean(errors.processes)}
                  disabled={disabled || saving || gpuProfile}
                  onChange={event => updateDraft(profile.name, 'processes', event.target.value)}
                  className="mt-1 h-8 w-full rounded border bg-[var(--color-bg-field)] px-2 disabled:opacity-60" />
                <FieldError message={errors.processes} />
              </label>
              <label>Scale (Slurm Jobs)
                <input type="number" min="1" step="1" value={draft.scale}
                  aria-invalid={Boolean(errors.scale)}
                  onChange={event => updateDraft(profile.name, 'scale', event.target.value)}
                  className="mt-1 h-8 w-full rounded border bg-[var(--color-bg-field)] px-2" />
                <FieldError message={errors.scale} />
              </label>
            </div>
            <p className="mt-2 text-[9px]" style={{ color: 'var(--color-text-muted)' }}>
              Total Workers: {totalWorkers ?? '—'}
            </p>
          </fieldset>
        );
      })}
      {error && <FieldError message={error} />}
      <div className="flex justify-end">
        <Button type="button" variant="secondary" size="sm" loading={saving}
          disabled={disabled} onClick={() => { void save(); }}>
          Save Worker Resources
        </Button>
      </div>
    </section>
  );
};

export default function ExecutionSettingsDrawer() {
  const {
    isExecutionSettingsOpen,
    activeWorkflowDocumentId,
    activeExecutionSettings,
    executionSettingsValidation,
    executionPreflight,
    closeExecutionSettings,
    saveExecutionSettings,
    refreshExecutionSettingsPreflight,
    isConnected,
    isExecuting,
    isPreflighting,
  } = useFlow();

  const savedSettings = activeExecutionSettings ?? DEFAULT_SETTINGS;
  const [draft, setDraft] = useState<SettingsDraft>(() => (
    settingsToDraft(savedSettings, executionPreflight)
  ));
  const [dirtyFields, setDirtyFields] = useState<Set<WorkflowExecutionSettingsField>>(
    () => new Set(),
  );
  const [isSaving, setIsSaving] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const wasOpenRef = useRef(false);
  const draftWorkflowIdRef = useRef<string | null>(null);

  useEffect(() => {
    const workflowChanged = draftWorkflowIdRef.current !== activeWorkflowDocumentId;
    if (isExecutionSettingsOpen && (!wasOpenRef.current || workflowChanged)) {
      setDraft(settingsToDraft(
        savedSettings,
        workflowChanged ? null : executionPreflight,
      ));
      setDirtyFields(new Set());
      setActionError(null);
    }
    if (isExecutionSettingsOpen) {
      draftWorkflowIdRef.current = activeWorkflowDocumentId;
    }
    wasOpenRef.current = isExecutionSettingsOpen;
  }, [
    activeWorkflowDocumentId,
    executionPreflight,
    isExecutionSettingsOpen,
    savedSettings,
  ]);

  const outputShape = useMemo(
    () => preflightOutputShape(executionPreflight),
    [executionPreflight],
  );
  const outputs = executionPreflight?.outputs ?? [];
  const savedAnchorIsMissing = Boolean(
    draft.anchorNodeId
    && executionPreflight
    && !outputs.some(output => output.nodeId === draft.anchorNodeId),
  );

  const markDirty = (...fields: WorkflowExecutionSettingsField[]) => {
    setDirtyFields(current => {
      const next = new Set(current);
      fields.forEach(field => next.add(field));
      return next;
    });
    setActionError(null);
  };

  const contextualError = (field: WorkflowExecutionSettingsField): string | undefined => (
    dirtyFields.has(field)
      ? undefined
      : executionSettingsValidation?.fieldErrors[field]
  );

  const parsedWindowShape = draft.windowShapeInputs.map(value => Number(value));
  const localWindowShapeError = draft.mode !== 'window'
    ? undefined
    : draft.windowShapeInputs.length === 0
      ? 'Enter one Window dimension for every output axis.'
      : draft.windowShapeInputs.some(value => (
        value.trim() === ''
        || !Number.isSafeInteger(Number(value))
        || Number(value) <= 0
      ))
        ? 'Window dimensions must be positive integers.'
        : outputShape.length > 0 && parsedWindowShape.length !== outputShape.length
          ? `The saved Window shape has rank ${parsedWindowShape.length}, but the current output has rank ${outputShape.length}.`
          : undefined;
  const parsedMaxInFlight = draft.maxInFlightInput.trim() === ''
    ? undefined
    : Number(draft.maxInFlightInput);
  const localMaxInFlightError = draft.mode === 'window'
    && parsedMaxInFlight !== undefined
    && (!Number.isSafeInteger(parsedMaxInFlight) || parsedMaxInFlight <= 0)
    ? 'Maximum in-flight Windows must be a positive integer.'
    : undefined;
  const localAnchorError = draft.mode === 'window' && draft.checkpointMode === 'output_sidecar'
    ? !draft.anchorNodeId
      ? 'Select a terminal output node.'
      : savedAnchorIsMissing
        ? 'The saved output anchor no longer exists. Select another terminal output node.'
        : undefined
    : undefined;
  const localDirectoryError = draft.mode === 'window' && draft.checkpointMode === 'custom'
    ? !draft.customDirectory.trim()
      ? 'Enter an absolute server directory.'
      : !isAbsoluteServerPath(draft.customDirectory)
        ? 'The custom directory must be an absolute server path.'
        : undefined
    : undefined;

  const windowShapeError = localWindowShapeError ?? contextualError('windowShape');
  const maxInFlightError = localMaxInFlightError ?? contextualError('maxInFlightWindows');
  const anchorError = localAnchorError ?? contextualError('anchorNodeId');
  const directoryError = localDirectoryError ?? contextualError('directory');
  const locationError = contextualError('newRunRecoveryLocation');
  const modeError = contextualError('mode');
  const versionError = contextualError('version');
  const canSave = !isExecuting
    && !isSaving
    && !isPreflighting
    && !versionError
    && !modeError
    && (
    draft.mode === 'full_graph'
    || (
      !windowShapeError
      && !maxInFlightError
      && !anchorError
      && !directoryError
      && !locationError
    )
  );

  const currentDraftSettings = () => draftToSettings(draft, savedSettings);

  const handleSave = async (event: FormEvent) => {
    event.preventDefault();
    if (!canSave) return;
    setIsSaving(true);
    setActionError(null);
    try {
      const saved = await saveExecutionSettings(currentDraftSettings());
      if (saved) {
        closeExecutionSettings();
      } else {
        setDirtyFields(new Set());
      }
    } catch (error) {
      setDirtyFields(new Set());
      setActionError((error as Error).message);
    } finally {
      setIsSaving(false);
    }
  };

  const handleRefreshPreflight = async () => {
    setActionError(null);
    try {
      await refreshExecutionSettingsPreflight(currentDraftSettings());
      setDirtyFields(new Set());
    } catch (error) {
      setDirtyFields(new Set());
      setActionError((error as Error).message);
    }
  };

  const handleReset = () => {
    setDraft(settingsToDraft(DEFAULT_SETTINGS, executionPreflight));
    setDirtyFields(new Set([
      'mode',
      'windowShape',
      'maxInFlightWindows',
      'newRunRecoveryLocation',
      'anchorNodeId',
      'directory',
    ]));
    setActionError(null);
  };

  if (!isExecutionSettingsOpen) return null;

  const requiredResources = executionPreflight?.requiredResources;
  const availableResources = executionPreflight?.availableResources;
  const lastPreflight = savedSettings.lastPreflight;
  const currentPreflightFailed = Boolean(executionPreflight?.preflightError);
  // A current failed preflight must not be visually replaced by an older
  // successful summary. Cached values are useful only before a fresh response
  // exists, for example immediately after browser reload.
  const summaryOutputShape = executionPreflight
    ? (outputShape.length > 0 ? outputShape : undefined)
    : lastPreflight?.outputShape;
  const totalWindows = executionPreflight
    ? (Number.isSafeInteger(executionPreflight.totalWindows)
      ? Number(executionPreflight.totalWindows)
      : undefined)
    : lastPreflight?.totalWindows;
  const requiredWorkerProfiles = executionPreflight
    ? requiredResources?.requiredWorkerProfiles
    : lastPreflight?.requiredWorkerProfiles;
  const profileSummary = requiredWorkerProfiles
    ? Object.entries(requiredWorkerProfiles)
      .map(([profile, count]) => `${profile} (${count} nodes)`)
      .join(', ')
    : '—';
  const allocationPlan = executionPreflight?.allocationPlan;
  const workerProfileNames = Object.keys(requiredWorkerProfiles ?? {}).sort();
  const preflightErrorMessage = executionPreflight?.preflightError?.message?.trim();
  const generalError = actionError
    ?? executionSettingsValidation?.generalError
    ?? (preflightErrorMessage ? `Preflight failed: ${preflightErrorMessage}` : undefined)
    ?? (versionError
      ? `${versionError} Use Reset, review the values, and Save Settings to migrate explicitly.`
      : undefined)
    ?? (executionPreflight?.resourcesSatisfied === false
      ? executionPreflight.resourceError || 'The current cluster cannot satisfy this workflow.'
      : undefined);

  return (
    <aside
      role="dialog"
      aria-modal="false"
      aria-labelledby="execution-settings-title"
      className="fixed bottom-0 right-0 top-12 z-[90] flex w-full max-w-[440px] flex-col border-l"
      style={{
        backgroundColor: 'var(--color-bg-surface)',
        borderColor: 'var(--color-border-default)',
        boxShadow: 'var(--shadow-floating)',
      }}
    >
      <div
        className="flex items-start justify-between gap-4 border-b px-5 py-4"
        style={{ borderColor: 'var(--color-border-subtle)' }}
      >
        <div>
          <h2 id="execution-settings-title" className="text-[15px] font-semibold">
            Execution Settings
          </h2>
          <p className="mt-1 text-[10px]" style={{ color: 'var(--color-text-muted)' }}>
            Saved with this workflow and reused by the next Run.
          </p>
        </div>
        <button
          type="button"
          aria-label="Close execution settings"
          onClick={closeExecutionSettings}
          disabled={isSaving}
          className="flex h-7 w-7 items-center justify-center rounded-[var(--radius-md)] text-lg hover:bg-[var(--color-bg-field-hover)] disabled:opacity-50"
          style={{ color: 'var(--color-text-muted)' }}
        >
          ×
        </button>
      </div>

      <form onSubmit={handleSave} className="flex min-h-0 flex-1 flex-col">
        <div className="min-h-0 flex-1 space-y-5 overflow-y-auto px-5 py-5">
          {generalError && (
            <div
              role="alert"
              className="rounded-[var(--radius-md)] border px-3 py-2 text-[10px]"
              style={{
                color: 'var(--color-danger)',
                borderColor: 'var(--color-danger)',
                backgroundColor: 'var(--color-danger-soft)',
              }}
            >
              {generalError}
            </div>
          )}

          <fieldset disabled={isExecuting || isSaving}>
            <legend
              className="mb-2 text-[11px] font-semibold uppercase tracking-wide"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              Execution Mode
            </legend>
            <div className="grid grid-cols-2 gap-2">
              {([
                ['full_graph', 'Full Graph'],
                ['window', 'Window'],
              ] as const).map(([value, label]) => (
                <label
                  key={value}
                  className="flex cursor-pointer items-center gap-2 rounded-[var(--radius-md)] border px-3 py-2.5 text-[11px]"
                  style={{
                    borderColor: draft.mode === value
                      ? 'var(--color-accent)'
                      : 'var(--color-border-default)',
                    backgroundColor: draft.mode === value
                      ? 'var(--color-accent-soft)'
                      : 'var(--color-bg-field)',
                  }}
                >
                  <input
                    type="radio"
                    name="execution-settings-mode"
                    value={value}
                    checked={draft.mode === value}
                    onChange={() => {
                      setDraft(current => ({
                        ...current,
                        mode: value,
                        ...(value === 'window' && !current.maxInFlightInput.trim()
                          ? { maxInFlightInput: '1' }
                          : {}),
                        ...(value === 'window' && current.windowShapeInputs.length === 0 && outputShape.length > 0
                          ? { windowShapeInputs: outputShape.map(() => '1') }
                          : {}),
                        ...(value === 'window' && !current.anchorNodeId && outputs[0]
                          ? {
                              hasCheckpointLocation: true,
                              checkpointMode: 'output_sidecar' as const,
                              anchorNodeId: outputs[0].nodeId,
                            }
                          : {}),
                      }));
                      markDirty(
                        'mode',
                        ...(value === 'window'
                          ? [
                              'windowShape',
                              'maxInFlightWindows',
                              'newRunRecoveryLocation',
                              'anchorNodeId',
                            ] as const
                          : []),
                      );
                    }}
                    className="accent-[var(--color-accent)]"
                  />
                  {label}
                </label>
              ))}
            </div>
            <FieldError message={modeError} />
          </fieldset>

          {draft.mode === 'window' && (
            <>
              <section>
                <div className="flex items-center justify-between gap-3">
                  <h3 className="text-[11px] font-semibold" style={{ color: 'var(--color-text-secondary)' }}>
                    Window Shape
                  </h3>
                  {outputShape.length > 0 && (
                    <span className="font-mono text-[9px]" style={{ color: 'var(--color-text-muted)' }}>
                      Output ({outputShape.join(', ')})
                    </span>
                  )}
                </div>
                {draft.windowShapeInputs.length > 0 ? (
                  <div className="mt-2 flex flex-wrap gap-2">
                    {draft.windowShapeInputs.map((value, index) => (
                      <label key={index} className="min-w-[72px] flex-1">
                        <span className="mb-1 block text-[9px]" style={{ color: 'var(--color-text-muted)' }}>
                          {axisLabel(index, draft.windowShapeInputs.length)}
                        </span>
                        <input
                          type="number"
                          min="1"
                          step="1"
                          inputMode="numeric"
                          value={value}
                          disabled={isExecuting || isSaving}
                          aria-label={`Window shape ${axisLabel(index, draft.windowShapeInputs.length)}`}
                          aria-invalid={Boolean(windowShapeError)}
                          onChange={event => {
                            const nextValue = event.target.value;
                            setDraft(current => ({
                              ...current,
                              windowShapeInputs: current.windowShapeInputs.map((item, itemIndex) => (
                                itemIndex === index ? nextValue : item
                              )),
                            }));
                            markDirty('windowShape');
                          }}
                          className="h-9 w-full rounded-[var(--radius-md)] border bg-[var(--color-bg-field)] px-2 text-center font-mono text-[12px] outline-none focus:border-[var(--color-border-focus)]"
                          style={{
                            color: 'var(--color-text-primary)',
                            borderColor: windowShapeError
                              ? 'var(--color-danger)'
                              : 'var(--color-border-default)',
                          }}
                        />
                      </label>
                    ))}
                  </div>
                ) : (
                  <div className="mt-2 rounded-[var(--radius-md)] border px-3 py-2 text-[10px]" style={{ borderColor: 'var(--color-border-subtle)' }}>
                    Refresh preflight to discover the current output rank.
                    {outputShape.length > 0 && (
                      <Button
                        type="button"
                        size="xs"
                        className="ml-2"
                        onClick={() => {
                          setDraft(current => ({
                            ...current,
                            windowShapeInputs: outputShape.map(() => '1'),
                          }));
                          markDirty('windowShape');
                        }}
                      >
                        Use Current Rank
                      </Button>
                    )}
                  </div>
                )}
                <FieldError message={windowShapeError} />
              </section>

              <label className="block">
                <span className="text-[11px] font-semibold" style={{ color: 'var(--color-text-secondary)' }}>
                  Maximum In-Flight Windows
                </span>
                <input
                  type="number"
                  min="1"
                  step="1"
                  inputMode="numeric"
                  value={draft.maxInFlightInput}
                  placeholder="1"
                  disabled={isExecuting || isSaving}
                  aria-invalid={Boolean(maxInFlightError)}
                  onChange={event => {
                    setDraft(current => ({ ...current, maxInFlightInput: event.target.value }));
                    markDirty('maxInFlightWindows');
                  }}
                  className="mt-2 h-9 w-full rounded-[var(--radius-md)] border bg-[var(--color-bg-field)] px-2 font-mono text-[12px] outline-none focus:border-[var(--color-border-focus)]"
                  style={{
                    color: 'var(--color-text-primary)',
                    borderColor: maxInFlightError
                      ? 'var(--color-danger)'
                      : 'var(--color-border-default)',
                  }}
                />
                <FieldError message={maxInFlightError} />
              </label>

              <fieldset disabled={isExecuting || isSaving}>
                <legend className="mb-2 text-[11px] font-semibold" style={{ color: 'var(--color-text-secondary)' }}>
                  Checkpoint Storage for New Runs
                </legend>
                <div className="space-y-3">
                  <label className="flex items-start gap-2 text-[11px]">
                    <input
                      type="radio"
                      name="checkpoint-location"
                      checked={draft.checkpointMode === 'output_sidecar'}
                      onChange={() => {
                        setDraft(current => ({
                          ...current,
                          checkpointMode: 'output_sidecar',
                          hasCheckpointLocation: true,
                        }));
                        markDirty('newRunRecoveryLocation', 'anchorNodeId');
                      }}
                      className="mt-0.5 accent-[var(--color-accent)]"
                    />
                    <span>
                      <span className="block font-medium">Output Sidecar</span>
                      <span className="text-[10px]" style={{ color: 'var(--color-text-muted)' }}>
                        Store checkpoints next to one terminal output.
                      </span>
                    </span>
                  </label>

                  {draft.checkpointMode === 'output_sidecar' && (
                    <div>
                      <label className="mb-1 block text-[9px]" htmlFor="execution-settings-anchor" style={{ color: 'var(--color-text-muted)' }}>
                        Anchor Output
                      </label>
                      <select
                        id="execution-settings-anchor"
                        value={draft.anchorNodeId}
                        aria-invalid={Boolean(anchorError)}
                        onChange={event => {
                          setDraft(current => ({
                            ...current,
                            hasCheckpointLocation: true,
                            anchorNodeId: event.target.value,
                          }));
                          markDirty('anchorNodeId', 'newRunRecoveryLocation');
                        }}
                        className="h-9 w-full rounded-[var(--radius-md)] border bg-[var(--color-bg-field)] px-2 text-[11px] outline-none focus:border-[var(--color-border-focus)]"
                        style={{
                          color: 'var(--color-text-primary)',
                          borderColor: anchorError
                            ? 'var(--color-danger)'
                            : 'var(--color-border-default)',
                        }}
                      >
                        <option value="">Select a terminal output</option>
                        {savedAnchorIsMissing && (
                          <option value={draft.anchorNodeId} disabled>
                            Missing output · {draft.anchorNodeId}
                          </option>
                        )}
                        {outputs.map(output => (
                          <option key={output.nodeId} value={output.nodeId}>
                            {output.displayName} · {output.path}
                          </option>
                        ))}
                      </select>
                      <FieldError message={anchorError} />
                    </div>
                  )}

                  <label className="flex items-center gap-2 text-[11px]">
                    <input
                      type="radio"
                      name="checkpoint-location"
                      checked={draft.checkpointMode === 'custom'}
                      onChange={() => {
                        setDraft(current => ({
                          ...current,
                          checkpointMode: 'custom',
                          hasCheckpointLocation: true,
                        }));
                        markDirty('newRunRecoveryLocation', 'directory');
                      }}
                      className="accent-[var(--color-accent)]"
                    />
                    <span className="font-medium">Custom Directory</span>
                  </label>

                  {draft.checkpointMode === 'custom' && (
                    <div>
                      <input
                        type="text"
                        value={draft.customDirectory}
                        aria-label="Custom checkpoint directory"
                        aria-invalid={Boolean(directoryError)}
                        placeholder="C:\\WorkFlowRecovery\\sample-run"
                        onChange={event => {
                          setDraft(current => ({
                            ...current,
                            hasCheckpointLocation: true,
                            customDirectory: event.target.value,
                          }));
                          markDirty('directory', 'newRunRecoveryLocation');
                        }}
                        className="h-9 w-full rounded-[var(--radius-md)] border bg-[var(--color-bg-field)] px-2 font-mono text-[11px] outline-none focus:border-[var(--color-border-focus)]"
                        style={{
                          color: 'var(--color-text-primary)',
                          borderColor: directoryError
                            ? 'var(--color-danger)'
                            : 'var(--color-border-default)',
                        }}
                      />
                      <FieldError message={directoryError} />
                    </div>
                  )}
                  <FieldError message={locationError} />
                </div>
              </fieldset>
            </>
          )}

          <WorkerResourcesSection
            requiredProfiles={workerProfileNames}
            disabled={isExecuting || isSaving}
            onSaved={async () => refreshExecutionSettingsPreflight()}
          />

          <section
            className="rounded-[var(--radius-md)] border px-3 py-3"
            style={{
              borderColor: 'var(--color-border-subtle)',
              backgroundColor: 'var(--color-bg-field)',
            }}
          >
            <div className="flex items-center justify-between gap-3">
              <h3 className="text-[11px] font-semibold">Preflight Summary</h3>
              {isPreflighting && (
                <span className="text-[9px]" style={{ color: 'var(--color-info)' }}>
                  Checking…
                </span>
              )}
            </div>
            <dl className="mt-2 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-[10px]">
              <dt style={{ color: 'var(--color-text-muted)' }}>Output Shape</dt>
              <dd className="text-right font-mono">
                {currentPreflightFailed
                  ? 'Preflight failed'
                  : summaryOutputShape?.length ? summaryOutputShape.join(' × ') : 'Not checked'}
              </dd>
              <dt style={{ color: 'var(--color-text-muted)' }}>Total Windows</dt>
              <dd className="text-right font-mono">
                {currentPreflightFailed
                  ? 'Preflight failed'
                  : totalWindows === undefined ? 'Not checked' : totalWindows.toLocaleString()}
              </dd>
              <dt style={{ color: 'var(--color-text-muted)' }}>Required Profiles</dt>
              <dd className="text-right font-mono">{profileSummary}</dd>
              {availableResources && (
                <>
                  <dt style={{ color: 'var(--color-text-muted)' }}>Current Workers</dt>
                  <dd className="text-right font-mono">
                    CPU {availableResources.cpuWorkers ?? '—'} · GPU {availableResources.gpuWorkers ?? '—'}
                  </dd>
                </>
              )}
              {allocationPlan && (
                <>
                  <dt style={{ color: 'var(--color-text-muted)' }}>Planned Workers</dt>
                  <dd className="text-right font-mono">{allocationPlan.totalWorkers}</dd>
                  <dt style={{ color: 'var(--color-text-muted)' }}>Slurm Jobs</dt>
                  <dd className="text-right font-mono">{allocationPlan.jobs.length}</dd>
                  <dt style={{ color: 'var(--color-text-muted)' }}>Slurm Partitions</dt>
                  <dd className="text-right font-mono">
                    {allocationPlan.partitions.join(', ')}
                  </dd>
                  <dt style={{ color: 'var(--color-text-muted)' }}>Target Nodes</dt>
                  <dd className="text-right font-mono">
                    {allocationPlan.nodes
                      .map(node => `${node.node} (${node.partition})`)
                      .join(', ')}
                  </dd>
                  <dt style={{ color: 'var(--color-text-muted)' }}>Slurm Resources</dt>
                  <dd className="text-right font-mono">
                    CPU {allocationPlan.totalCpu} · GPU {allocationPlan.totalGpu} · {allocationPlan.totalMemoryGiB} GiB
                  </dd>
                </>
              )}
            </dl>
            {draft.mode === 'window' && executionPreflight?.windowable === false && (
              <p className="mt-2 text-[10px]" style={{ color: 'var(--color-warning)' }}>
                {executionPreflight.reason || 'Window execution is unavailable for the current workflow.'}
              </p>
            )}
            {lastPreflight?.validatedAt && !executionPreflight && (
              <p className="mt-2 text-[9px]" style={{ color: 'var(--color-text-muted)' }}>
                Last checked {new Date(lastPreflight.validatedAt).toLocaleString()}
              </p>
            )}
          </section>
        </div>

        <div
          className="flex shrink-0 flex-wrap items-center justify-between gap-2 border-t px-5 py-3"
          style={{
            borderColor: 'var(--color-border-subtle)',
            backgroundColor: 'var(--color-bg-surface-2)',
          }}
        >
          <Button
            type="button"
            variant="ghost"
            size="md"
            disabled={isExecuting || isSaving}
            onClick={handleReset}
          >
            Reset
          </Button>
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="secondary"
              size="md"
              loading={isPreflighting}
              disabled={!isConnected || isExecuting || isSaving}
              onClick={() => { void handleRefreshPreflight(); }}
            >
              Refresh Preflight
            </Button>
            <Button
              type="submit"
              variant="primary"
              size="md"
              loading={isSaving}
              disabled={!canSave}
            >
              Save Settings
            </Button>
          </div>
        </div>
      </form>
    </aside>
  );
}
