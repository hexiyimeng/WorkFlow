import type { ExecutionConfig, RecoverySummary, SubmittedExecutionSettings } from '../../types';

export default function ExecutionSettingsInspector({ snapshot, recoveryConfig, recoverySummary, executionId, onClose }: {
  snapshot: SubmittedExecutionSettings | null;
  recoveryConfig?: ExecutionConfig;
  recoverySummary?: RecoverySummary;
  executionId?: string | null;
  onClose: () => void;
}) {
  const config = snapshot?.config ?? recoveryConfig;
  const preflight = snapshot?.preflight;
  const shape = config?.mode === 'window' ? config.windowShape ?? recoverySummary?.windowShape : undefined;
  const outputShape = preflight?.outputShape ?? preflight?.output_shape ?? recoverySummary?.outputShape;
  const totalWindows = preflight?.totalWindows ?? recoverySummary?.totalWindows;
  return <aside role="dialog" aria-modal="false" aria-labelledby="execution-settings-title"
    className="fixed bottom-0 right-0 top-[var(--workspace-header-height)] z-[90] flex w-full max-w-[440px] flex-col border-l"
    style={{ backgroundColor: 'var(--color-bg-surface)', borderColor: 'var(--color-border-default)', boxShadow: 'var(--shadow-floating)' }}>
    <header className="flex items-start justify-between gap-4 border-b px-5 py-4" style={{ borderColor: 'var(--color-border-subtle)' }}>
      <div><h2 id="execution-settings-title" className="text-[15px] font-semibold">Execution Settings</h2>
        <p className="mt-1 text-xs" style={{ color: 'var(--color-text-muted)' }}>{snapshot ? 'Submitted settings · Read only' : recoveryConfig ? 'Recovery settings · Read only' : 'Read only'}</p></div>
      <button type="button" aria-label="Close execution settings" onClick={onClose} className="h-7 w-7 rounded text-lg hover:bg-[var(--color-bg-field-hover)]">×</button>
    </header>
    <div className="min-h-0 flex-1 space-y-5 overflow-y-auto px-5 py-5 text-[13px]">
      <p style={{ color: 'var(--color-text-secondary)' }}>{recoveryConfig && !snapshot ? 'Saved recovery settings are read only.' : 'Settings are locked while execution is active or its status is unresolved.'}</p>
      {executionId && <div><h3 className="mb-1 text-xs" style={{ color: 'var(--color-text-muted)' }}>Execution ID</h3><code className="break-all text-xs select-text">{executionId}</code></div>}
      {!config ? <p role="status">The settings submitted for this execution are not available in this browser. Saved workflow settings are not a substitute for the running configuration.</p> : <>
        <dl className="execution-settings-snapshot">
          <dt>Execution Mode</dt><dd>{config.mode === 'window' ? 'Window' : 'Full Graph'}</dd>
          {config.mode === 'window' && <>
            <dt>Window Shape</dt><dd>{shape?.join(' × ') ?? 'From saved recovery record'}</dd>
            <dt>Max in-flight Windows</dt><dd>{config.maxInFlightWindows ?? 'Backend default'}</dd>
            <dt>Run action</dt><dd>{config.resumeAction}</dd>
            <dt>Recovery location</dt><dd>{config.recoveryLocation.mode === 'custom' ? config.recoveryLocation.directory : `Beside output: ${config.recoveryLocation.anchorNodeId}`}</dd>
            {Number.isSafeInteger(totalWindows) && <><dt>Total Windows</dt><dd>{totalWindows}</dd></>}
          </>}
          {Array.isArray(outputShape) && <><dt>Output Shape</dt><dd>{outputShape.join(' × ')}</dd></>}
        </dl>
        <section><h3 className="font-semibold">CPU and GPU Workers</h3>
          <p className="mt-1 text-xs" style={{ color: 'var(--color-text-muted)' }}>Resource settings sent with this run. Live Worker counts may change.</p>
          {!snapshot ? <p className="mt-3 text-xs">Worker settings are available after submitting an execution in this browser.</p> : snapshot.workerProfiles.length === 0 ? <p className="mt-3 text-xs">No explicit Worker profiles were submitted; backend defaults apply.</p> : snapshot.workerProfiles.map(profile => {
            const pool = snapshot.workerPools.find(item => item.profile === profile.name);
            return <section key={profile.name} className="mt-3 rounded border p-3" style={{ borderColor: 'var(--color-border-subtle)' }}>
              <h4 className="mb-2 font-semibold">{profile.name}</h4>
              <dl className="execution-settings-snapshot"><dt>CPU / Worker</dt><dd>{profile.physical_resources.cpu}</dd>
                <dt>Memory / Worker</dt><dd>{profile.physical_resources.memory}</dd>
                <dt>GPU / Worker</dt><dd>{profile.physical_resources.gpu}</dd>
                <dt>Threads / Worker</dt><dd>{profile.threads}</dd>
                {pool && <><dt>Processes / Job</dt><dd>{pool.processes}</dd><dt>Minimum Jobs</dt><dd>{pool.minimum_jobs}</dd><dt>Maximum Jobs</dt><dd>{pool.maximum_jobs}</dd></>}
              </dl>
            </section>;
          })}
        </section>
      </>}
    </div>
    <footer className="border-t px-5 py-3" style={{ borderColor: 'var(--color-border-subtle)' }}><button type="button" onClick={onClose} className="workspace-text-button">Close</button></footer>
  </aside>;
}
