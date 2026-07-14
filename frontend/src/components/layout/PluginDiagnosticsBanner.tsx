import { useMemo, useState } from 'react';
import { AlertTriangle, X } from 'lucide-react';
import { useFlow } from '../../hooks/useFlowContext';
import type { PluginImportFailure, PluginNodeInfoError } from '../../types';

type DisplayFailure =
  | (PluginImportFailure & { kind: 'Import failure'; title: string })
  | (PluginNodeInfoError & { kind: 'Object info error'; title: string });

const hasTraceback = (failure: DisplayFailure) =>
  typeof failure.traceback === 'string' && failure.traceback.trim().length > 0;

export default function PluginDiagnosticsBanner() {
  const { pluginDiagnostics, pluginStatusError } = useFlow();
  const [dismissedKey, setDismissedKey] = useState<string | null>(null);

  const failures = useMemo<DisplayFailure[]>(() => {
    if (!pluginDiagnostics) return [];
    const importFailures = pluginDiagnostics.failed_imports.map(failure => ({
      ...failure,
      kind: 'Import failure' as const,
      title: failure.module,
    }));
    const objectInfoFailures = (pluginDiagnostics.node_info_errors ?? []).map(failure => ({
      ...failure,
      kind: 'Object info error' as const,
      title: failure.node,
    }));
    return [...importFailures, ...objectInfoFailures];
  }, [pluginDiagnostics]);

  const issueCount = failures.length;
  const issueKey = pluginStatusError
    ? `status:${pluginStatusError}`
    : failures.map(failure => `${failure.kind}:${failure.title}:${failure.error_type}:${failure.message}`).join('|');
  const dismissed = dismissedKey === issueKey;

  if (!pluginStatusError && issueCount === 0) return null;

  if (dismissed) {
    return (
      <button
        type="button"
        onClick={() => setDismissedKey(null)}
        className="fixed right-3 top-14 z-50 inline-flex items-center gap-1.5 rounded-[var(--radius-md)] border px-2.5 py-1.5 text-[11px] font-semibold shadow-[var(--shadow-floating)]"
        style={{
          backgroundColor: 'var(--color-warning-soft)',
          borderColor: 'var(--color-warning)',
          color: 'var(--color-warning)',
        }}
      >
        <AlertTriangle className="h-3.5 w-3.5" />
        Plugin issues
      </button>
    );
  }

  if (pluginStatusError) {
    return (
      <section
        className="border-b px-4 py-2"
        style={{
          backgroundColor: 'var(--color-warning-soft)',
          borderColor: 'var(--color-warning)',
          color: 'var(--color-text-primary)',
        }}
      >
        <div className="flex items-start gap-3">
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" style={{ color: 'var(--color-warning)' }} />
          <div className="min-w-0 flex-1">
            <div className="text-[12px] font-semibold">Could not fetch plugin status from backend.</div>
            <div className="mt-0.5 break-all text-[11px]" style={{ color: 'var(--color-text-secondary)' }}>
              {pluginStatusError}
            </div>
          </div>
          <button
            type="button"
            onClick={() => setDismissedKey(issueKey)}
            className="rounded-[var(--radius-sm)] p-1 transition-colors hover:bg-black/10"
            title="Dismiss"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      </section>
    );
  }

  const importCount = pluginDiagnostics?.failed_count ?? 0;
  const objectInfoCount = pluginDiagnostics?.node_info_error_count ?? pluginDiagnostics?.node_info_errors?.length ?? 0;

  return (
    <section
      className="max-h-[38vh] overflow-y-auto border-b px-4 py-3"
      style={{
        backgroundColor: 'var(--color-warning-soft)',
        borderColor: 'var(--color-warning)',
        color: 'var(--color-text-primary)',
      }}
    >
      <div className="flex items-start gap-3">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" style={{ color: 'var(--color-warning)' }} />
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
            <span className="text-[12px] font-semibold">Some nodes failed to load.</span>
            <span className="text-[11px]" style={{ color: 'var(--color-text-secondary)' }}>
              {importCount} plugin import failure(s), {objectInfoCount} object_info error(s). These nodes may be missing from the node list.
            </span>
          </div>

          <div className="mt-2 space-y-2">
            {failures.map((failure, index) => (
              <div
                key={`${failure.kind}-${failure.title}-${index}`}
                className="rounded-[var(--radius-sm)] border p-2"
                style={{
                  backgroundColor: 'var(--color-bg-surface)',
                  borderColor: 'var(--color-border-subtle)',
                }}
              >
                <div className="grid gap-1 text-[11px] md:grid-cols-[120px_minmax(0,1fr)]">
                  <span className="font-semibold" style={{ color: 'var(--color-text-secondary)' }}>Stage</span>
                  <span>{failure.kind}</span>

                  <span className="font-semibold" style={{ color: 'var(--color-text-secondary)' }}>
                    {failure.kind === 'Import failure' ? 'Module' : 'Node'}
                  </span>
                  <span className="break-all font-mono">{failure.title}</span>

                  <span className="font-semibold" style={{ color: 'var(--color-text-secondary)' }}>File</span>
                  <span className="break-all font-mono">{failure.file || 'Unknown'}</span>

                  <span className="font-semibold" style={{ color: 'var(--color-text-secondary)' }}>Error</span>
                  <span className="break-all font-mono">
                    {failure.error_type}: {failure.message}
                  </span>
                </div>

                {hasTraceback(failure) && (
                  <details className="mt-2">
                    <summary className="cursor-pointer text-[11px] font-semibold" style={{ color: 'var(--color-warning)' }}>
                      Traceback
                    </summary>
                    <pre
                      className="mt-1 max-h-52 overflow-auto whitespace-pre-wrap break-words rounded-[var(--radius-sm)] p-2 text-[10px]"
                      style={{
                        backgroundColor: 'var(--color-console-bg)',
                        color: 'var(--color-console-text)',
                      }}
                    >
                      {failure.traceback}
                    </pre>
                  </details>
                )}
              </div>
            ))}
          </div>
        </div>
        <button
          type="button"
          onClick={() => setDismissedKey(issueKey)}
          className="rounded-[var(--radius-sm)] p-1 transition-colors hover:bg-black/10"
          title="Dismiss"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>
    </section>
  );
}
