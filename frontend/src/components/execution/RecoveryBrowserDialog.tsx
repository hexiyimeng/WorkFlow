import { useEffect, useState } from 'react';
import { useFlow } from '../../hooks/useFlowContext';
import type { RecoverySummary, ServerDirectoryListing } from '../../types';
import { Button } from '../ui/Button';

export default function RecoveryBrowserDialog() {
  const {
    isRecoveryBrowserOpen,
    closeRecoveryBrowser,
    recoveredGraphView,
    browseServerDirectories,
    inspectRecoveryDirectory,
    openRecoveryDirectory,
    deleteRecoveryDirectory,
    executeRecoveryDirectory,
    isConnected,
    isExecuting,
  } = useFlow();
  const [path, setPath] = useState('');
  const [listing, setListing] = useState<ServerDirectoryListing | null>(null);
  const [summary, setSummary] = useState<RecoverySummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<{
    message: string;
    severity: 'success' | 'warning';
  } | null>(null);
  const [busyAction, setBusyAction] = useState<string | null>(null);

  useEffect(() => {
    if (!isRecoveryBrowserOpen) {
      setListing(null);
      setSummary(null);
      setError(null);
      setNotice(null);
      setBusyAction(null);
      return;
    }

    let cancelled = false;
    const recoveredDirectory = recoveredGraphView?.recoveryDirectory ?? '';
    setPath(recoveredDirectory);
    // Re-inspect an already opened record instead of showing a potentially
    // stale progress snapshot from the previous drawer session.
    setSummary(null);
    setError(null);
    setNotice(null);
    setBusyAction(recoveredDirectory ? 'inspect' : 'browse');
    void (async () => {
      try {
        const result = await browseServerDirectories('');
        if (cancelled) return;
        setListing(result);
        if (!recoveredDirectory) setPath(result.path);
      } catch (reason) {
        if (!recoveredDirectory && !cancelled) {
          setError((reason as Error).message);
        }
      }

      if (recoveredDirectory && !cancelled) {
        try {
          const inspected = await inspectRecoveryDirectory(recoveredDirectory);
          if (cancelled) return;
          setPath(inspected.recoveryDirectory);
          setSummary(inspected);
        } catch (reason) {
          if (!cancelled) setError((reason as Error).message);
        }
      }

      if (!cancelled) setBusyAction(null);
    })();
    return () => {
      cancelled = true;
    };
  }, [
    browseServerDirectories,
    inspectRecoveryDirectory,
    isRecoveryBrowserOpen,
    recoveredGraphView,
  ]);

  useEffect(() => {
    if (!isRecoveryBrowserOpen) return undefined;
    const handleEscape = (event: globalThis.KeyboardEvent) => {
      if (event.key === 'Escape' && busyAction === null) {
        closeRecoveryBrowser();
      }
    };
    window.addEventListener('keydown', handleEscape);
    return () => window.removeEventListener('keydown', handleEscape);
  }, [busyAction, closeRecoveryBrowser, isRecoveryBrowserOpen]);

  if (!isRecoveryBrowserOpen) return null;

  const loadDirectory = async (nextPath: string) => {
    setBusyAction('browse');
    setError(null);
    setNotice(null);
    setSummary(null);
    try {
      const result = await browseServerDirectories(nextPath);
      setListing(result);
      setPath(result.path);
    } catch (reason) {
      setError((reason as Error).message);
    } finally {
      setBusyAction(null);
    }
  };

  const inspect = async (directory: string) => {
    setPath(directory);
    setBusyAction('inspect');
    setError(null);
    setNotice(null);
    setSummary(null);
    try {
      const inspected = await inspectRecoveryDirectory(directory);
      setPath(inspected.recoveryDirectory);
      setSummary(inspected);
    } catch (reason) {
      setError((reason as Error).message);
    } finally {
      setBusyAction(null);
    }
  };

  const openSavedWorkflow = async () => {
    setBusyAction('open');
    setError(null);
    setNotice(null);
    try {
      await openRecoveryDirectory(path);
    } catch (reason) {
      setError((reason as Error).message);
    } finally {
      setBusyAction(null);
    }
  };

  const execute = async (action: 'resume' | 'restart') => {
    if (
      action === 'restart'
      && !window.confirm(
        'Restart will reset the saved Window completion bitmap and rerun every Window. Continue?',
      )
    ) {
      return;
    }
    setBusyAction(action);
    setError(null);
    setNotice(null);
    try {
      const submitted = await executeRecoveryDirectory(path, action);
      if (!submitted) {
        setError('Recovery was not submitted because the backend is unavailable or another execution is unresolved.');
      }
    } catch (reason) {
      setError((reason as Error).message);
    } finally {
      setBusyAction(null);
    }
  };

  const deleteRecord = async () => {
    if (!summary || summary.recoveryDirectory !== path) return;
    if (!window.confirm(
      `Delete the recovery record at:\n${path}\n\n`
      + 'Resume and Restart for this record will no longer be available. '
      + 'Its saved graph, execution snapshot, and Window bitmap will be removed now; '
      + 'Zarr/Parquet outputs are not deleted by this action. A later new Run may still '
      + 'overwrite those outputs when a Writer has overwrite enabled. Continue?',
    )) {
      return;
    }

    setBusyAction('delete');
    setError(null);
    setNotice(null);
    try {
      const deleted = await deleteRecoveryDirectory(
        path,
        summary.executionId,
      );
      setSummary(null);
      setNotice({
        message: deleted.cleanupPending
          ? `Recovery record deleted and ${deleted.outputsPreserved.length} output location(s) were not deleted. `
            + `Detached metadata still needs filesystem cleanup at ${deleted.cleanupDirectory}.`
          : `Recovery record deleted. ${deleted.outputsPreserved.length} output location(s) `
            + 'were not deleted. If Execution Settings still points here, Normal Run can create '
            + 'a new record using the current workflow.',
        severity: deleted.cleanupPending ? 'warning' : 'success',
      });
      if (listing) {
        try {
          setListing(await browseServerDirectories(listing.path));
        } catch {
          // The deletion already succeeded; listing refresh is best effort.
        }
      }
    } catch (reason) {
      setError((reason as Error).message);
    } finally {
      setBusyAction(null);
    }
  };

  const selectedIsInspected = summary?.recoveryDirectory === path;

  return (
    <aside
      role="dialog"
      aria-modal="false"
      aria-labelledby="recovery-browser-title"
      className="fixed bottom-0 right-0 top-12 z-[10020] flex w-full max-w-xl flex-col overflow-hidden border-l"
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
          <h2 id="recovery-browser-title" className="text-[15px] font-semibold">
            Recovery
          </h2>
          <p className="mt-1 text-[10px]" style={{ color: 'var(--color-text-muted)' }}>
            Inspect, delete, open, resume, or restart a saved Window recovery record.
          </p>
        </div>
        <Button
          type="button"
          size="xs"
          variant="ghost"
          aria-label="Close Recovery"
          onClick={closeRecoveryBrowser}
          disabled={busyAction !== null}
        >
          Close
        </Button>
      </div>

      <div className="min-h-0 flex-1 space-y-4 overflow-y-auto px-5 py-4">
          <div>
            <label className="mb-1 block text-[10px] font-medium" htmlFor="recovery-server-path">
              Recovery directory
            </label>
            <div className="flex flex-col gap-2 sm:flex-row">
              <input
                id="recovery-server-path"
                value={path}
                disabled={busyAction !== null || !isConnected}
                onChange={event => {
                  setPath(event.target.value);
                  setSummary(null);
                  setError(null);
                  setNotice(null);
                }}
                onKeyDown={event => {
                  if (event.key === 'Enter') {
                    event.preventDefault();
                    void loadDirectory(path);
                  } else if (event.key === 'Escape') {
                    closeRecoveryBrowser();
                  }
                }}
                placeholder="/shared/project/run.workflow"
                className="h-9 min-w-0 flex-1 rounded-[var(--radius-md)] border bg-[var(--color-bg-field)] px-2 font-mono text-[11px] outline-none focus:border-[var(--color-border-focus)]"
                style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-default)' }}
                autoFocus
              />
              <Button
                type="button"
                size="md"
                loading={busyAction === 'browse'}
                disabled={busyAction !== null || !isConnected}
                onClick={() => { void loadDirectory(path); }}
              >
                Browse
              </Button>
              <Button
                type="button"
                size="md"
                loading={busyAction === 'inspect'}
                disabled={!path.trim() || busyAction !== null || !isConnected}
                onClick={() => { void inspect(path); }}
              >
                Inspect
              </Button>
            </div>
          </div>

          <div
            className="overflow-hidden rounded-[var(--radius-md)] border"
            style={{ borderColor: 'var(--color-border-subtle)' }}
          >
            <div
              className="flex items-center justify-between border-b px-3 py-2 text-[10px]"
              style={{ borderColor: 'var(--color-border-subtle)', backgroundColor: 'var(--color-bg-surface-2)' }}
            >
              <span className="truncate font-mono">
                {listing ? (listing.path || 'Server directories') : 'No directory loaded'}
              </span>
              {listing?.parent && (
                <button
                  type="button"
                  className="ml-3 shrink-0 text-[var(--color-accent)] hover:underline"
                  onClick={() => { void loadDirectory(listing.parent ?? ''); }}
                >
                  Parent
                </button>
              )}
            </div>
            <div className="max-h-52 overflow-y-auto">
              {listing?.directories.map(directory => (
                <div
                  key={directory.path}
                  className="flex items-center gap-3 border-b px-3 py-2 last:border-b-0"
                  style={{ borderColor: 'var(--color-border-subtle)' }}
                >
                  <button
                    type="button"
                    className="min-w-0 flex-1 text-left"
                    onDoubleClick={() => { void loadDirectory(directory.path); }}
                    onClick={() => {
                      setPath(directory.path);
                      setSummary(null);
                      setError(null);
                      setNotice(null);
                    }}
                  >
                    <span className="block truncate text-[11px] font-medium">{directory.name}</span>
                    <span className="block truncate font-mono text-[9px]" style={{ color: 'var(--color-text-muted)' }}>
                      {directory.path}
                    </span>
                  </button>
                  {directory.isRecoveryDirectory ? (
                    <Button type="button" size="xs" onClick={() => { void inspect(directory.path); }}>
                      Recovery
                    </Button>
                  ) : (
                    <Button type="button" size="xs" onClick={() => { void loadDirectory(directory.path); }}>
                      Open
                    </Button>
                  )}
                </div>
              ))}
              {listing && listing.directories.length === 0 && (
                <div className="px-3 py-6 text-center text-[10px]" style={{ color: 'var(--color-text-muted)' }}>
                  No accessible subdirectories.
                </div>
              )}
            </div>
          </div>

          {summary && selectedIsInspected && (
            <div
              className="rounded-[var(--radius-md)] border px-3 py-3"
              style={{ borderColor: 'var(--color-border-subtle)', backgroundColor: 'var(--color-bg-field)' }}
            >
              <div className="flex items-center justify-between gap-3">
                <span className="text-[11px] font-semibold">Valid recovery · {summary.status}</span>
                <span className="font-mono text-[11px]" style={{ color: 'var(--color-info)' }}>
                  {summary.completedWindows.toLocaleString()} / {summary.totalWindows.toLocaleString()}
                </span>
              </div>
              <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-[var(--color-bg-surface-2)]">
                <div
                  className="h-full bg-[var(--color-accent)]"
                  style={{
                    width: `${summary.totalWindows === 0
                      ? 100
                      : summary.completedWindows * 100 / summary.totalWindows}%`,
                  }}
                />
              </div>
              <div className="mt-2 font-mono text-[9px]" style={{ color: 'var(--color-text-muted)' }}>
                Window ({summary.windowShape.join(' × ')}) · grid ({summary.windowGridShape.join(' × ')})
              </div>
              <div className="mt-2 space-y-1">
                {summary.outputs.map(output => (
                  <div key={output.nodeId} className="text-[10px]">
                    <span className="font-medium">{output.displayName}</span>
                    <span className="ml-2 break-all font-mono" style={{ color: 'var(--color-text-muted)' }}>
                      {output.path}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {error && (
            <div
              role="alert"
              className="rounded-[var(--radius-md)] px-3 py-2 text-[10px]"
              style={{ color: 'var(--color-danger)', backgroundColor: 'var(--color-danger-soft)' }}
            >
              {error}
            </div>
          )}
          {notice && (
            <div
              role="status"
              className="rounded-[var(--radius-md)] px-3 py-2 text-[10px]"
              style={{
                color: notice.severity === 'warning'
                  ? 'var(--color-warning)'
                  : 'var(--color-success)',
                backgroundColor: notice.severity === 'warning'
                  ? 'var(--color-warning-soft)'
                  : 'var(--color-success-soft)',
              }}
            >
              {notice.message}
            </div>
          )}
        </div>

      <div
        className="shrink-0 space-y-2 border-t px-5 py-3"
        style={{ borderColor: 'var(--color-border-subtle)', backgroundColor: 'var(--color-bg-surface-2)' }}
      >
        <p className="text-[9px]" style={{ color: 'var(--color-text-muted)' }}>
          Resume and Restart use the immutable saved graph. Delete removes the recovery record, not its outputs.
        </p>
        <div className="flex flex-wrap items-center justify-end gap-2">
          <Button
            type="button"
            size="md"
            variant="danger"
            disabled={!selectedIsInspected || busyAction !== null || !isConnected || isExecuting}
            loading={busyAction === 'delete'}
            onClick={() => { void deleteRecord(); }}
          >
            Delete Recovery Record
          </Button>
          <Button
            type="button"
            size="md"
            disabled={!selectedIsInspected || busyAction !== null || !isConnected || isExecuting}
            loading={busyAction === 'open'}
            onClick={() => { void openSavedWorkflow(); }}
          >
            Open Saved Workflow
          </Button>
          <Button
            type="button"
            size="md"
            variant="warning"
            disabled={!selectedIsInspected || busyAction !== null || !isConnected || isExecuting}
            loading={busyAction === 'restart'}
            onClick={() => { void execute('restart'); }}
          >
            Restart
          </Button>
          <Button
            type="button"
            size="md"
            variant="primary"
            disabled={!selectedIsInspected || busyAction !== null || !isConnected || isExecuting}
            loading={busyAction === 'resume'}
            onClick={() => { void execute('resume'); }}
          >
            Resume
          </Button>
        </div>
      </div>
    </aside>
  );
}
