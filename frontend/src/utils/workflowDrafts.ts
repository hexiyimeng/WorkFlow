import type { LibraryReference } from '../types';

// Scope drafts to this backend, so switching servers cannot restore a different
// server's unsaved document. Legacy active autosave remains readable.
export function workflowDraftKey(workflowId: string, server: string): string {
  return `WorkFlow_DRAFT:${encodeURIComponent(server)}:${workflowId}`;
}

export function workflowServer(): string {
  return import.meta.env?.VITE_API_BASE_URL || (import.meta.env?.DEV ? 'http://localhost:8000' : typeof window === 'undefined' ? 'local' : window.location.origin);
}

export function readLibraryReference(raw: unknown): LibraryReference | undefined {
  if (!raw || typeof raw !== 'object') return undefined;
  const ref = raw as Partial<LibraryReference>;
  return typeof ref.id === 'string' && /^[0-9a-f-]{36}$/.test(ref.id) && Number.isSafeInteger(ref.revision) && Number(ref.revision) > 0
    && typeof ref.folder === 'string' && typeof ref.fingerprint === 'string'
    && (ref.kind === undefined || ref.kind === 'workflows' || ref.kind === 'subgraphs') ? ref as LibraryReference : undefined;
}
