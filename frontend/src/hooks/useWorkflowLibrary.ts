import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { LibraryItem, LibraryReference, NodeData, NodeSpec, Workflow } from '../types';
import type { Edge, Node } from '@xyflow/react';
import { serializeWorkflowDocument, parseWorkflowDocument, hydrateFlowWithLatestSpecs, type ParsedWorkflowDocument } from '../utils/workflowPersistence';
import { documentFingerprint, libraryReference, libraryRequest, type LibraryKind, type LibraryRecord } from '../utils/libraryApi';
import { createWorkflowId } from '../utils/workflowExecutionSettings';
import { cloneGraphIds, type Graph } from '../utils/subgraphs';
import { readLibraryReference, workflowDraftKey, workflowServer } from '../utils/workflowDrafts';

export interface LibrarySaveDialog { kind: LibraryKind; asCopy: boolean; name: string; folder: string; graph?: Graph }

export function useWorkflowLibrary(options: {
  nodes: Node<NodeData>[]; edges: Edge[]; activeWorkflow: Workflow | null; workflows: Workflow[];
  nodeDefs: Record<string, NodeSpec>; locked: boolean;
  openDocument: (document: ParsedWorkflowDocument, nodes: Node<NodeData>[], edges: Edge[], reference?: LibraryReference) => void;
  updateWorkflow: (id: string, patch: Partial<Workflow>) => void;
  switchWorkflow: (id: string) => void; editGraph: (change: (graph: Graph) => Graph) => void;
  addLog: (message: string, level?: 'info' | 'error' | 'success' | 'warning') => void;
}) {
  const { nodes, edges, activeWorkflow, workflows, nodeDefs, locked, openDocument, updateWorkflow, switchWorkflow, editGraph, addLog } = options;
  const [libraryItems, setItems] = useState<Record<LibraryKind, LibraryItem[]>>({ workflows: [], subgraphs: [] });
  const [libraryError, setError] = useState<string | null>(null);
  const [libraryBusy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [librarySaveDialog, setSaveDialog] = useState<LibrarySaveDialog | null>(null);
  const currentDocument = useMemo(() => activeWorkflow ? serializeWorkflowDocument({ workflowId: activeWorkflow.workflowId || activeWorkflow.id,
    workflowName: activeWorkflow.name, metadata: activeWorkflow.metadata, nodes, edges }) : null, [activeWorkflow, nodes, edges]);
  const fingerprint = useMemo(() => currentDocument ? documentFingerprint(currentDocument) : '', [currentDocument]);
  const workflowDirty = !activeWorkflow?.libraryRef || !currentDocument
    || activeWorkflow.libraryRef.fingerprint !== fingerprint;

  const refreshLibrary = useCallback(async () => {
    try {
      const result = await Promise.all((['workflows', 'subgraphs'] as const).map(async kind => {
        const response = await libraryRequest<{ items: LibraryItem[]; warnings: string[] }>(kind);
        return { kind, ...response };
      }));
      setItems(Object.fromEntries(result.map(r => [r.kind, r.items])) as Record<LibraryKind, LibraryItem[]>);
      setError(result.flatMap(r => r.warnings).join('\n') || null);
    } catch (error) { setError((error as Error).message); }
  }, []);
  useEffect(() => { void refreshLibrary(); }, [refreshLibrary]);

  const saveLibraryDocument = async (dialog: LibrarySaveDialog, name: string, folder: string) => {
    if (locked || busyRef.current || !activeWorkflow || !currentDocument) return false;
    busyRef.current = true; setBusy(true); setError(null);
    try {
      const isSubgraph = dialog.kind === 'subgraphs';
      const document = isSubgraph ? serializeWorkflowDocument({ workflowId: createWorkflowId(), workflowName: name,
        nodes: dialog.graph!.nodes, edges: dialog.graph!.edges }) : { ...currentDocument,
        workflow_name: name, workflowId: dialog.asCopy ? createWorkflowId() : currentDocument.workflowId };
      const reference = !isSubgraph && !dialog.asCopy ? activeWorkflow.libraryRef : undefined;
      const record = await libraryRequest<LibraryRecord>(`${dialog.kind}${reference ? `/${reference.id}` : ''}`, {
        method: reference ? 'PUT' : 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, folder, document, revision: reference?.revision }),
      });
      if (!isSubgraph) updateWorkflow(activeWorkflow.id, { name: record.name, workflowId: document.workflowId, libraryRef: libraryReference(record) });
      setSaveDialog(null); addLog(`Saved ${isSubgraph ? 'subgraph' : 'workflow'}: ${name}`, 'success');
      await refreshLibrary(); return true;
    } catch (error) { setError((error as Error).message); addLog(`Save failed: ${(error as Error).message}`, 'error'); return false; }
    finally { busyRef.current = false; setBusy(false); }
  };
  const requestLibrarySave = (asCopy = false) => {
    if (locked || librarySaveDialog || !activeWorkflow) return;
    const dialog: LibrarySaveDialog = { kind: 'workflows', asCopy, name: activeWorkflow.name + (asCopy ? ' copy' : ''), folder: activeWorkflow.libraryRef?.folder || '' };
    if (!asCopy && activeWorkflow.libraryRef) void saveLibraryDocument(dialog, dialog.name, dialog.folder);
    else { setError(null); setSaveDialog(dialog); }
  };
  const saveSubgraphToLibrary = (node: Node<NodeData>) => {
    if (locked || !node.data.subgraph) return;
    setError(null); setSaveDialog({ kind: 'subgraphs', asCopy: true, name: node.data.title || 'Subgraph', folder: '', graph: { nodes: [node], edges: [] } });
  };
  const openLibraryItem = async (kind: LibraryKind, id: string) => {
    if (locked || busyRef.current) return;
    if (kind === 'workflows') {
      const existing = workflows.find(w => w.libraryRef?.id === id);
      if (existing) { switchWorkflow(existing.id); return; }
    }
    busyRef.current = true; setBusy(true); setError(null);
    try {
      const record = await libraryRequest<LibraryRecord>(`${kind}/${id}`);
      let document = parseWorkflowDocument(record.document);
      if (!document) throw new Error('Invalid workflow document.');
      let reference = libraryReference(record);
      if (kind === 'workflows') {
        try {
          const raw = localStorage.getItem(workflowDraftKey(document.workflowId, workflowServer()));
          const draft = raw ? JSON.parse(raw) : null;
          const draftRef = readLibraryReference(draft?._libraryRef);
          if (draftRef?.id === id && Number(draft.timestamp) > record.updatedAt
            && documentFingerprint(draft) !== documentFingerprint(record.document)
            && confirm(`Restore unsaved browser changes for "${record.name}"? Cancel opens the server version.`)) {
            const parsed = parseWorkflowDocument(draft);
            if (parsed) { document = parsed; reference = draftRef; }
          }
        } catch { /* A corrupt browser draft cannot prevent opening a saved server file. */ }
      }
      const hydrated = hydrateFlowWithLatestSpecs(document, nodeDefs);
      if (hydrated.invalidNodeTypes.length) addLog(`Missing plugins: ${[...new Set(hydrated.invalidNodeTypes)].join(', ')}. The workflow is preserved; install them before running.`, 'warning');
      if (kind === 'workflows') openDocument({ ...document, workflowName: record.name }, hydrated.nodes, hydrated.edges, reference);
      else {
        if (hydrated.nodes.length !== 1 || !hydrated.nodes[0].data.subgraph) throw new Error('Invalid subgraph blueprint.');
        const cloned = cloneGraphIds(hydrated);
        editGraph(graph => ({ ...graph, nodes: [...graph.nodes.map(n => ({ ...n, selected: false })),
          ...cloned.nodes.map(n => ({ ...n, position: { x: 200, y: 150 }, selected: true }))] }));
      }
      addLog(`${kind === 'workflows' ? 'Opened' : 'Inserted'} ${record.name}`, 'info');
    } catch (error) { setError((error as Error).message); addLog((error as Error).message, 'error'); }
    finally { busyRef.current = false; setBusy(false); }
  };
  const deleteLibraryItem = async (kind: LibraryKind, item: LibraryItem) => {
    if (locked || busyRef.current) return;
    busyRef.current = true; setBusy(true);
    try {
      await libraryRequest(`${kind}/${item.id}?revision=${item.revision}`, { method: 'DELETE' });
      if (kind === 'workflows') workflows.filter(w => w.libraryRef?.id === item.id).forEach(w => updateWorkflow(w.id, { libraryRef: undefined }));
      await refreshLibrary();
    } catch (error) { setError((error as Error).message); }
    finally { busyRef.current = false; setBusy(false); }
  };
  const renameLibraryItem = async (kind: LibraryKind, item: LibraryItem, name: string) => {
    if (locked || busyRef.current) return;
    busyRef.current = true; setBusy(true);
    try {
      const record = await libraryRequest<LibraryRecord>(`${kind}/${item.id}`);
      const updated = await libraryRequest<LibraryRecord>(`${kind}/${item.id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, folder: item.folder, revision: item.revision, document: { ...record.document, workflow_name: name } }) });
      if (kind === 'workflows') workflows.filter(w => w.libraryRef?.id === item.id).forEach(w => updateWorkflow(w.id, {
        name, libraryRef: libraryReference(updated),
      }));
      await refreshLibrary();
    } catch (error) { setError((error as Error).message); }
    finally { busyRef.current = false; setBusy(false); }
  };
  return { libraryItems, libraryError, libraryBusy, librarySaveDialog, workflowDirty, refreshLibrary,
    requestLibrarySave, saveLibraryDocument, openLibraryItem, deleteLibraryItem, renameLibraryItem, saveSubgraphToLibrary,
    closeLibrarySave: () => { if (!busyRef.current) setSaveDialog(null); } };
}
