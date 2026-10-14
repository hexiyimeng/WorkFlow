import type { WSMessage } from '../types';

/** Keep the cause visible; put long exceptions and stack traces behind details. */
export function executionDiagnostic(msg: WSMessage): { message: string; details?: string } {
  const cause = msg.message?.trim() || 'Unknown error';
  const context = [
    msg.stage,
    msg.taskId ? `${msg.nodeType || 'Node'} [${msg.taskId}]` : msg.nodeType,
    msg.errorType,
  ].filter(Boolean).join(' · ');
  const firstLine = cause.split('\n')[0];
  const summary = firstLine.length > 500 ? `${firstLine.slice(0, 500)}…` : firstLine;
  const details = [
    msg.executionId ? `Execution: ${msg.executionId}` : '',
    msg.taskKey ? `Dask task: ${msg.taskKey}` : '',
    cause !== summary ? cause.slice(0, 16000) : '',
    msg.traceback?.slice(-16000),
  ].filter(Boolean).join('\n\n');
  return { message: context ? `${context}: ${summary}` : summary, details: details || undefined };
}
