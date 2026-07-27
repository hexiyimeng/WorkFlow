import type { WindowExecutionProgress, WindowProgressStatus } from '../types';

const isSafeInteger = (value: unknown): value is number => (
  typeof value === 'number' && Number.isSafeInteger(value)
);

export const normalizeWindowProgress = (value: unknown): WindowExecutionProgress | null => {
  if (!value || typeof value !== 'object') return null;
  const payload = value as Record<string, unknown>;
  const currentWindow = payload.currentWindow;
  const completedWindows = payload.completedWindows;
  const totalWindows = payload.totalWindows;
  const windowStatus = payload.windowStatus;

  if (
    !isSafeInteger(currentWindow)
    || !isSafeInteger(completedWindows)
    || !isSafeInteger(totalWindows)
    || totalWindows < 0
    || completedWindows < 0
    || completedWindows > totalWindows
    || (windowStatus !== 'running' && windowStatus !== 'finalizing')
  ) {
    return null;
  }

  if (totalWindows === 0) {
    if (currentWindow !== 0 || completedWindows !== 0 || windowStatus !== 'finalizing') {
      return null;
    }
  } else if (
    currentWindow < 1
    || currentWindow > totalWindows
    || completedWindows > currentWindow
    || currentWindow > completedWindows + 1
    || (windowStatus === 'finalizing' && (
      currentWindow !== totalWindows || completedWindows !== totalWindows
    ))
  ) {
    return null;
  }

  const calculatedProgress = totalWindows === 0
    ? 100
    : completedWindows * 100 / totalWindows;
  const suppliedProgress = payload.progress;
  if (
    suppliedProgress !== undefined
    && (
      typeof suppliedProgress !== 'number'
      || !Number.isFinite(suppliedProgress)
      || suppliedProgress < 0
      || suppliedProgress > 100
    )
  ) {
    return null;
  }

  const status = windowStatus as WindowProgressStatus;
  const defaultMessage = status === 'finalizing'
    ? 'Finalizing Window Execution'
    : `Window ${currentWindow} / ${totalWindows}`;

  return {
    currentWindow,
    completedWindows,
    totalWindows,
    progress: suppliedProgress ?? calculatedProgress,
    windowStatus: status,
    message: typeof payload.message === 'string' && payload.message.trim()
      ? payload.message
      : defaultMessage,
  };
};

export const parseLegacyWindowProgress = (
  message: unknown,
): WindowExecutionProgress | null => {
  if (typeof message !== 'string') return null;
  const match = /^Window\s+(\d+)\s*\/\s*(\d+)\s*$/i.exec(message.trim());
  if (!match) return null;

  const currentWindow = Number(match[1]);
  const totalWindows = Number(match[2]);
  if (!Number.isSafeInteger(currentWindow) || !Number.isSafeInteger(totalWindows)) {
    return null;
  }
  const completedWindows = currentWindow - 1;
  return normalizeWindowProgress({
    currentWindow,
    completedWindows,
    totalWindows,
    progress: totalWindows > 0 ? completedWindows * 100 / totalWindows : 100,
    windowStatus: 'running',
    message: `Window ${currentWindow} / ${totalWindows}`,
  });
};
