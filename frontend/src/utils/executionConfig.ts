import type {
  ExecutionOutput,
  ExecutionPreflightResponse,
  RecoveryLocation,
} from '../types';

export const isValidOutputShape = (shape: unknown): shape is number[] => (
  Array.isArray(shape)
  && shape.every(size => Number.isSafeInteger(size) && size >= 0)
);

export const isValidWindowShape = (
  outputShape: number[],
  windowShape: number[],
): boolean => (
  outputShape.length > 0
  && windowShape.length === outputShape.length
  && windowShape.every(size => Number.isSafeInteger(size) && size > 0)
);

export const isValidMaxInFlightWindows = (value: unknown): value is number => (
  Number.isSafeInteger(value) && Number(value) > 0
);

export const preflightResourcesAllowExecution = (
  preflight: Pick<ExecutionPreflightResponse, 'resourcesSatisfied'>,
): boolean => preflight.resourcesSatisfied !== false;

export const estimateWindowCount = (
  outputShape: number[],
  windowShape: number[],
): bigint | null => {
  if (!isValidOutputShape(outputShape) || !isValidWindowShape(outputShape, windowShape)) {
    return null;
  }

  return outputShape.reduce((total, outputSize, index) => {
    const output = BigInt(outputSize);
    const window = BigInt(windowShape[index]);
    const windowsOnAxis = (output + window - 1n) / window;
    return total * windowsOnAxis;
  }, 1n);
};

export const calculateWindowGridShape = (
  outputShape: number[],
  windowShape: number[],
): number[] | null => {
  if (!isValidOutputShape(outputShape) || !isValidWindowShape(outputShape, windowShape)) {
    return null;
  }
  return outputShape.map((size, index) => Math.ceil(size / windowShape[index]));
};

export const preflightOutputShape = (
  preflight: ExecutionPreflightResponse,
): number[] => {
  const shape = preflight.outputShape ?? preflight.output_shape;
  return isValidOutputShape(shape) ? shape : [];
};

export const isAbsoluteServerPath = (value: string): boolean => {
  const path = value.trim();
  return (
    path.startsWith('/')
    || /^[A-Za-z]:[\\/]/.test(path)
    || /^\\\\[^\\/]+[\\/][^\\/]+/.test(path)
  );
};

export const sameServerPath = (
  left: string | null | undefined,
  right: string | null | undefined,
): boolean => {
  if (!left || !right) return false;
  const normalize = (value: string): string => {
    const path = value.trim();
    const isWindowsPath = /^[A-Za-z]:[\\/]/.test(path) || /^\\\\/.test(path);
    if (isWindowsPath) {
      const windowsPath = path.replace(/\//g, '\\');
      return windowsPath.replace(/\\+$/, '').toLocaleLowerCase('en-US');
    }
    return path === '/' ? path : path.replace(/\/+$/, '');
  };
  return normalize(left) === normalize(right);
};

export const resolveRecoveryDirectory = (
  location: RecoveryLocation,
  outputs: ExecutionOutput[],
): string | null => {
  if (location.mode === 'custom') {
    const directory = location.directory.trim();
    return isAbsoluteServerPath(directory) ? directory : null;
  }

  const output = outputs.find(item => item.nodeId === location.anchorNodeId);
  return output ? `${output.path}.workflow` : null;
};
