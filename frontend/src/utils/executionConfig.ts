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
