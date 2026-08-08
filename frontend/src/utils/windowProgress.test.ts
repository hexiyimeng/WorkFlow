import {
  createWindowProgressProtocolState,
  normalizeWindowProgress,
  parseLegacyWindowProgress,
  resolveWindowProgressProtocolEvent,
} from './windowProgress.ts';

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

const first = normalizeWindowProgress({
  currentWindow: 1,
  completedWindows: 0,
  totalWindows: 125,
  progress: 0,
  windowStatus: 'running',
  message: 'Window 1 / 125',
});
assert(first?.currentWindow === 1, 'first Window should be accepted');
assert(first?.progress === 0, 'first Window should begin at zero completed progress');

const resumed = normalizeWindowProgress({
  currentWindow: 5,
  completedWindows: 4,
  totalWindows: 125,
  progress: 3.2,
  windowStatus: 'running',
  message: 'Window 5 / 125',
});
assert(resumed?.completedWindows === 4, 'resume progress should preserve completed Windows');

const finalizing = normalizeWindowProgress({
  currentWindow: 125,
  completedWindows: 125,
  totalWindows: 125,
  progress: 100,
  windowStatus: 'finalizing',
  message: 'Finalizing Window Execution',
});
assert(finalizing?.progress === 100, 'finalizing progress should be complete');

const legacy = parseLegacyWindowProgress('Window 5 / 125');
assert(legacy?.currentWindow === 5, 'legacy progress should recover the current Window');
assert(legacy?.completedWindows === 4, 'legacy progress should infer completed Windows');
assert(legacy?.progress === 3.2, 'legacy progress should retain fractional precision');

const noncontiguous = normalizeWindowProgress({
  currentWindow: 6,
  completedWindows: 4,
  totalWindows: 125,
  progress: 3.2,
  windowStatus: 'running',
});
assert(noncontiguous?.currentWindow === 6, 'noncontiguous recovery progress should be accepted');

const laterCompletions = normalizeWindowProgress({
  currentWindow: 2,
  completedWindows: 4,
  totalWindows: 8,
  progress: 50,
  windowStatus: 'running',
});
assert(laterCompletions?.completedWindows === 4, 'completed count may include later Window indices');
assert(
  normalizeWindowProgress({
    currentWindow: 1,
    completedWindows: 0,
    totalWindows: 125,
    progress: 101,
    windowStatus: 'running',
  }) === null,
  'out-of-range progress should be rejected',
);
assert(parseLegacyWindowProgress('Running') === null, 'unrelated node progress must be ignored');

let protocolState = createWindowProgressProtocolState('exec-structured');
let displayedProgress = null as ReturnType<typeof normalizeWindowProgress>;

let protocolResult = resolveWindowProgressProtocolEvent(
  protocolState,
  {
    source: 'structured',
    executionId: 'exec-structured',
    value: {
      currentWindow: 1,
      completedWindows: 0,
      totalWindows: 4,
      progress: 0,
      windowStatus: 'running',
      message: 'Window 1 / 4',
    },
  },
);
protocolState = protocolResult.state;
displayedProgress = protocolResult.progress;

protocolResult = resolveWindowProgressProtocolEvent(
  protocolState,
  {
    source: 'legacy',
    executionId: 'exec-structured',
    value: 'Window 1 / 4',
  },
);
assert(protocolResult.progress === null, 'legacy text must not override structured progress');

protocolResult = resolveWindowProgressProtocolEvent(
  protocolState,
  {
    source: 'structured',
    executionId: 'exec-structured',
    value: {
      currentWindow: 2,
      completedWindows: 0,
      totalWindows: 4,
      progress: 0,
      windowStatus: 'running',
      message: 'Window 2 / 4',
    },
  },
);
protocolState = protocolResult.state;
displayedProgress = protocolResult.progress;

protocolResult = resolveWindowProgressProtocolEvent(
  protocolState,
  {
    source: 'legacy',
    executionId: 'exec-structured',
    value: 'Window 2 / 4',
  },
);
assert(protocolResult.progress === null, 'submitting Window 2 must not imply Window 1 completed');
assert(displayedProgress?.completedWindows === 0, 'two submitted Windows must still display zero completed');
assert(displayedProgress?.progress === 0, 'two submitted Windows must keep the progress bar at zero');

protocolResult = resolveWindowProgressProtocolEvent(
  protocolState,
  {
    source: 'structured',
    executionId: 'exec-structured',
    value: {
      currentWindow: 1,
      completedWindows: 1,
      totalWindows: 4,
      progress: 25,
      windowStatus: 'running',
      message: 'Completed Window 1 / 4',
    },
  },
);
assert(protocolResult.progress?.progress === 25, 'only a structured completion may advance progress to 25%');

let legacyOnlyState = createWindowProgressProtocolState('exec-legacy');
const legacyOnlyResult = resolveWindowProgressProtocolEvent(
  legacyOnlyState,
  {
    source: 'legacy',
    executionId: 'exec-legacy',
    value: 'Window 2 / 4',
  },
);
legacyOnlyState = legacyOnlyResult.state;
assert(legacyOnlyState.hasStructuredProgress === false, 'legacy-only executions must remain legacy-compatible');
assert(legacyOnlyResult.progress?.progress === 25, 'legacy-only backends should retain inferred progress');

let resumeState = createWindowProgressProtocolState('exec-resume');
const resumeStructured = resolveWindowProgressProtocolEvent(
  resumeState,
  {
    source: 'structured',
    executionId: 'exec-resume',
    value: {
      currentWindow: 2,
      completedWindows: 2,
      totalWindows: 4,
      progress: 50,
      windowStatus: 'running',
    },
  },
);
resumeState = resumeStructured.state;
const resumeLegacy = resolveWindowProgressProtocolEvent(
  resumeState,
  {
    source: 'legacy',
    executionId: 'exec-resume',
    value: 'Window 2 / 4',
  },
);
assert(resumeLegacy.progress === null, 'legacy inference must not reduce a noncontiguous completion count');
assert(resumeStructured.progress?.completedWindows === 2, 'structured resume count must remain authoritative');

const missingIdLegacy = resolveWindowProgressProtocolEvent(
  protocolState,
  {
    source: 'legacy',
    value: 'Window 2 / 4',
  },
  'exec-structured',
);
assert(missingIdLegacy.progress === null, 'active execution ID must suppress ID-less legacy messages');

const newExecutionLegacy = resolveWindowProgressProtocolEvent(
  protocolState,
  {
    source: 'legacy',
    executionId: 'exec-new',
    value: 'Window 2 / 4',
  },
);
assert(newExecutionLegacy.progress?.progress === 25, 'a new execution must not inherit structured protocol state');

let invalidStructuredState = createWindowProgressProtocolState('exec-invalid');
const invalidStructured = resolveWindowProgressProtocolEvent(
  invalidStructuredState,
  {
    source: 'structured',
    executionId: 'exec-invalid',
    value: { currentWindow: 2 },
  },
);
invalidStructuredState = invalidStructured.state;
assert(!invalidStructuredState.hasStructuredProgress, 'invalid structured data must not disable legacy fallback');
const fallbackAfterInvalid = resolveWindowProgressProtocolEvent(
  invalidStructuredState,
  {
    source: 'legacy',
    executionId: 'exec-invalid',
    value: 'Window 2 / 4',
  },
);
assert(fallbackAfterInvalid.progress?.progress === 25, 'legacy fallback must survive invalid structured data');
