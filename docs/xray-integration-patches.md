# Local patches for Xray native TUN integration

Audit date: 2026-11-02. Upstream reference: SagerNet/sing-tun
`611d83b7a57b4719be45581eb674942ff561de6c`, selected by sing-box
`v1.15.0-alpha.13`. The repository retains the upstream GPL-3.0-or-later license.

## Upstream-first maintenance policy

These fixes are temporary differences, not a design to preserve over official fixes.
Before merging/rebasing a newer SagerNet revision:

1. Inspect the affected upstream functions and any linked issue/PR. Compare ownership
   and failure behavior, not just line similarity or a commit subject.
2. Run the listed regressions against clean upstream **without our implementation
   patches**, carrying the tests separately where upstream lacks equivalent tests.
   A skipped privileged test does not establish that upstream fixed the problem.
3. When official code fixes the whole issue and the regression passes, prefer the
   official implementation and remove the corresponding local hunk/helper. Keep a
   regression if upstream does not already cover it. If only part is fixed, retain
   only the still-required difference; do not preserve an entire old patch with ours.
4. Mark the entry retired or partially resolved. Record the upstream commit/PR,
   new dependency baseline, removal commit, test platform and result. Do not erase
   the original entry. Never resolve a conflict mechanically in favor of our version.
5. Re-run Xray native integration tests and its default build as well as this
   library's platform tests. Update the pinned dependency and checksums together.

No upstream report/PR has been submitted by this work. A blank upstream-fix field
means not yet identified, not that upstream has declined or accepted a fix.

## Active fix ledger

### ST-FD-001 — Linux failed construction must close the os.File owner

- Status: active. Upstream fix: not yet identified.
- Location: `tun_linux.go`, `New`, configure-error cleanup.
- Original behavior: `os.NewFile` already owns tunFd, but configure failure closes
  only the raw number with unix.Close. Its surviving finalizer can later close an
  unrelated descriptor that reuses the same number.
- Local difference: close nativeTun.tunFile, invalidating its registered owner.
- Regression: `TestNativeTunOwnedConfigureFailureDoesNotCloseReusedDescriptor` in
  `tun_lifecycle_linux_test.go`; requires usable /dev/net/tun and CAP_NET_ADMIN.
- Retirement: official cleanup invalidates every os.File owner before descriptor
  reuse, with the regression passing without this local implementation.

### ST-FD-002 — deterministic Windows adapter cleanup on StartSession failure

- Status: active. Upstream fix: not yet identified.
- Location: `tun_windows.go`, `New`, adapter.StartSession error.
- Original behavior: the explicit adapter handle is left for its Go finalizer;
  cleanup timing is nondeterministic. This is not a demonstrated permanent leak.
- Local difference: close the acquired adapter before returning the session error.
- Validation: Windows test-binary compilation. Actual Wintun failure-path runtime
  verification is outstanding; compilation alone does not validate cleanup.
- Retirement: official constructor explicitly closes the acquired handle on every
  failed StartSession path, preserving the original error; verify on Windows.

### ST-FD-003 — Darwin allocation failures return errors and unwind ownership

- Status: active. Upstream fix: not yet identified.
- Location: `tun_darwin.go`, `New`, stop-pipe and raw read-poller setup.
- Original behavior: common.Must1 panics on allocation failure. Poller failure can
  leave the already-acquired stop pipe open; the caller cannot recover its handle.
- Local difference: stage fallible setup before os.NewFile ownership; close the
  stop pipe and internally created TUN socket on failure. A supplied descriptor
  remains caller-owned on failure and transfers ownership only on success.
- Regressions: `TestNativeTunConstructionFailureRetainsDescriptor` (stop pipe and
  read poller subprocess cases) and `TestNativeTunExternalDescriptorOwnership` in
  `tun_lifecycle_darwin_test.go`. Descriptor exhaustion is isolated to child processes.
- Evidence: both exhaustion cases reproduce the original Must1 panics against an
  overlay of the audited upstream constructor; the patched macOS cases pass.
- Retirement: official constructor has equivalent error/ownership behavior and
  these regressions pass against its unpatched implementation.

### ST-FD-004 — close-on-exec for owned TUN queues, sockets and stop pipes

- Status: active. Upstream fix: not yet identified.
- Locations: Linux openTun (primary and additional queues); Darwin owned TUN
  socket, useSocket and internal/stopfd_darwin.New.
- Original behavior: these raw descriptors can survive exec; fixing only the
  initial Linux descriptor at stack startup leaves extra queues and the pre-start
  window uncovered.
- Local difference: O_CLOEXEC for Linux open; Darwin creation and FD_CLOEXEC under
  syscall.ForkLock. Darwin kqueue already supplies CLOEXEC and is not modified.
- Regressions: `TestNativeTunOwnedQueuesCloseOnExec`,
  `TestNativeTunExternalDescriptorOwnership`,
  `TestUseSocketCloseOnExecAndFailureCleanup`.
- Retirement: upstream covers every listed owned descriptor, and corresponding
  tests pass without our hunks. Record partial resolution by descriptor kind.

## Separate local API extension

### ST-API-001 — download-only native socket splice

This is a local feature, distinct from the constructor fixes. It permits Xray to
hand over only a protocol-ready raw download while retaining the original socket
for its independently processed upload. Full bidirectional Splice remains separate.
The API must preserve EOF/FIN ordering, cancellation, counters and original socket
ownership. It is not an assertion that one-way or two-way transfer is universally
faster. The Xray integration must measure eligible paths separately.

When upstream offers an equivalent feature, prefer it after the Xray compatibility
and lifetime regressions pass. Similar names or a bidirectional Splice API alone
are not sufficient equivalence. Record the replacement revision and remove the
local API only with the coordinated Xray caller update.

### ST-API-002 — independently ready upload and download

- Status: active local extension, not an upstream bug-fix claim.
- Locations: `stack_go_splice_directional.go`, `stack_go_splice.go`, socket helpers
  and the TCP close/receive/writer ownership guards.
- Purpose: let Xray hand over its raw upload only after the final Vision padding
  write, without waiting for download readiness. Either direction may attach first,
  finish first or attach after the other direction reaches EOF.
- Contract: the same comparable connected nonblocking stream socket identifies
  both directions. Only duplicated close-on-exec descriptors are owned by this
  API. Rejection consumes nothing. Each accepted direction has one completion
  callback and independent counters. Original socket ownership and the existing
  full `Splice` contract stay with their callers. Zero-interest upload sockets are
  not polled, preventing an idle HUP loop.
- Regressions: `TestGoDirectionalSplice*`, `TestGoKernelDirectionalSplice`, the
  existing download/full-Splice tests, and Xray's Vision and counter tests.
- Retirement: prefer an upstream directional handoff once these ownership,
  readiness, half-close, backpressure and cancellation contracts pass without our
  implementation. Update both Xray callers and dependency pins together.

### ST-API-003 — borrowed vector write

- Status: active local extension.
- Location: `stack_go_tcp_write_vector.go`.
- Purpose: enter the native writer once for an Xray multi-buffer batch. Slices are
  borrowed synchronously and copied into the existing TCP retransmission store;
  they are never adopted across pools or retained after the call.
- Regressions: `TestGoWriteVector*` covers ordering, concurrent writers, partial
  results, deadlines and close wakeup.
- Retirement: use an equivalent upstream vector write after these tests and Xray
  buffer-release tests pass; do not equate a retaining API with this borrowed API.

### ST-PERF-001 — opt-in local TCP aggregation on plain Linux TUN

- Status: active local performance extension; default is off.
- Locations: `StackOptions.TCPLocalJumbo`, `stack_go_tcp_jumbo.go`, native Linux
  capability detection and TCP output/loss/validated-PTB hooks.
- Scope: Linux/Android, successfully verified plain TUN without virtio/GSO, peer
  exactly matching a configured local TUN address. The caller must guarantee
  delivery to this host, not forwarding or NAT onto another interface. Negotiated
  MSS, congestion accounting units, UDP and default paths are unchanged.
- Safety: full software checksums; payload bounded to 61440 even at maximum MTU,
  with TCP options and during concurrent fallback. Loss, retransmission, validated
  PTB or rejected oversized writes disable aggregation for the flow and use the
  existing retransmission machinery.
- Regressions: `TestGoLocalJumbo*`, `TestGoKernelLocalJumbo` (IPv4/IPv6,
  Cubic/BBR, ordinary/vector/full-Splice writers, injected jumbo loss).
- Retirement: compare upstream aggregation behavior and repeat checksum,
  fallback, ownership and performance controls. Prefer upstream when equivalent;
  keep only still-needed integration differences. Similar throughput alone does
  not establish equivalent packet and lifetime contracts.

## Change references

- ST-FD-001 through ST-FD-004: `8c6765b9b40c2f33da9925fdd48f1e06e2067df6`.
- ST-API-001: `66478c57be4c2807e07023e481d7a6c5a23281db`.
- macOS arm64: focused lifecycle and download API tests pass with `-race`.
- Linux/amd64 and Windows/amd64: new API test binaries compile. Linux privileged
  lifecycle and kernel-transfer regressions are additionally required by CI.

The Xray integration handoff records the exact dependency pin and final CI runs.
Keep that record synchronized when creating a new pin. Separate these patch IDs
even when several fixes share one commit; retire individual hunks as upstream
resolves them, without retaining the other hunks unnecessarily.
