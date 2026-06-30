# Plan: io-uring Support for Commit Write Path

## Top-Level Overview

bbolt currently writes dirty pages to disk during `Tx.Commit()` by calling `ops.writeAt`
(which is `os.File.WriteAt`) sequentially inside a loop in `tx.write()`. Each page is
written one at a time, waiting for each call to return before proceeding.

This plan introduces an optional io-uring-backed write path that batches all dirty-page
writes in a single io-uring submission, reducing syscall round-trips. The feature is:
- **Linux-only** — gated behind a `//go:build linux` file.
- **Opt-in** — controlled by a new `UseIOUring bool` field in `Options`; default is `false`.
- **Gracefully degrading** — if io-uring setup fails at open time (old kernel, restricted
  container, etc.), the DB falls back silently to the standard sequential `os.File.WriteAt`
  path and logs a warning.
- **Zero new runtime dependencies** — implemented using the raw Linux syscalls
  (`SYS_IO_URING_SETUP`, `SYS_IO_URING_ENTER`) that are already available via
  `golang.org/x/sys/unix`, which is already an indirect dependency through `bolt_unix.go`.

### Key design choice: separate `writeAll` function pointer

Rather than restructuring `tx.write()` fundamentally, a second function pointer is added
to `db.ops`:

```
ops struct {
    writeAt  func(b []byte, off int64) (n int, err error)   // unchanged
    writeAll func(writes []WriteRequest) error               // NEW
}
```

`tx.write()` is changed to collect all page-write operations into a `[]WriteRequest` slice
and then call `ops.writeAll` once. The default implementation of `writeAll` iterates the
slice and calls `ops.writeAt` sequentially (no behaviour change). The io-uring
implementation submits all writes to the ring in one `io_uring_enter` call, then harvests
completions.

`tx.writeMeta()` is a single write; it continues to call `ops.writeAt` directly and is
not batched.

---

## Sub-Tasks

---

### Sub-Task 1 — Add `UseIOUring` to `Options` and `DB`

**Status:** [x] done

**Intent:**  
Expose the opt-in knob to callers without breaking any existing API. The field on `Options`
is read during `Open()` and stored on `DB` so the write-path initialisation can use it.

**Expected Outcomes:**
- `Options.UseIOUring bool` field exists, documented, defaulting to `false`.
- `DB.useIOUring bool` (unexported) field exists, set from `options.UseIOUring` in `Open`.
- `Options.String()` updated to include the new field if it already renders all fields
  (check current implementation).
- No behaviour change — no write path wiring yet.

**Todo List:**
1. In `db.go`, add `UseIOUring bool` to the `Options` struct with a doc comment explaining
   it is Linux-only and silently falls back if unavailable.
2. In `db.go`, add unexported `useIOUring bool` field to the `DB` struct.
3. In `Open()` in `db.go`, add `db.useIOUring = options.UseIOUring` alongside the existing
   option assignments (around line 194).

**Relevant Context:**
- `Options` struct: `db.go` ~line 1321
- `DB` struct: `db.go` ~line 38
- `Open()` option assignments: `db.go` ~line 187–194
- Existing pattern to follow: `Mlock bool` in both `Options` and `DB`

---

### Sub-Task 2 — Introduce `WriteRequest` type and `writeAll` function pointer

**Status:** [x] done

**Intent:**  
Introduce the minimal abstraction that lets `tx.write()` hand a batch of writes to the ops
layer, with a default sequential implementation that preserves existing behaviour exactly.

**Expected Outcomes:**
- A new exported-or-unexported `writeRequest` struct (unexported, package-internal) with
  fields `buf []byte` and `offset int64`.
- `db.ops` gains a second function pointer: `writeAll func(reqs []writeRequest) error`.
- In `Open()`, `db.ops.writeAll` is initialized to a default closure that iterates `reqs`
  and calls `db.ops.writeAt` for each entry (sequential fallback, identical to current
  behaviour).
- `tx.write()` is updated to collect all write operations into a `[]writeRequest` slice
  and call `db.ops.writeAll(reqs)` once, rather than calling `db.ops.writeAt` inside the
  loop.
- All existing tests continue to pass.

**Todo List:**
1. In `db.go` (or a new `write.go`), define `type writeRequest struct { buf []byte; offset int64 }`.
2. In `db.go`, add `writeAll func(reqs []writeRequest) error` to the `ops` struct (next to `writeAt`).
3. In `Open()` in `db.go` (after line 260), initialise `db.ops.writeAll` as a sequential
   closure over `db.ops.writeAt`.
4. In `tx.go`, refactor `tx.write()`:
   - Replace the inner `tx.db.ops.writeAt(buf, offset)` call with an append to a local
     `reqs []writeRequest`.
   - After the loop, call `tx.db.ops.writeAll(reqs)` once.
   - Move the `tx.stats.IncWrite(n)` call to after `writeAll` returns, using `len(reqs)`
     as the count.

**Relevant Context:**
- `tx.write()`: `tx.go` ~line 520
- `db.ops` struct: `db.go` ~line 150
- `db.ops.writeAt` init: `db.go` ~line 260

---

### Sub-Task 3 — Implement io-uring write backend (Linux-only)

**Status:** [x] done

**Intent:**  
Provide the Linux io-uring implementation of `writeAll` that submits all page writes as
a batched SQE submission. This is gated behind `//go:build linux` and only wired in when
`db.useIOUring` is `true` and the ring initializes successfully.

**Expected Outcomes:**
- A new file `bolt_iouring_linux.go` (`//go:build linux`) containing:
  - `type ioUringWriter struct` — holds the ring file descriptor and mapped SQ/CQ memory.
  - `newIOUringWriter(fd int, queueDepth uint32) (*ioUringWriter, error)` — calls
    `SYS_IO_URING_SETUP`, maps the SQ/CQ rings and SQE array via `mmap`.
  - `(*ioUringWriter).writeAll(reqs []writeRequest) error` — populates SQEs for each
    request (opcode `IORING_OP_WRITE_FIXED` or `IORING_OP_WRITE`), calls
    `SYS_IO_URING_ENTER` to submit and wait, then harvests CQEs and checks for errors.
  - `(*ioUringWriter).close() error` — unmaps ring memory and closes the ring fd.
- A new file `bolt_noiouring.go` (`//go:build !linux`) that provides a stub
  `tryInitIOUring` returning `false, nil` so non-Linux builds compile cleanly.
- In `Open()` in `db.go`, after the default `writeAll` is set (Sub-Task 2), call
  `tryInitIOUring(db)`: if `db.useIOUring` is true, attempt `newIOUringWriter`; on
  success wire its `writeAll` method into `db.ops.writeAll`; on failure log a warning and
  leave the sequential default in place.
- `db.ioUringWriter` field (unexported `*ioUringWriter`) added to `DB` struct to allow
  `db.Close()` to call `ioUringWriter.close()`.

**Todo List:**
1. Create `bolt_iouring_linux.go` with build tag `//go:build linux`.
2. Implement ring setup using `unix.Syscall(unix.SYS_IO_URING_SETUP, ...)` (or
   `syscall.Syscall` for the setup syscall), mmap the SQ ring, CQ ring, and SQEs.
   Reference: kernel docs `Documentation/io_uring/io_uring.rst`; use struct layout
   constants from `golang.org/x/sys/unix` if available, otherwise define them locally.
3. Implement `writeAll`: fill SQEs in the SQ ring, advance the SQ tail, call
   `SYS_IO_URING_ENTER` with `submit = len(reqs)` and `min_complete = len(reqs)` and
   `IORING_ENTER_GETEVENTS` flag to block until all completions are available, then
   iterate CQEs and collect any non-zero `res` values as errors, advance CQ head.
4. Implement `close()` to unmap the three mmapped regions and close the ring fd.
5. Create `bolt_noiouring.go` (`//go:build !linux`) with a no-op `tryInitIOUring`.
6. In `db.go`, add `ioUringWriter *ioUringWriter` to `DB` struct (unexported).
7. In `db.go`'s `Open()`, call `tryInitIOUring(db)` after `db.ops.writeAll` is set.
8. In `db.close()` in `db.go`, if `db.ioUringWriter != nil`, call
   `db.ioUringWriter.close()`.

**Relevant Context:**
- `bolt_linux.go` — existing Linux-only file to mirror the pattern.
- `bolt_unix.go` — existing `//go:build` pattern, uses `unix.Mmap`.
- `mlock_unix.go` — pattern for a `//go:build !windows` stub.
- `db.close()`: `db.go` (search for `func (db *DB) close`).
- `golang.org/x/sys/unix` is already an indirect dep via `bolt_unix.go`.
- io-uring queue depth: start with 256 as a reasonable default; this can be a constant.

---

### Sub-Task 4 — Tests

**Status:** [x] done

**Intent:**  
Verify the io-uring path works correctly on Linux and that the fallback works on all
platforms. No separate test infrastructure is needed — extend existing patterns.

**Expected Outcomes:**
- A new test file `bolt_iouring_linux_test.go` (`//go:build linux`) that:
  - Opens a DB with `Options{UseIOUring: true}` and performs a write transaction.
  - Verifies the data is readable after the commit.
  - Tests the fallback: if `UseIOUring: true` but the system does not support it, the DB
    opens successfully and writes succeed.
- Existing test suite (`db_test.go`, `tx_test.go`) passes without modification, confirming
  no regression on the default sequential path.

**Todo List:**
1. Create `bolt_iouring_linux_test.go` with `//go:build linux`.
2. Add a test `TestIOUringWriteCommit` that opens a DB with `UseIOUring: true`, writes
   several key-value pairs in a transaction, commits, reopens the DB, and reads them back.
3. Add a test `TestIOUringFallback` that stubs or disables io-uring availability and
   confirms the DB still opens and commits successfully.
4. Run `go test ./...` to confirm all tests pass.

**Relevant Context:**
- Existing DB open/write test patterns in `db_test.go` and `tx_test.go`.
- `unix_test.go` for the `//go:build !windows` test pattern.

---

## Non-Goals

- Windows, macOS, or other non-Linux platform support for io-uring (not applicable).
- Vectored reads (only writes during commit are batched).
- io-uring-backed `fdatasync` (the existing `fdatasync` call is retained; only the page
  write loop is batched via io-uring).
- Tunable ring queue depth exposed via `Options` (use a fixed internal constant for now).
- Benchmarks (out of scope for this plan; can be added as a follow-up).
