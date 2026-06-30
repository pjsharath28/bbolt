//go:build linux

package bbolt

// io-uring support for batching dirty-page writes during Tx.Commit.
//
// This file is compiled only on Linux.  It uses the raw io_uring syscalls
// (SYS_IO_URING_SETUP / SYS_IO_URING_ENTER) together with the kernel struct
// layout documented in Documentation/io_uring/io_uring.rst.  No external
// library is required beyond golang.org/x/sys (already a dependency).
//
// The implementation follows the single-issuer, fixed-depth submission model:
//
//   1. Open a ring via io_uring_setup.
//   2. mmap the SQ ring, CQ ring, and SQE array as described by the params.
//   3. For each Tx.write() call, fill SQEs with IORING_OP_WRITE operations,
//      advance the SQ tail, then call io_uring_enter to submit *and* wait for
//      all completions (IORING_ENTER_GETEVENTS).
//   4. Harvest CQEs and propagate any per-operation errors.
//   5. On db.Close() unmap the three mmapped regions and close the ring fd.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// ---------------------------------------------------------------------------
// Kernel struct definitions (stable ABI since Linux 5.1)
// ---------------------------------------------------------------------------

// ioUringParams mirrors struct io_uring_params (120 bytes).
type ioUringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFd         uint32
	resv         [3]uint32
	sqOff        sqRingOffsets
	cqOff        cqRingOffsets
}

// sqRingOffsets mirrors struct io_sqring_offsets (40 bytes).
type sqRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	userAddr    uint64
}

// cqRingOffsets mirrors struct io_cqring_offsets (40 bytes).
type cqRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	userAddr    uint64
}

// ioUringSqe mirrors struct io_uring_sqe (64 bytes).
type ioUringSqe struct {
	opcode      uint8
	flags       uint8
	ioprio      uint16
	fd          int32
	off         uint64
	addr        uint64
	len         uint32
	opFlags     uint32
	userData    uint64
	bufIndex    uint16
	personality uint16
	spliceFdIn  int32
	_pad2       [2]uint64
}

// ioUringCqe mirrors struct io_uring_cqe (16 bytes).
type ioUringCqe struct {
	userData uint64
	res      int32
	flags    uint32
}

// io-uring opcodes / flags / enter flags.
const (
	iORING_OP_WRITE      = 23
	iORING_ENTER_GETEVENTS = 1
	iORING_DEFAULT_DEPTH   = 256
)

// ---------------------------------------------------------------------------
// ioUringWriter
// ---------------------------------------------------------------------------

// ioUringWriter holds the live io-uring ring for a single DB instance.
// Its zero value is not usable; always construct via newIOUringWriter.
type ioUringWriter struct {
	ringFd int // io-uring ring file descriptor
	dataFd int // database file descriptor (target of IORING_OP_WRITE)

	// SQ ring mapped region and its derived pointers.
	sqMem   []byte
	sqHead  *uint32
	sqTail  *uint32
	sqMask  *uint32
	sqArray *uint32 // base of the uint32 index array

	// CQ ring mapped region and its derived pointers.
	cqMem   []byte
	cqHead  *uint32
	cqTail  *uint32
	cqMask  *uint32
	cqCQEs  uintptr // byte offset within cqMem where the CQE array begins

	// SQE array mapped region.
	sqeMem []byte
}

// newIOUringWriter creates an io-uring ring of depth queueDepth associated
// with the file descriptor fd (the database file).
func newIOUringWriter(fd int, queueDepth uint32) (*ioUringWriter, error) {
	var params ioUringParams
	params.sqEntries = queueDepth

	// io_uring_setup(entries, params)
	r, _, errno := syscall.Syscall(unix.SYS_IO_URING_SETUP,
		uintptr(queueDepth),
		uintptr(unsafe.Pointer(&params)),
		0)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_setup: %w", errno)
	}
	ringFd := int(r)

	w := &ioUringWriter{ringFd: ringFd, dataFd: fd}

	// --- mmap SQ ring ---
	//
	// The SQ ring lives at offset IORING_OFF_SQ_RING (0) with size
	// sqOff.array + sqEntries*4.
	sqRingSize := uintptr(params.sqOff.array) + uintptr(params.sqEntries)*4
	sqMem, err := unix.Mmap(ringFd, 0, int(sqRingSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		_ = syscall.Close(ringFd)
		return nil, fmt.Errorf("mmap SQ ring: %w", err)
	}
	w.sqMem = sqMem
	w.sqHead = (*uint32)(unsafe.Pointer(&sqMem[params.sqOff.head]))
	w.sqTail = (*uint32)(unsafe.Pointer(&sqMem[params.sqOff.tail]))
	w.sqMask = (*uint32)(unsafe.Pointer(&sqMem[params.sqOff.ringMask]))
	w.sqArray = (*uint32)(unsafe.Pointer(&sqMem[params.sqOff.array]))

	// --- mmap SQE array ---
	//
	// The SQEs live at offset IORING_OFF_SQES (0x10000000).
	const iORING_OFF_SQES = 0x10000000
	sqeSize := uintptr(params.sqEntries) * 64 // sizeof(io_uring_sqe) == 64
	sqeMem, err := unix.Mmap(ringFd, iORING_OFF_SQES, int(sqeSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		_ = unix.Munmap(sqMem)
		_ = syscall.Close(ringFd)
		return nil, fmt.Errorf("mmap SQE array: %w", err)
	}
	w.sqeMem = sqeMem

	// --- mmap CQ ring ---
	//
	// The CQ ring lives at offset IORING_OFF_CQ_RING (0x8000000) with size
	// cqOff.cqes + cqEntries*16.
	const iORING_OFF_CQ_RING = 0x8000000
	cqRingSize := uintptr(params.cqOff.cqes) + uintptr(params.cqEntries)*16 // sizeof(io_uring_cqe) == 16
	cqMem, err := unix.Mmap(ringFd, iORING_OFF_CQ_RING, int(cqRingSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		_ = unix.Munmap(sqeMem)
		_ = unix.Munmap(sqMem)
		_ = syscall.Close(ringFd)
		return nil, fmt.Errorf("mmap CQ ring: %w", err)
	}
	w.cqMem = cqMem
	w.cqHead = (*uint32)(unsafe.Pointer(&cqMem[params.cqOff.head]))
	w.cqTail = (*uint32)(unsafe.Pointer(&cqMem[params.cqOff.tail]))
	w.cqMask = (*uint32)(unsafe.Pointer(&cqMem[params.cqOff.ringMask]))
	w.cqCQEs = uintptr(params.cqOff.cqes) // byte offset within cqMem

	return w, nil
}

// writeAll submits all write requests as a single io-uring batch and waits
// for all completions before returning.
//
// The number of requests must not exceed the ring queue depth (256 by
// default).  Callers that may exceed the depth must split into multiple
// batches; see tryInitIOUring for the splitting wrapper.
func (w *ioUringWriter) writeAll(reqs []writeRequest) error {
	if len(reqs) == 0 {
		return nil
	}

	// Fill SQEs.
	sqTail := atomic32Load(w.sqTail)
	mask := atomic32Load(w.sqMask)

	for i, req := range reqs {
		idx := (sqTail + uint32(i)) & mask

		// Write the uint32 SQ index array entry.
		// The array element at position idx stores the SQE slot index.
		storeUint32AtIndex(w.sqArray, int(idx), idx)

		sqe := w.sqeAt(idx)
		sqe.opcode = iORING_OP_WRITE
		sqe.flags = 0
		sqe.ioprio = 0
		sqe.fd = int32(w.dataFd)
		sqe.off = uint64(req.offset)
		sqe.addr = uint64(uintptr(unsafe.Pointer(&req.buf[0])))
		sqe.len = uint32(len(req.buf))
		sqe.opFlags = 0
		sqe.userData = uint64(i)
		sqe.bufIndex = 0
		sqe.personality = 0
		sqe.spliceFdIn = 0
	}

	// Advance SQ tail to publish the SQEs.
	atomic32Store(w.sqTail, sqTail+uint32(len(reqs)))

	// io_uring_enter(ringFd, toSubmit, minComplete, flags, sigset, sigsetSize)
	_, _, errno := syscall.Syscall6(unix.SYS_IO_URING_ENTER,
		uintptr(w.ringFd),
		uintptr(len(reqs)),
		uintptr(len(reqs)),
		iORING_ENTER_GETEVENTS,
		0, 0)
	if errno != 0 {
		return fmt.Errorf("io_uring_enter: %w", errno)
	}

	// Harvest CQEs and collect errors.
	var errs []error
	cqHead := atomic32Load(w.cqHead)
	cqTail := atomic32Load(w.cqTail)
	cqMask := atomic32Load(w.cqMask)

	for cqHead != cqTail {
		cqe := w.cqeAt(cqHead & cqMask)
		if cqe.res < 0 {
			errs = append(errs, fmt.Errorf("io-uring write (userData=%d offset=%d): %w",
				cqe.userData, reqs[cqe.userData].offset, syscall.Errno(-cqe.res)))
		}
		cqHead++
	}
	atomic32Store(w.cqHead, cqHead)

	return errors.Join(errs...)
}

// close unmaps all ring memory and closes the ring file descriptor.
func (w *ioUringWriter) close() error {
	var errs []error
	if w.cqMem != nil {
		if err := unix.Munmap(w.cqMem); err != nil {
			errs = append(errs, fmt.Errorf("munmap CQ ring: %w", err))
		}
		w.cqMem = nil
	}
	if w.sqeMem != nil {
		if err := unix.Munmap(w.sqeMem); err != nil {
			errs = append(errs, fmt.Errorf("munmap SQE array: %w", err))
		}
		w.sqeMem = nil
	}
	if w.sqMem != nil {
		if err := unix.Munmap(w.sqMem); err != nil {
			errs = append(errs, fmt.Errorf("munmap SQ ring: %w", err))
		}
		w.sqMem = nil
	}
	if w.ringFd >= 0 {
		if err := syscall.Close(w.ringFd); err != nil {
			errs = append(errs, fmt.Errorf("close ring fd: %w", err))
		}
		w.ringFd = -1
	}
	return errors.Join(errs...)
}

// ---------------------------------------------------------------------------
// Ring memory helpers
// ---------------------------------------------------------------------------

// sqeAt returns a pointer to the SQE at slot index idx in the SQE array.
func (w *ioUringWriter) sqeAt(idx uint32) *ioUringSqe {
	return (*ioUringSqe)(unsafe.Pointer(&w.sqeMem[uint64(idx)*64]))
}

// cqeAt returns a pointer to the CQE at position pos in the CQ ring.
func (w *ioUringWriter) cqeAt(pos uint32) *ioUringCqe {
	offset := w.cqCQEs + uintptr(pos)*16
	return (*ioUringCqe)(unsafe.Pointer(&w.cqMem[offset]))
}

// storeUint32AtIndex stores v at the idx-th element of the uint32 array
// rooted at base.
func storeUint32AtIndex(base *uint32, idx int, v uint32) {
	p := unsafe.Pointer(uintptr(unsafe.Pointer(base)) + uintptr(idx)*4)
	*(*uint32)(p) = v
}

// atomic32Load performs a load-acquire on a uint32 shared with the kernel.
// Go's memory model treats normal pointer reads as sequentially consistent on
// amd64/arm64, but we use binary.LittleEndian on the backing byte slice to
// make the intent explicit and avoid any compiler reordering.
func atomic32Load(p *uint32) uint32 {
	// Read through the pointer directly; the kernel uses the same cache-line.
	b := (*[4]byte)(unsafe.Pointer(p))[:]
	return binary.LittleEndian.Uint32(b)
}

// atomic32Store performs a store-release on a uint32 shared with the kernel.
func atomic32Store(p *uint32, v uint32) {
	b := (*[4]byte)(unsafe.Pointer(p))[:]
	binary.LittleEndian.PutUint32(b, v)
}

// ---------------------------------------------------------------------------
// tryInitIOUring — wired into Open()
// ---------------------------------------------------------------------------

// tryInitIOUring attempts to initialise the io-uring ring for db if the
// caller requested it (db.useIOUring == true).  On success it replaces
// db.ops.writeAll with the batched io-uring implementation and stores the
// writer in db.ioUringWriter.
//
// On any failure (old kernel, seccomp, container restrictions) it logs a
// warning and leaves db.ops.writeAll as the sequential default.
func tryInitIOUring(db *DB) {
	if !db.useIOUring {
		return
	}

	depth := uint32(iORING_DEFAULT_DEPTH)
	w, err := newIOUringWriter(int(db.file.Fd()), depth)
	if err != nil {
		db.logger.Warningf("io-uring requested but unavailable, falling back to sequential writes: %v", err)
		return
	}

	db.ioUringWriter = w

	// Replace writeAll with the io-uring batched implementation.
	// Large commits (> depth requests) are split into sub-batches so the
	// ring is never overflowed.
	db.ops.writeAll = func(reqs []writeRequest) error {
		for len(reqs) > 0 {
			batch := reqs
			if uint32(len(batch)) > depth {
				batch = reqs[:depth]
			}
			if err := w.writeAll(batch); err != nil {
				return err
			}
			reqs = reqs[len(batch):]
		}
		return nil
	}
}
