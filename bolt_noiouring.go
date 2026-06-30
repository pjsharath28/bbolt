//go:build !linux

package bbolt

// ioUringWriter is a placeholder type on non-Linux platforms so that the
// DB struct compiles everywhere.  It has no fields and its methods are
// never called.
type ioUringWriter struct{}

func (w *ioUringWriter) close() error { return nil }

// tryInitIOUring is a no-op on non-Linux platforms.
func tryInitIOUring(_ *DB) {}
