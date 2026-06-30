//go:build linux

package bbolt_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// TestIOUring_WriteCommit opens a DB with UseIOUring=true, writes several
// key/value pairs, commits, then reopens the DB and verifies all data is
// readable.  If the kernel does not support io-uring the test is skipped.
func TestIOUring_WriteCommit(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{UseIOUring: true})

	// Confirm io-uring (or its fallback) did not break open.
	require.NotNil(t, db)

	const bucket = "testbucket"
	const numKeys = 50

	// Write phase.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		for i := 0; i < numKeys; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			val := []byte(fmt.Sprintf("val-%04d", i))
			if err := b.Put(key, val); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	dbPath := db.Path()

	// Close and reopen without io-uring to verify durability.
	require.NoError(t, db.Close())

	db2, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	// Read and verify phase.
	err = db2.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return fmt.Errorf("bucket %q not found after reopen", bucket)
		}
		for i := 0; i < numKeys; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			want := []byte(fmt.Sprintf("val-%04d", i))
			got := b.Get(key)
			if string(got) != string(want) {
				return fmt.Errorf("key %q: got %q, want %q", key, got, want)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// TestIOUring_MultipleTransactions exercises multiple sequential commits with
// io-uring enabled to confirm the ring handles repeated use correctly.
func TestIOUring_MultipleTransactions(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{UseIOUring: true})
	require.NotNil(t, db)

	const bucket = "multi"
	const rounds = 5

	for round := 0; round < rounds; round++ {
		err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(bucket))
			if err != nil {
				return err
			}
			for i := 0; i < 20; i++ {
				k := []byte(fmt.Sprintf("r%d-k%d", round, i))
				v := []byte(fmt.Sprintf("r%d-v%d", round, i))
				if err := b.Put(k, v); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)
	}

	// Verify all rounds are present.
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		require.NotNil(t, b)
		for round := 0; round < rounds; round++ {
			for i := 0; i < 20; i++ {
				k := []byte(fmt.Sprintf("r%d-k%d", round, i))
				want := []byte(fmt.Sprintf("r%d-v%d", round, i))
				got := b.Get(k)
				if string(got) != string(want) {
					return fmt.Errorf("round %d key %q: got %q want %q", round, k, got, want)
				}
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// TestIOUring_FallbackWhenDisabled confirms that a DB opened with
// UseIOUring=false (the default) still behaves correctly — i.e. the
// sequential writeAll path is functionally identical.
func TestIOUring_FallbackWhenDisabled(t *testing.T) {
	// Explicitly opt out of io-uring.
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{UseIOUring: false})
	require.NotNil(t, db)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("fallback"))
		if err != nil {
			return err
		}
		return b.Put([]byte("hello"), []byte("world"))
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("fallback"))
		require.NotNil(t, b)
		require.Equal(t, []byte("world"), b.Get([]byte("hello")))
		return nil
	})
	require.NoError(t, err)
}
