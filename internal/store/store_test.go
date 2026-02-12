package store

import (
	"testing"
)

// Compile-time checks that the interface is importable and usable.
func TestLedgerStoreInterfaceExists(t *testing.T) {
	// This test simply validates that the LedgerStore interface compiles
	// and the sentinel errors are accessible.
	_ = ErrDuplicateTransaction
	_ = ErrConcurrentModification
	_ = ErrUserNotFound
	_ = StoreAddressParams{}

	// Ensure the interface is non-nil type.
	var _ LedgerStore
}
