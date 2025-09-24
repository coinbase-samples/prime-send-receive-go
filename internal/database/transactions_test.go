package database

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

func setupTestDB(t *testing.T) (*SubledgerService, func()) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	logger := zap.NewNop()
	service := NewSubledgerService(db, logger)

	// Use the actual schema initialization
	if err := service.InitSchema(); err != nil {
		t.Fatalf("Failed to create test schema: %v", err)
	}

	cleanup := func() {
		db.Close()
	}

	return service, cleanup
}

func TestProcessTransaction_Deposit(t *testing.T) {
	service, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	userId := "user1"
	asset := "BTC"
	amount := 1.5

	// Process deposit
	result, err := service.ProcessTransaction(ctx, userId, asset, "deposit", amount, "tx1", "addr1", "memo1")
	if err != nil {
		t.Fatalf("ProcessTransaction failed: %v", err)
	}

	// Verify result
	if result.UserId != userId {
		t.Errorf("Expected userId %s, got %s", userId, result.UserId)
	}
	if result.Asset != asset {
		t.Errorf("Expected asset %s, got %s", asset, result.Asset)
	}
	if result.Amount != amount {
		t.Errorf("Expected amount %f, got %f", amount, result.Amount)
	}
	if result.BalanceAfter != amount {
		t.Errorf("Expected balance %f, got %f", amount, result.BalanceAfter)
	}
}

func TestProcessTransaction_Withdrawal(t *testing.T) {
	service, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	userId := "user1"
	asset := "BTC"

	// First, make a deposit
	_, err := service.ProcessTransaction(ctx, userId, asset, "deposit", 2.0, "tx1", "addr1", "")
	if err != nil {
		t.Fatalf("Initial deposit failed: %v", err)
	}

	// Now process withdrawal (should be negative amount)
	withdrawalAmount := -0.5
	result, err := service.ProcessTransaction(ctx, userId, asset, "withdrawal", withdrawalAmount, "tx2", "", "")
	if err != nil {
		t.Fatalf("ProcessTransaction withdrawal failed: %v", err)
	}

	// Verify result - balance should be 2.0 + (-0.5) = 1.5
	expectedBalance := 1.5
	if result.BalanceAfter != expectedBalance {
		t.Errorf("Expected balance %f, got %f", expectedBalance, result.BalanceAfter)
	}
}

func TestProcessTransaction_DuplicateHandling(t *testing.T) {
	service, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	userId := "user1"
	asset := "BTC"
	amount := 1.0
	txId := "duplicate-tx"

	// Process transaction first time
	_, err := service.ProcessTransaction(ctx, userId, asset, "deposit", amount, txId, "addr1", "")
	if err != nil {
		t.Fatalf("First ProcessTransaction failed: %v", err)
	}

	// Process same transaction again - should return error for duplicate
	_, err = service.ProcessTransaction(ctx, userId, asset, "deposit", amount, txId, "addr1", "")
	if err == nil {
		t.Fatalf("Expected duplicate transaction error, got nil")
	}

	// Should contain "duplicate transaction" in error message
	if !strings.Contains(err.Error(), "duplicate transaction") {
		t.Errorf("Expected duplicate transaction error, got: %v", err)
	}
}

func TestProcessTransaction_NegativeBalanceAllowed(t *testing.T) {
	service, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	userId := "user1"
	asset := "BTC"

	// Process withdrawal from zero balance (should be allowed for historical transactions)
	withdrawalAmount := -1.0
	result, err := service.ProcessTransaction(ctx, userId, asset, "withdrawal", withdrawalAmount, "tx1", "", "")
	if err != nil {
		t.Fatalf("ProcessTransaction with negative balance failed: %v", err)
	}

	// Balance should be negative
	if result.BalanceAfter != withdrawalAmount {
		t.Errorf("Expected negative balance %f, got %f", withdrawalAmount, result.BalanceAfter)
	}
}
