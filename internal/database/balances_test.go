package database

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

func setupBalanceTestDB(t *testing.T) (*Service, func()) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	logger := zap.NewNop()
	subledger := NewSubledgerService(db, logger)
	service := &Service{
		db:        db,
		subledger: subledger,
		logger:    logger,
	}

	// Use the actual schema initialization
	if err := subledger.InitSchema(); err != nil {
		t.Fatalf("Failed to create subledger schema: %v", err)
	}

	// Create additional tables needed for Service
	additionalSchema := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL UNIQUE
		);

		CREATE TABLE addresses (
			id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL,
			asset TEXT NOT NULL,
			network TEXT NOT NULL,
			address TEXT NOT NULL UNIQUE,
			wallet_id TEXT NOT NULL,
			account_identifier TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (user_id) REFERENCES users(id)
		);
	`

	if _, err := db.Exec(additionalSchema); err != nil {
		t.Fatalf("Failed to create additional test schema: %v", err)
	}

	// Insert test user
	_, err = db.Exec("INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
		"user1", "Test User", "test@example.com")
	if err != nil {
		t.Fatalf("Failed to insert test user: %v", err)
	}

	cleanup := func() {
		db.Close()
	}

	return service, cleanup
}

func TestGetUserBalanceV2_NoBalance(t *testing.T) {
	service, cleanup := setupBalanceTestDB(t)
	defer cleanup()

	ctx := context.Background()
	userId := "user1"
	asset := "BTC"

	balance, err := service.GetUserBalanceV2(ctx, userId, asset)
	if err != nil {
		t.Fatalf("GetUserBalanceV2 failed: %v", err)
	}

	// Should return 0 for non-existent balance
	if balance != 0 {
		t.Errorf("Expected balance 0, got %f", balance)
	}
}

func TestGetUserBalanceV2_WithTransactions(t *testing.T) {
	service, cleanup := setupBalanceTestDB(t)
	defer cleanup()

	ctx := context.Background()
	userId := "user1"
	asset := "BTC"

	// Create some transactions
	_, err := service.subledger.ProcessTransaction(ctx, userId, asset, "deposit", 2.0, "tx1", "addr1", "")
	if err != nil {
		t.Fatalf("Failed to create deposit: %v", err)
	}

	_, err = service.subledger.ProcessTransaction(ctx, userId, asset, "withdrawal", -0.5, "tx2", "", "")
	if err != nil {
		t.Fatalf("Failed to create withdrawal: %v", err)
	}

	balance, err := service.GetUserBalanceV2(ctx, userId, asset)
	if err != nil {
		t.Fatalf("GetUserBalanceV2 failed: %v", err)
	}

	expectedBalance := 1.5
	if balance != expectedBalance {
		t.Errorf("Expected balance %f, got %f", expectedBalance, balance)
	}
}

func TestGetAllUserBalancesV2(t *testing.T) {
	service, cleanup := setupBalanceTestDB(t)
	defer cleanup()

	ctx := context.Background()
	userId := "user1"

	// Create transactions for multiple assets
	_, err := service.subledger.ProcessTransaction(ctx, userId, "BTC", "deposit", 1.0, "tx1", "", "")
	if err != nil {
		t.Fatalf("Failed to create BTC deposit: %v", err)
	}

	_, err = service.subledger.ProcessTransaction(ctx, userId, "ETH", "deposit", 10.0, "tx2", "", "")
	if err != nil {
		t.Fatalf("Failed to create ETH deposit: %v", err)
	}

	balances, err := service.GetAllUserBalancesV2(ctx, userId)
	if err != nil {
		t.Fatalf("GetAllUserBalancesV2 failed: %v", err)
	}

	if len(balances) != 2 {
		t.Fatalf("Expected 2 balances, got %d", len(balances))
	}

	// Check that both assets are present
	found := make(map[string]float64)
	for _, balance := range balances {
		found[balance.Asset] = balance.Balance
	}

	if found["BTC"] != 1.0 {
		t.Errorf("Expected BTC balance 1.0, got %f", found["BTC"])
	}
	if found["ETH"] != 10.0 {
		t.Errorf("Expected ETH balance 10.0, got %f", found["ETH"])
	}
}
