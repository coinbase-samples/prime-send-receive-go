package database

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/shopspring/decimal"
)

func setupBalanceTestDB(t *testing.T) (*Service, func()) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	subledger := NewSubledgerService(db)
	service := &Service{
		db:        db,
		subledger: subledger,
	}

	if err := subledger.InitSchema(); err != nil {
		t.Fatalf("Failed to create subledger schema: %v", err)
	}

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

func TestGetUserBalance_NoBalance(t *testing.T) {
	service, cleanup := setupBalanceTestDB(t)
	defer cleanup()

	ctx := context.Background()
	userId := "user1"
	asset := "BTC"

	balance, err := service.GetUserBalance(ctx, userId, asset)
	if err != nil {
		t.Fatalf("GetUserBalance failed: %v", err)
	}

	if !balance.Equal(decimal.Zero) {
		t.Errorf("Expected balance 0, got %s", balance.String())
	}
}

func TestGetUserBalance_WithTransactions(t *testing.T) {
	service, cleanup := setupBalanceTestDB(t)
	defer cleanup()

	ctx := context.Background()
	userId := "user1"
	asset := "BTC"

	depositAmount := decimal.NewFromFloat(2.0)
	_, err := service.subledger.ProcessTransaction(ctx, ProcessTransactionParams{userId, asset, "deposit", depositAmount, "tx1", "addr1", ""})
	if err != nil {
		t.Fatalf("Failed to create deposit: %v", err)
	}

	withdrawalAmount := decimal.NewFromFloat(-0.5)
	_, err = service.subledger.ProcessTransaction(ctx, ProcessTransactionParams{userId, asset, "withdrawal", withdrawalAmount, "tx2", "", ""})
	if err != nil {
		t.Fatalf("Failed to create withdrawal: %v", err)
	}

	balance, err := service.GetUserBalance(ctx, userId, asset)
	if err != nil {
		t.Fatalf("GetUserBalance failed: %v", err)
	}

	expectedBalance := decimal.NewFromFloat(1.5)
	if !balance.Equal(expectedBalance) {
		t.Errorf("Expected balance %s, got %s", expectedBalance.String(), balance.String())
	}
}

func TestGetAllUserBalances(t *testing.T) {
	service, cleanup := setupBalanceTestDB(t)
	defer cleanup()

	ctx := context.Background()
	userId := "user1"

	btcAmount := decimal.NewFromFloat(1.0)
	_, err := service.subledger.ProcessTransaction(ctx, ProcessTransactionParams{userId, "BTC", "deposit", btcAmount, "tx1", "", ""})
	if err != nil {
		t.Fatalf("Failed to create BTC deposit: %v", err)
	}

	ethAmount := decimal.NewFromFloat(10.0)
	_, err = service.subledger.ProcessTransaction(ctx, ProcessTransactionParams{userId, "ETH", "deposit", ethAmount, "tx2", "", ""})
	if err != nil {
		t.Fatalf("Failed to create ETH deposit: %v", err)
	}

	balances, err := service.GetAllUserBalances(ctx, userId)
	if err != nil {
		t.Fatalf("GetAllUserBalances failed: %v", err)
	}

	if len(balances) != 2 {
		t.Fatalf("Expected 2 balances, got %d", len(balances))
	}

	found := make(map[string]decimal.Decimal)
	for _, balance := range balances {
		found[balance.Asset] = balance.Balance
	}

	expectedBTC := decimal.NewFromFloat(1.0)
	if !found["BTC"].Equal(expectedBTC) {
		t.Errorf("Expected BTC balance %s, got %s", expectedBTC.String(), found["BTC"].String())
	}
	expectedETH := decimal.NewFromFloat(10.0)
	if !found["ETH"].Equal(expectedETH) {
		t.Errorf("Expected ETH balance %s, got %s", expectedETH.String(), found["ETH"].String())
	}
}
