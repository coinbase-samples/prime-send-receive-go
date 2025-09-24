package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

type Service struct {
	db        *sql.DB
	logger    *zap.Logger
	subledger *SubledgerService
}

type User struct {
	Id        string    `db:"id"`
	Name      string    `db:"name"`
	Email     string    `db:"email"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

type Address struct {
	Id                string    `db:"id"`
	UserId            string    `db:"user_id"`
	Asset             string    `db:"asset"`
	Network           string    `db:"network"`
	Address           string    `db:"address"`
	WalletId          string    `db:"wallet_id"`
	AccountIdentifier string    `db:"account_identifier"`
	CreatedAt         time.Time `db:"created_at"`
}

type LedgerEntry struct {
	Id              string    `db:"id"`
	UserId          string    `db:"user_id"`
	Asset           string    `db:"asset"`
	Balance         float64   `db:"balance"`
	TransactionId   string    `db:"transaction_id"`
	TransactionType string    `db:"transaction_type"`
	Amount          float64   `db:"amount"`
	Address         string    `db:"address"`
	CreatedAt       time.Time `db:"created_at"`
}

type UserBalance struct {
	UserId  string  `db:"user_id"`
	Asset   string  `db:"asset"`
	Balance float64 `db:"balance"`
}

func NewService(ctx context.Context, logger *zap.Logger, dbPath string) (*Service, error) {
	logger.Info("Opening SQLite database", zap.String("file", dbPath))
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=1000")
	if err != nil {
		return nil, fmt.Errorf("unable to open database: %v", err)
	}

	subledger := NewSubledgerService(db, logger)
	service := &Service{db: db, logger: logger, subledger: subledger}
	if err := service.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("unable to initialize schema: %v", err)
	}

	// Initialize subledger schema
	if err := subledger.InitSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("unable to initialize subledger schema: %v", err)
	}

	logger.Info("Database service initialized successfully")
	return service, nil
}

func (s *Service) Close() {
	s.db.Close()
}

func (s *Service) initSchema() error {
	schema := `
	-- Create users table
	CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT NOT NULL UNIQUE,
		active BOOLEAN NOT NULL DEFAULT 1,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	-- Create index on email for faster lookups
	CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
	-- Create index on active users
	CREATE INDEX IF NOT EXISTS idx_users_active ON users(active);

	-- Create addresses table to store generated deposit addresses
	CREATE TABLE IF NOT EXISTS addresses (
		id TEXT PRIMARY KEY,
		user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		asset TEXT NOT NULL,
		network TEXT NOT NULL,
		address TEXT NOT NULL,
		wallet_id TEXT NOT NULL,
		account_identifier TEXT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	-- Create index for user/asset lookups
	CREATE INDEX IF NOT EXISTS idx_addresses_user_asset ON addresses(user_id, asset);
	-- Create index for address lookups
	CREATE INDEX IF NOT EXISTS idx_addresses_address ON addresses(address);
	-- Create index for wallet_id lookups
	CREATE INDEX IF NOT EXISTS idx_addresses_wallet_id ON addresses(wallet_id);
	-- Create index for created_at for sorting
	CREATE INDEX IF NOT EXISTS idx_addresses_created_at ON addresses(created_at);

	-- Create ledger table to track user balances and transactions
	CREATE TABLE IF NOT EXISTS ledger (
		id TEXT PRIMARY KEY,
		user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		asset TEXT NOT NULL,
		balance REAL NOT NULL DEFAULT 0,
		transaction_id TEXT,
		transaction_type TEXT NOT NULL,
		amount REAL NOT NULL DEFAULT 0,
		address TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	-- Create index for user/asset balance lookups
	CREATE INDEX IF NOT EXISTS idx_ledger_user_asset ON ledger(user_id, asset);
	-- Create index for transaction_id lookups
	CREATE INDEX IF NOT EXISTS idx_ledger_transaction_id ON ledger(transaction_id);
	-- Create index for address lookups
	CREATE INDEX IF NOT EXISTS idx_ledger_address ON ledger(address);
	-- Create index for created_at for sorting
	CREATE INDEX IF NOT EXISTS idx_ledger_created_at ON ledger(created_at);

	`

	_, err := s.db.Exec(schema)
	if err != nil {
		return err
	}

	// Insert 3 dummy users for testing with real deposits
	users := []struct {
		id    string
		name  string
		email string
	}{
		{uuid.New().String(), "Alice Johnson", "alice.johnson@example.com"},
		{uuid.New().String(), "Bob Smith", "bob.smith@example.com"},
		{uuid.New().String(), "Carol Williams", "carol.williams@example.com"},
	}

	for _, user := range users {
		_, err := s.db.Exec("INSERT OR IGNORE INTO users (id, name, email) VALUES (?, ?, ?)",
			user.id, user.name, user.email)
		if err != nil {
			s.logger.Error("Failed to insert user", zap.String("name", user.name), zap.Error(err))
		} else {
			s.logger.Debug("User created", zap.String("id", user.id), zap.String("name", user.name))
		}
	}

	return nil
}

// Subledger convenience methods

func (s *Service) GetUserBalanceV2(ctx context.Context, userId string, asset string) (float64, error) {
	return s.subledger.GetBalance(ctx, userId, asset)
}

func (s *Service) GetAllUserBalancesV2(ctx context.Context, userId string) ([]AccountBalance, error) {
	return s.subledger.GetAllBalances(ctx, userId)
}

func (s *Service) ProcessDepositV2(ctx context.Context, address, asset string, amount float64, transactionId string) error {
	// Find user by address
	user, addr, err := s.FindUserByAddress(ctx, address)
	if err != nil {
		return fmt.Errorf("error finding user by address: %v", err)
	}

	if user == nil {
		s.logger.Warn("Deposit to unknown address", zap.String("address", address))
		return fmt.Errorf("no user found for address: %s", address)
	}

	// Verify asset matches
	if addr.Asset != asset {
		s.logger.Warn("Asset mismatch for deposit",
			zap.String("address", address),
			zap.String("expected_asset", addr.Asset),
			zap.String("received_asset", asset))
		return fmt.Errorf("asset mismatch: expected %s, received %s", addr.Asset, asset)
	}

	// Process transaction using new subledger
	_, err = s.subledger.ProcessTransaction(ctx, user.Id, asset, "deposit", amount, transactionId, address, "")
	if err != nil {
		return fmt.Errorf("error processing deposit transaction: %v", err)
	}

	s.logger.Info("Deposit processed successfully using V2 subledger",
		zap.String("user_id", user.Id),
		zap.String("user_name", user.Name),
		zap.String("asset", asset),
		zap.Float64("amount", amount))

	return nil
}

// ProcessWithdrawalV2 processes a withdrawal transaction for a user by user Id
func (s *Service) ProcessWithdrawalV2(ctx context.Context, userId, asset string, amount float64, transactionId string) error {
	// Get all users and find by Id
	users, err := s.GetUsers(ctx)
	if err != nil {
		return fmt.Errorf("error getting users: %v", err)
	}

	var user *User
	for _, u := range users {
		if u.Id == userId {
			user = &u
			break
		}
	}

	if user == nil {
		s.logger.Warn("Withdrawal for unknown user", zap.String("user_id", userId))
		return fmt.Errorf("no user found for Id: %s", userId)
	}

	// Get current balance for logging purposes (no validation for historical transactions)
	currentBalance, err := s.GetUserBalanceV2(ctx, userId, asset)
	if err != nil {
		return fmt.Errorf("error getting current balance: %v", err)
	}

	s.logger.Info("Processing withdrawal information",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.Float64("current_balance", currentBalance),
		zap.Float64("withdrawal_amount", amount))

	// Process transaction using new subledger (negate amount for withdrawal)
	_, err = s.subledger.ProcessTransaction(ctx, user.Id, asset, "withdrawal", -amount, transactionId, "", "")
	if err != nil {
		return fmt.Errorf("error processing withdrawal transaction: %v", err)
	}

	s.logger.Info("Withdrawal processed successfully using V2 subledger",
		zap.String("user_id", user.Id),
		zap.String("user_name", user.Name),
		zap.String("asset", asset),
		zap.Float64("amount", amount))

	return nil
}

func (s *Service) GetTransactionHistoryV2(ctx context.Context, userId, asset string, limit, offset int) ([]Transaction, error) {
	return s.subledger.GetTransactionHistory(ctx, userId, asset, limit, offset)
}

func (s *Service) ReconcileUserBalance(ctx context.Context, userId, asset string) error {
	return s.subledger.ReconcileBalance(ctx, userId, asset)
}

func (s *Service) GetMostRecentTransactionTime(ctx context.Context) (time.Time, error) {
	return s.subledger.GetMostRecentTransactionTime(ctx)
}
