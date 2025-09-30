package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"prime-send-receive-go/internal/database/models"
)

type Service struct {
	db        *sql.DB
	logger    *zap.Logger
	subledger *SubledgerService
}

func NewService(ctx context.Context, logger *zap.Logger, dbPath string) (*Service, error) {
	logger.Info("Opening SQLite database", zap.String("file", dbPath))
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=1000")
	if err != nil {
		return nil, fmt.Errorf("unable to open database: %v", err)
	}

	// Set connection timeouts and limits
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(30 * time.Second)

	// Test connection with timeout
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		err := db.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unable to ping database: %v", err)
	}

	subledger := NewSubledgerService(db, logger)
	service := &Service{db: db, logger: logger, subledger: subledger}
	if err := service.initSchema(); err != nil {
		err := db.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unable to initialize schema: %v", err)
	}

	// Initialize subledger schema
	if err := subledger.InitSchema(); err != nil {
		err := db.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unable to initialize subledger schema: %v", err)
	}

	logger.Info("Database service initialized successfully")
	return service, nil
}

func (s *Service) Close() {
	err := s.db.Close()
	if err != nil {
		return
	}
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
		_, err := s.db.Exec(queryInsertUser, user.id, user.name, user.email)
		if err != nil {
			s.logger.Error("Failed to insert user", zap.String("name", user.name), zap.Error(err))
		} else {
			s.logger.Debug("User created", zap.String("id", user.id), zap.String("name", user.name))
		}
	}

	return nil
}

// Subledger convenience methods

func (s *Service) GetUserBalance(ctx context.Context, userId string, asset string) (decimal.Decimal, error) {
	return s.subledger.GetBalance(ctx, userId, asset)
}

func (s *Service) GetAllUserBalances(ctx context.Context, userId string) ([]models.AccountBalance, error) {
	return s.subledger.GetAllBalances(ctx, userId)
}

func (s *Service) ProcessDeposit(ctx context.Context, address, asset string, amount decimal.Decimal, transactionId string) error {
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

	_, err = s.subledger.ProcessTransaction(ctx, user.Id, asset, "deposit", amount, transactionId, address, "")
	if err != nil {
		return fmt.Errorf("error processing deposit transaction: %v", err)
	}

	s.logger.Info("Deposit processed successfully",
		zap.String("user_id", user.Id),
		zap.String("user_name", user.Name),
		zap.String("asset_network", asset),
		zap.String("amount", amount.String()))

	return nil
}

// ProcessWithdrawal processes a withdrawal transaction for a user by user Id
func (s *Service) ProcessWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, transactionId string) error {
	user, err := s.GetUserById(ctx, userId)
	if err != nil {
		s.logger.Warn("Withdrawal for unknown user", zap.String("user_id", userId))
		return fmt.Errorf("error getting user: %v", err)
	}

	// Get current balance for logging purposes (no validation for historical transactions)
	currentBalance, err := s.GetUserBalance(ctx, userId, asset)
	if err != nil {
		return fmt.Errorf("error getting current balance: %v", err)
	}

	s.logger.Info("Processing withdrawal information",
		zap.String("user_id", userId),
		zap.String("asset_network", asset),
		zap.String("current_balance", currentBalance.String()),
		zap.String("withdrawal_amount", amount.String()))

	_, err = s.subledger.ProcessTransaction(ctx, user.Id, asset, "withdrawal", amount.Neg(), transactionId, "", "")
	if err != nil {
		return fmt.Errorf("error processing withdrawal transaction: %v", err)
	}

	s.logger.Info("Withdrawal processed successfully",
		zap.String("user_id", user.Id),
		zap.String("user_name", user.Name),
		zap.String("asset_network", asset),
		zap.String("amount", amount.String()))

	return nil
}

func (s *Service) GetTransactionHistory(ctx context.Context, userId, asset string, limit, offset int) ([]models.Transaction, error) {
	return s.subledger.GetTransactionHistory(ctx, userId, asset, limit, offset)
}

func (s *Service) ReconcileUserBalance(ctx context.Context, userId, asset string) error {
	return s.subledger.ReconcileBalance(ctx, userId, asset)
}

func (s *Service) GetMostRecentTransactionTime(ctx context.Context) (time.Time, error) {
	return s.subledger.GetMostRecentTransactionTime(ctx)
}
