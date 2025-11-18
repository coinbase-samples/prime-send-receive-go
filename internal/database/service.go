/**
 * Copyright 2025-present Coinbase Global, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"prime-send-receive-go/internal/models"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type Service struct {
	db        *sql.DB
	subledger *SubledgerService
}

func NewService(ctx context.Context, cfg models.DatabaseConfig) (*Service, error) {
	// Validate configuration
	if cfg.Path == "" {
		return nil, fmt.Errorf("database path cannot be empty")
	}
	if cfg.MaxOpenConns <= 0 {
		return nil, fmt.Errorf("max open connections must be positive, got %d", cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns < 0 {
		return nil, fmt.Errorf("max idle connections cannot be negative, got %d", cfg.MaxIdleConns)
	}
	if cfg.PingTimeout <= 0 {
		return nil, fmt.Errorf("ping timeout must be positive, got %v", cfg.PingTimeout)
	}

	zap.L().Info("Opening SQLite database", zap.String("file", cfg.Path))
	db, err := sql.Open("sqlite3", cfg.Path+"?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=1000")
	if err != nil {
		return nil, fmt.Errorf("unable to open database: %w", err)
	}

	// Set connection timeouts and limits
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	// Test connection with timeout
	pingCtx, cancel := context.WithTimeout(ctx, cfg.PingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		err := db.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	subledger := NewSubledgerService(db)
	service := &Service{db: db, subledger: subledger}
	if err := service.initSchema(cfg.CreateDummyUsers); err != nil {
		err := db.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unable to initialize schema: %w", err)
	}

	// Initialize subledger schema
	if err := subledger.InitSchema(); err != nil {
		err := db.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("unable to initialize subledger schema: %w", err)
	}

	zap.L().Info("Database service initialized successfully")
	return service, nil
}

func (s *Service) Close() {
	if err := s.db.Close(); err != nil {
		zap.L().Warn("Failed to close database connection", zap.Error(err))
	}
}

func (s *Service) initSchema(createDummyUsers bool) error {
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

	// Insert 3 dummy users for testing if configured to do so
	if createDummyUsers {
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
				zap.L().Error("Failed to insert dummy user", zap.String("name", user.name), zap.Error(err))
			} else {
				zap.L().Info("Dummy user created", zap.String("id", user.id), zap.String("name", user.name))
			}
		}
	} else {
		zap.L().Info("Skipping dummy user creation (CREATE_DUMMY_USERS=false)")
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
		return fmt.Errorf("error finding user by address: %w", err)
	}

	if user == nil {
		zap.L().Warn("Deposit to unknown address", zap.String("address", address))
		return fmt.Errorf("no user found for address: %s", address)
	}

	// Use canonical symbol from address table (not Prime API's symbol which varies by network)
	// e.g., Prime API returns "BASEUSDC" but we store as symbol="USDC", network="base-mainnet"
	canonicalSymbol := addr.Asset

	if canonicalSymbol != asset {
		zap.L().Info("Using canonical symbol from address table",
			zap.String("address", address),
			zap.String("prime_api_symbol", asset),
			zap.String("canonical_symbol", canonicalSymbol),
			zap.String("network", addr.Network))
	}

	_, err = s.subledger.ProcessTransaction(ctx, ProcessTransactionParams{
		UserId:          user.Id,
		Asset:           canonicalSymbol,
		TransactionType: "deposit",
		Amount:          amount,
		ExternalTxId:    transactionId,
		Address:         address,
		Reference:       "",
	})
	if err != nil {
		return fmt.Errorf("error processing deposit transaction: %w", err)
	}

	zap.L().Info("Deposit processed successfully",
		zap.String("user_id", user.Id),
		zap.String("user_name", user.Name),
		zap.String("canonical_symbol", canonicalSymbol),
		zap.String("network", addr.Network),
		zap.String("amount", amount.String()))

	return nil
}

// ProcessWithdrawal processes a withdrawal transaction for a user by user Id
func (s *Service) ProcessWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, transactionId string) error {
	user, err := s.GetUserById(ctx, userId)
	if err != nil {
		zap.L().Warn("Withdrawal for unknown user", zap.String("user_id", userId))
		return fmt.Errorf("error getting user: %w", err)
	}

	// Get current balance for logging purposes (no validation for historical transactions)
	currentBalance, err := s.GetUserBalance(ctx, userId, asset)
	if err != nil {
		return fmt.Errorf("error getting current balance: %w", err)
	}

	zap.L().Info("Processing withdrawal information",
		zap.String("user_id", userId),
		zap.String("asset_network", asset),
		zap.String("current_balance", currentBalance.String()),
		zap.String("withdrawal_amount", amount.String()))

	_, err = s.subledger.ProcessTransaction(ctx, ProcessTransactionParams{
		UserId:          user.Id,
		Asset:           asset,
		TransactionType: "withdrawal",
		Amount:          amount.Neg(),
		ExternalTxId:    transactionId,
		Address:         "",
		Reference:       "",
	})
	if err != nil {
		return fmt.Errorf("error processing withdrawal transaction: %w", err)
	}

	zap.L().Info("Withdrawal processed successfully",
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

// ReverseWithdrawal credits back a withdrawal that failed (rollback)
func (s *Service) ReverseWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, originalTxId string) error {
	reversalTxId := originalTxId + "-reversal"

	zap.L().Info("Reversing failed withdrawal",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.String("amount", amount.String()),
		zap.String("original_tx", originalTxId),
		zap.String("reversal_tx", reversalTxId))

	// Credit back the amount (deposit to reverse the withdrawal)
	_, err := s.subledger.ProcessTransaction(ctx, ProcessTransactionParams{
		UserId:          userId,
		Asset:           asset,
		TransactionType: "deposit",
		Amount:          amount,
		ExternalTxId:    reversalTxId,
		Address:         "",
		Reference:       "Reversal of failed withdrawal",
	})
	if err != nil {
		return fmt.Errorf("error reversing withdrawal: %w", err)
	}

	zap.L().Info("Withdrawal reversed successfully",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.String("amount", amount.String()))

	return nil
}
