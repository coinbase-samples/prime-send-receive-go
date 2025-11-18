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
	"database/sql"
	"errors"
)

// Sentinel errors for database operations
var (
	ErrDuplicateTransaction   = errors.New("duplicate transaction")
	ErrConcurrentModification = errors.New("concurrent modification detected")
	ErrUserNotFound           = errors.New("no user found for address")
)

// SubledgerService handles subledger operations
type SubledgerService struct {
	db *sql.DB
}

func NewSubledgerService(db *sql.DB) *SubledgerService {
	return &SubledgerService{
		db: db,
	}
}

func (s *SubledgerService) InitSchema() error {
	schema := `
	-- Account Balances Table (Current State - Hot Data)
	CREATE TABLE IF NOT EXISTS account_balances (
		id TEXT PRIMARY KEY,
		user_id TEXT NOT NULL,
		asset TEXT NOT NULL,
		balance REAL NOT NULL DEFAULT 0,
		last_transaction_id TEXT,
		version INTEGER NOT NULL DEFAULT 1,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(user_id, asset)
	);

	-- Transactions Table (Audit Trail - Cold Data)
	CREATE TABLE IF NOT EXISTS transactions (
		id TEXT PRIMARY KEY,
		user_id TEXT NOT NULL,
		asset TEXT NOT NULL,
		transaction_type TEXT NOT NULL,
		amount REAL NOT NULL,
		balance_before REAL NOT NULL,
		balance_after REAL NOT NULL,
		external_transaction_id TEXT,
		address TEXT,
		reference TEXT,
		status TEXT DEFAULT 'confirmed',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	-- Performance Indexes for Account Balances
	CREATE INDEX IF NOT EXISTS idx_account_balances_user_id ON account_balances(user_id);
	CREATE INDEX IF NOT EXISTS idx_account_balances_asset ON account_balances(asset);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_account_balances_user_asset ON account_balances(user_id, asset);

	-- Performance Indexes for Transactions
	CREATE INDEX IF NOT EXISTS idx_transactions_user_asset ON transactions(user_id, asset);
	CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);
	CREATE INDEX IF NOT EXISTS idx_transactions_external_id ON transactions(external_transaction_id);
	CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
	CREATE INDEX IF NOT EXISTS idx_transactions_address ON transactions(address);
	CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);

	-- Optional: Journal Entries for Double-Entry Bookkeeping
	CREATE TABLE IF NOT EXISTS journal_entries (
		id TEXT PRIMARY KEY,
		transaction_id TEXT NOT NULL,
		account_type TEXT NOT NULL,
		account_id TEXT NOT NULL,
		debit_amount REAL DEFAULT 0,
		credit_amount REAL DEFAULT 0,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_journal_transaction_id ON journal_entries(transaction_id);
	CREATE INDEX IF NOT EXISTS idx_journal_account ON journal_entries(account_type, account_id);
	`

	_, err := s.db.Exec(schema)
	return err
}
