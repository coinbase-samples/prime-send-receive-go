package database

import (
	"database/sql"
	"time"

	"go.uber.org/zap"
)

// AccountBalance represents current balance state (hot data)
type AccountBalance struct {
	Id                string    `db:"id"`
	UserId            string    `db:"user_id"`
	Asset             string    `db:"asset"`
	Balance           float64   `db:"balance"`
	LastTransactionId string    `db:"last_transaction_id"`
	Version           int64     `db:"version"`
	UpdatedAt         time.Time `db:"updated_at"`
}

// Transaction represents immutable transaction history (cold data)
type Transaction struct {
	Id                    string    `db:"id"`
	UserId                string    `db:"user_id"`
	Asset                 string    `db:"asset"`
	TransactionType       string    `db:"transaction_type"`
	Amount                float64   `db:"amount"`
	BalanceBefore         float64   `db:"balance_before"`
	BalanceAfter          float64   `db:"balance_after"`
	ExternalTransactionId string    `db:"external_transaction_id"`
	Address               string    `db:"address"`
	Reference             string    `db:"reference"`
	Status                string    `db:"status"`
	CreatedAt             time.Time `db:"created_at"`
	ProcessedAt           time.Time `db:"processed_at"`
}

// SubledgerService handles production-ready subledger operations
type SubledgerService struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewSubledgerService(db *sql.DB, logger *zap.Logger) *SubledgerService {
	return &SubledgerService{
		db:     db,
		logger: logger,
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
