package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// ProcessTransaction atomically updates balance and records transaction
func (s *SubledgerService) ProcessTransaction(ctx context.Context, userId, asset, transactionType string,
	amount float64, externalTxId, address, reference string) (*Transaction, error) {

	s.logger.Info("Processing transaction",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.String("type", transactionType),
		zap.Float64("amount", amount),
		zap.String("external_tx_id", externalTxId))

	// Check for duplicate external transaction Id
	if externalTxId != "" {
		var existingTxId string
		duplicateQuery := `SELECT id FROM transactions WHERE external_transaction_id = ? LIMIT 1`
		err := s.db.QueryRowContext(ctx, duplicateQuery, externalTxId).Scan(&existingTxId)
		if err == nil {
			s.logger.Warn("Duplicate external transaction Id detected, skipping",
				zap.String("external_tx_id", externalTxId),
				zap.String("existing_internal_tx_id", existingTxId))
			return nil, fmt.Errorf("duplicate transaction: external_transaction_id %s already exists", externalTxId)
		} else if err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to check for duplicate transaction: %v", err)
		}
	}

	// Start database transaction for atomicity
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Get current balance (with row locking)
	var currentBalance float64
	var accountId string
	var version int64

	balanceQuery := `
		SELECT id, balance, version 
		FROM account_balances 
		WHERE user_id = ? AND asset = ?
	`
	err = tx.QueryRowContext(ctx, balanceQuery, userId, asset).Scan(&accountId, &currentBalance, &version)
	if err == sql.ErrNoRows {
		// Create new account balance record
		accountId = uuid.New().String()
		currentBalance = 0
		version = 1

		insertQuery := `
			INSERT INTO account_balances (id, user_id, asset, balance, version)
			VALUES (?, ?, ?, ?, ?)
		`
		_, err = tx.ExecContext(ctx, insertQuery, accountId, userId, asset, 0, 1)
		if err != nil {
			return nil, fmt.Errorf("failed to create account balance: %v", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to get current balance: %v", err)
	}

	// Calculate new balance
	newBalance := currentBalance + amount

	// Create transaction record
	transactionId := uuid.New().String()
	transaction := &Transaction{
		Id:                    transactionId,
		UserId:                userId,
		Asset:                 asset,
		TransactionType:       transactionType,
		Amount:                amount,
		BalanceBefore:         currentBalance,
		BalanceAfter:          newBalance,
		ExternalTransactionId: externalTxId,
		Address:               address,
		Reference:             reference,
		Status:                "confirmed",
		CreatedAt:             time.Now(),
		ProcessedAt:           time.Now(),
	}

	insertTxQuery := `
		INSERT INTO transactions (
			id, user_id, asset, transaction_type, amount, balance_before, balance_after,
			external_transaction_id, address, reference, status, created_at, processed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err = tx.ExecContext(ctx, insertTxQuery,
		transaction.Id, transaction.UserId, transaction.Asset, transaction.TransactionType,
		transaction.Amount, transaction.BalanceBefore, transaction.BalanceAfter,
		transaction.ExternalTransactionId, transaction.Address, transaction.Reference,
		transaction.Status, transaction.CreatedAt, transaction.ProcessedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to insert transaction: %v", err)
	}

	// Update account balance (with optimistic locking)
	updateBalanceQuery := `
		UPDATE account_balances 
		SET balance = ?, last_transaction_id = ?, version = version + 1, updated_at = CURRENT_TIMESTAMP
		WHERE user_id = ? AND asset = ? AND version = ?
	`
	result, err := tx.ExecContext(ctx, updateBalanceQuery, newBalance, transactionId, userId, asset, version)
	if err != nil {
		return nil, fmt.Errorf("failed to update balance: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to check rows affected: %v", err)
	}
	if rowsAffected == 0 {
		return nil, fmt.Errorf("balance update failed - concurrent modification detected")
	}

	// Optional: Add double-entry journal entries
	if err := s.addJournalEntries(ctx, tx, transaction); err != nil {
		return nil, fmt.Errorf("failed to add journal entries: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %v", err)
	}

	s.logger.Info("Transaction processed successfully",
		zap.String("transaction_id", transactionId),
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.Float64("old_balance", currentBalance),
		zap.Float64("new_balance", newBalance))

	return transaction, nil
}

// addJournalEntries creates double-entry bookkeeping entries
func (s *SubledgerService) addJournalEntries(ctx context.Context, tx *sql.Tx, transaction *Transaction) error {
	// For a deposit: Debit user asset account, Credit system liability account
	// For a withdrawal: Credit user asset account, Debit system liability account

	journalEntries := []struct {
		accountType  string
		accountId    string
		debitAmount  float64
		creditAmount float64
	}{}

	switch transaction.TransactionType {
	case "deposit":
		// User asset account increases (debit)
		journalEntries = append(journalEntries, struct {
			accountType  string
			accountId    string
			debitAmount  float64
			creditAmount float64
		}{"user_asset", fmt.Sprintf("%s_%s", transaction.UserId, transaction.Asset), transaction.Amount, 0})

		// System liability increases (credit) - we owe the user this amount
		journalEntries = append(journalEntries, struct {
			accountType  string
			accountId    string
			debitAmount  float64
			creditAmount float64
		}{"system_liability", fmt.Sprintf("user_deposits_%s", transaction.Asset), 0, transaction.Amount})

	case "withdrawal":
		// User asset account decreases (credit)
		journalEntries = append(journalEntries, struct {
			accountType  string
			accountId    string
			debitAmount  float64
			creditAmount float64
		}{"user_asset", fmt.Sprintf("%s_%s", transaction.UserId, transaction.Asset), 0, -transaction.Amount})

		// System liability decreases (debit) - we no longer owe the user this amount
		journalEntries = append(journalEntries, struct {
			accountType  string
			accountId    string
			debitAmount  float64
			creditAmount float64
		}{"system_liability", fmt.Sprintf("user_deposits_%s", transaction.Asset), -transaction.Amount, 0})
	}

	for _, entry := range journalEntries {
		entryId := uuid.New().String()
		insertJournalQuery := `
			INSERT INTO journal_entries (id, transaction_id, account_type, account_id, debit_amount, credit_amount)
			VALUES (?, ?, ?, ?, ?, ?)
		`
		_, err := tx.ExecContext(ctx, insertJournalQuery,
			entryId, transaction.Id, entry.accountType, entry.accountId, entry.debitAmount, entry.creditAmount)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetTransactionHistory returns paginated transaction history for a user
func (s *SubledgerService) GetTransactionHistory(ctx context.Context, userId, asset string, limit, offset int) ([]Transaction, error) {
	s.logger.Debug("Getting transaction history",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.Int("limit", limit),
		zap.Int("offset", offset))

	query := `
		SELECT id, user_id, asset, transaction_type, amount, balance_before, balance_after,
		       external_transaction_id, address, reference, status, created_at, processed_at
		FROM transactions 
		WHERE user_id = ? AND asset = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`

	rows, err := s.db.QueryContext(ctx, query, userId, asset, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction history: %v", err)
	}
	defer rows.Close()

	var transactions []Transaction
	for rows.Next() {
		var tx Transaction
		err := rows.Scan(&tx.Id, &tx.UserId, &tx.Asset, &tx.TransactionType,
			&tx.Amount, &tx.BalanceBefore, &tx.BalanceAfter,
			&tx.ExternalTransactionId, &tx.Address, &tx.Reference,
			&tx.Status, &tx.CreatedAt, &tx.ProcessedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %v", err)
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// GetMostRecentTransactionTime returns the most recent transaction timestamp for recovery
func (s *SubledgerService) GetMostRecentTransactionTime(ctx context.Context) (time.Time, error) {
	query := `
		SELECT MAX(created_at) 
		FROM transactions 
		WHERE external_transaction_id IS NOT NULL AND external_transaction_id != ''
	`
	var timestampStr sql.NullString
	err := s.db.QueryRowContext(ctx, query).Scan(&timestampStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get most recent transaction time: %v", err)
	}

	if !timestampStr.Valid || timestampStr.String == "" {
		// No transactions yet, start from 2 hours ago
		return time.Now().Add(-2 * time.Hour), nil
	}

	// Parse the timestamp string - SQLite stores it with space instead of T
	// First try SQLite's TIMESTAMP format: "2006-01-02 15:04:05.999999-07:00"
	parsedTime, err := time.Parse("2006-01-02 15:04:05.999999-07:00", timestampStr.String)
	if err != nil {
		// Try without microseconds: "2006-01-02 15:04:05-07:00"
		parsedTime, err = time.Parse("2006-01-02 15:04:05-07:00", timestampStr.String)
		if err != nil {
			// Try RFC3339 format as fallback
			parsedTime, err = time.Parse(time.RFC3339Nano, timestampStr.String)
			if err != nil {
				parsedTime, err = time.Parse(time.RFC3339, timestampStr.String)
				if err != nil {
					return time.Time{}, fmt.Errorf("failed to parse timestamp %q: %v", timestampStr.String, err)
				}
			}
		}
	}

	return parsedTime, nil
}
