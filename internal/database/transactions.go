package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"prime-send-receive-go/internal/database/models"
)

// ProcessTransaction atomically updates balance and records transaction
func (s *SubledgerService) ProcessTransaction(ctx context.Context, userId, asset, transactionType string,
	amount decimal.Decimal, externalTxId, address, reference string) (*models.Transaction, error) {

	s.logger.Info("Processing transaction",
		zap.String("user_id", userId),
		zap.String("asset_network", asset),
		zap.String("type", transactionType),
		zap.String("amount", amount.String()),
		zap.String("external_tx_id", externalTxId))

	// Check for duplicate external transaction Id
	if externalTxId != "" {
		var existingTxId string
		err := s.db.QueryRowContext(ctx, queryCheckDuplicateTransaction, externalTxId).Scan(&existingTxId)
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
	var currentBalanceStr string
	var accountId string
	var version int64

	err = tx.QueryRowContext(ctx, queryGetAccountBalance, userId, asset).Scan(&accountId, &currentBalanceStr, &version)

	var currentBalance decimal.Decimal
	if err == sql.ErrNoRows {
		// Create new account balance record
		accountId = uuid.New().String()
		currentBalance = decimal.Zero
		version = 1

		_, err = tx.ExecContext(ctx, queryInsertAccountBalance, accountId, userId, asset, "0", 1)
		if err != nil {
			return nil, fmt.Errorf("failed to create account balance: %v", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to get current balance: %v", err)
	} else {
		currentBalance, err = decimal.NewFromString(currentBalanceStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse current balance '%s': %v", currentBalanceStr, err)
		}
	}

	// Calculate new balance
	newBalance := currentBalance.Add(amount)

	// Create transaction record
	transactionId := uuid.New().String()
	now := time.Now()
	transaction := &models.Transaction{}

	var amountStr, balanceBeforeStr, balanceAfterStr string
	err = tx.QueryRowContext(ctx, queryInsertTransaction,
		transactionId, userId, asset, transactionType,
		amount.String(), currentBalance.String(), newBalance.String(),
		externalTxId, address, reference, "confirmed", now, now).
		Scan(&transaction.Id, &transaction.UserId, &transaction.Asset, &transaction.TransactionType,
			&amountStr, &balanceBeforeStr, &balanceAfterStr,
			&transaction.ExternalTransactionId, &transaction.Address, &transaction.Reference,
			&transaction.Status, &transaction.CreatedAt, &transaction.ProcessedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to insert transaction: %v", err)
	}

	transaction.Amount, err = decimal.NewFromString(amountStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse returned amount: %v", err)
	}
	transaction.BalanceBefore, err = decimal.NewFromString(balanceBeforeStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse returned balance_before: %v", err)
	}
	transaction.BalanceAfter, err = decimal.NewFromString(balanceAfterStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse returned balance_after: %v", err)
	}

	// Update account balance (with optimistic locking)
	result, err := tx.ExecContext(ctx, queryUpdateAccountBalance, newBalance.String(), transactionId, userId, asset, version)
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
		zap.String("asset_network", asset),
		zap.String("old_balance", currentBalance.String()),
		zap.String("new_balance", newBalance.String()))

	return transaction, nil
}

// addJournalEntries creates double-entry bookkeeping entries
func (s *SubledgerService) addJournalEntries(ctx context.Context, tx *sql.Tx, transaction *models.Transaction) error {
	// For a deposit: Debit user asset account, Credit system liability account
	// For a withdrawal: Credit user asset account, Debit system liability account

	journalEntries := []struct {
		accountType  string
		accountId    string
		debitAmount  decimal.Decimal
		creditAmount decimal.Decimal
	}{}

	switch transaction.TransactionType {
	case "deposit":
		// User asset account increases (debit)
		journalEntries = append(journalEntries, struct {
			accountType  string
			accountId    string
			debitAmount  decimal.Decimal
			creditAmount decimal.Decimal
		}{"user_asset", fmt.Sprintf("%s_%s", transaction.UserId, transaction.Asset), transaction.Amount, decimal.Zero})

		// System liability increases (credit) - we owe the user this amount
		journalEntries = append(journalEntries, struct {
			accountType  string
			accountId    string
			debitAmount  decimal.Decimal
			creditAmount decimal.Decimal
		}{"system_liability", fmt.Sprintf("user_deposits_%s", transaction.Asset), decimal.Zero, transaction.Amount})

	case "withdrawal":
		// User asset account decreases (credit)
		journalEntries = append(journalEntries, struct {
			accountType  string
			accountId    string
			debitAmount  decimal.Decimal
			creditAmount decimal.Decimal
		}{"user_asset", fmt.Sprintf("%s_%s", transaction.UserId, transaction.Asset), decimal.Zero, transaction.Amount.Neg()})

		// System liability decreases (debit) - we no longer owe the user this amount
		journalEntries = append(journalEntries, struct {
			accountType  string
			accountId    string
			debitAmount  decimal.Decimal
			creditAmount decimal.Decimal
		}{"system_liability", fmt.Sprintf("user_deposits_%s", transaction.Asset), transaction.Amount.Neg(), decimal.Zero})
	}

	for _, entry := range journalEntries {
		entryId := uuid.New().String()
		_, err := tx.ExecContext(ctx, queryInsertJournalEntry,
			entryId, transaction.Id, entry.accountType, entry.accountId, entry.debitAmount.String(), entry.creditAmount.String())
		if err != nil {
			return err
		}
	}

	return nil
}

// GetTransactionHistory returns paginated transaction history for a user
func (s *SubledgerService) GetTransactionHistory(ctx context.Context, userId, asset string, limit, offset int) ([]models.Transaction, error) {
	s.logger.Debug("Getting transaction history",
		zap.String("user_id", userId),
		zap.String("asset_network", asset),
		zap.Int("limit", limit),
		zap.Int("offset", offset))

	rows, err := s.db.QueryContext(ctx, queryGetTransactionHistory, userId, asset, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction history: %v", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {

		}
	}(rows)

	var transactions []models.Transaction
	for rows.Next() {
		var tx models.Transaction
		var amountStr, balanceBeforeStr, balanceAfterStr string
		err := rows.Scan(&tx.Id, &tx.UserId, &tx.Asset, &tx.TransactionType,
			&amountStr, &balanceBeforeStr, &balanceAfterStr,
			&tx.ExternalTransactionId, &tx.Address, &tx.Reference,
			&tx.Status, &tx.CreatedAt, &tx.ProcessedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %v", err)
		}

		tx.Amount, err = decimal.NewFromString(amountStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse amount '%s': %v", amountStr, err)
		}

		tx.BalanceBefore, err = decimal.NewFromString(balanceBeforeStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse balance before '%s': %v", balanceBeforeStr, err)
		}

		tx.BalanceAfter, err = decimal.NewFromString(balanceAfterStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse balance after '%s': %v", balanceAfterStr, err)
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// GetMostRecentTransactionTime returns the most recent transaction timestamp for recovery
func (s *SubledgerService) GetMostRecentTransactionTime(ctx context.Context) (time.Time, error) {
	var timestampStr sql.NullString
	err := s.db.QueryRowContext(ctx, queryGetMostRecentTransactionTime).Scan(&timestampStr)
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
