package database

import (
	"context"
	"database/sql"
	"fmt"

	"go.uber.org/zap"
)

// GetBalance returns current balance for user/asset (O(1) lookup)
func (s *SubledgerService) GetBalance(ctx context.Context, userId, asset string) (float64, error) {
	s.logger.Debug("Getting balance", zap.String("user_id", userId), zap.String("asset", asset))

	query := `
		SELECT balance 
		FROM account_balances 
		WHERE user_id = ? AND asset = ?
	`

	var balance float64
	err := s.db.QueryRowContext(ctx, query, userId, asset).Scan(&balance)
	if err == sql.ErrNoRows {
		// No balance record means zero balance
		return 0, nil
	}
	if err != nil {
		s.logger.Error("Failed to get balance", zap.String("user_id", userId), zap.String("asset", asset), zap.Error(err))
		return 0, fmt.Errorf("failed to get balance: %v", err)
	}

	s.logger.Debug("Retrieved balance", zap.String("user_id", userId), zap.String("asset", asset), zap.Float64("balance", balance))
	return balance, nil
}

// GetAllBalances returns all non-zero balances for a user
func (s *SubledgerService) GetAllBalances(ctx context.Context, userId string) ([]AccountBalance, error) {
	s.logger.Debug("Getting all balances", zap.String("user_id", userId))

	query := `
		SELECT id, user_id, asset, balance, last_transaction_id, version, updated_at
		FROM account_balances 
		WHERE user_id = ? AND balance != 0
		ORDER BY asset
	`

	rows, err := s.db.QueryContext(ctx, query, userId)
	if err != nil {
		s.logger.Error("Failed to get all balances", zap.String("user_id", userId), zap.Error(err))
		return nil, fmt.Errorf("failed to get all balances: %v", err)
	}
	defer rows.Close()

	var balances []AccountBalance
	for rows.Next() {
		var balance AccountBalance
		err := rows.Scan(&balance.Id, &balance.UserId, &balance.Asset, &balance.Balance,
			&balance.LastTransactionId, &balance.Version, &balance.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan balance: %v", err)
		}
		balances = append(balances, balance)
	}

	s.logger.Debug("Retrieved all balances", zap.String("user_id", userId), zap.Int("count", len(balances)))
	return balances, nil
}

// ReconcileBalance verifies that current balance matches sum of all transactions
func (s *SubledgerService) ReconcileBalance(ctx context.Context, userId, asset string) error {
	s.logger.Info("Reconciling balance", zap.String("user_id", userId), zap.String("asset", asset))

	// Get current balance from account_balances table
	currentBalance, err := s.GetBalance(ctx, userId, asset)
	if err != nil {
		return fmt.Errorf("failed to get current balance: %v", err)
	}

	// Calculate balance from transaction history
	query := `
		SELECT COALESCE(SUM(amount), 0) as calculated_balance
		FROM transactions 
		WHERE user_id = ? AND asset = ? AND status = 'confirmed'
	`
	var calculatedBalance float64
	err = s.db.QueryRowContext(ctx, query, userId, asset).Scan(&calculatedBalance)
	if err != nil {
		return fmt.Errorf("failed to calculate balance from transactions: %v", err)
	}

	// Check if balances match (with small tolerance for floating point precision)
	tolerance := 0.00000001
	if abs(currentBalance-calculatedBalance) > tolerance {
		s.logger.Error("Balance reconciliation failed",
			zap.String("user_id", userId),
			zap.String("asset", asset),
			zap.Float64("current_balance", currentBalance),
			zap.Float64("calculated_balance", calculatedBalance),
			zap.Float64("difference", currentBalance-calculatedBalance))
		return fmt.Errorf("balance mismatch: current=%.8f, calculated=%.8f", currentBalance, calculatedBalance)
	}

	s.logger.Info("Balance reconciliation successful",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.Float64("balance", currentBalance))
	return nil
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
