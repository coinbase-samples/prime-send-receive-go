package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"prime-send-receive-go/internal/models"
)

// GetBalance returns current balance for user/asset (O(1) lookup)
func (s *SubledgerService) GetBalance(ctx context.Context, userId, asset string) (decimal.Decimal, error) {
	zap.L().Debug("Getting balance", zap.String("user_id", userId), zap.String("asset_network", asset))

	var balanceStr string
	err := s.db.QueryRowContext(ctx, queryGetBalance, userId, asset).Scan(&balanceStr)
	if err == sql.ErrNoRows {
		// No balance record means zero balance
		return decimal.Zero, nil
	}
	if err != nil {
		zap.L().Error("Failed to get balance", zap.String("user_id", userId), zap.String("asset_network", asset), zap.Error(err))
		return decimal.Zero, fmt.Errorf("failed to get balance: %w", err)
	}

	balance, err := decimal.NewFromString(balanceStr)
	if err != nil {
		zap.L().Error("Failed to parse balance", zap.String("balance_str", balanceStr), zap.Error(err))
		return decimal.Zero, fmt.Errorf("failed to parse balance: %w", err)
	}

	zap.L().Debug("Retrieved balance", zap.String("user_id", userId), zap.String("asset_network", asset), zap.String("balance", balance.String()))
	return balance, nil
}

// GetAllBalances returns all non-zero balances for a user
func (s *SubledgerService) GetAllBalances(ctx context.Context, userId string) ([]models.AccountBalance, error) {
	zap.L().Debug("Getting all balances", zap.String("user_id", userId))

	rows, err := s.db.QueryContext(ctx, queryGetAllUserBalances, userId)
	if err != nil {
		zap.L().Error("Failed to get all balances", zap.String("user_id", userId), zap.Error(err))
		return nil, fmt.Errorf("failed to get all balances: %w", err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			zap.L().Warn("Failed to close rows", zap.Error(err))
		}
	}(rows)

	var balances []models.AccountBalance
	for rows.Next() {
		var balance models.AccountBalance
		var balanceStr string
		err := rows.Scan(&balance.Id, &balance.UserId, &balance.Asset, &balanceStr,
			&balance.LastTransactionId, &balance.Version, &balance.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan balance: %w", err)
		}

		balance.Balance, err = decimal.NewFromString(balanceStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse balance '%s': %w", balanceStr, err)
		}

		balances = append(balances, balance)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		zap.L().Error("Error during balance row iteration", zap.Error(err))
		return nil, fmt.Errorf("error iterating balance rows: %w", err)
	}

	zap.L().Debug("Retrieved all balances", zap.String("user_id", userId), zap.Int("count", len(balances)))
	return balances, nil
}

// ReconcileBalance verifies that current balance matches sum of all transactions
func (s *SubledgerService) ReconcileBalance(ctx context.Context, userId, asset string) error {
	zap.L().Info("Reconciling balance", zap.String("user_id", userId), zap.String("asset_network", asset))

	// Get current balance from account_balances table
	currentBalance, err := s.GetBalance(ctx, userId, asset)
	if err != nil {
		return fmt.Errorf("failed to get current balance: %w", err)
	}

	// Calculate balance from transaction history
	var calculatedBalanceStr string
	err = s.db.QueryRowContext(ctx, queryReconcileBalance, userId, asset).Scan(&calculatedBalanceStr)
	if err != nil {
		return fmt.Errorf("failed to calculate balance from transactions: %w", err)
	}

	calculatedBalance, err := decimal.NewFromString(calculatedBalanceStr)
	if err != nil {
		return fmt.Errorf("failed to parse calculated balance '%s': %w", calculatedBalanceStr, err)
	}

	// Check if balances match (exact decimal comparison)
	if !currentBalance.Equal(calculatedBalance) {
		zap.L().Error("Balance reconciliation failed",
			zap.String("user_id", userId),
			zap.String("asset_network", asset),
			zap.String("current_balance", currentBalance.String()),
			zap.String("calculated_balance", calculatedBalance.String()),
			zap.String("difference", currentBalance.Sub(calculatedBalance).String()))
		return fmt.Errorf("balance mismatch: current=%s, calculated=%s", currentBalance.String(), calculatedBalance.String())
	}

	zap.L().Info("Balance reconciliation successful",
		zap.String("user_id", userId),
		zap.String("asset_network", asset),
		zap.String("balance", currentBalance.String()))
	return nil
}
