package api

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

// GetUserBalance returns the current balance for a user and specific asset
func (s *LedgerService) GetUserBalance(ctx context.Context, userId, asset string) (float64, error) {
	if userId == "" || asset == "" {
		return 0, fmt.Errorf("user_id and asset are required")
	}

	balance, err := s.db.GetUserBalanceV2(ctx, userId, asset)
	if err != nil {
		s.logger.Error("Failed to get user balance",
			zap.String("user_id", userId),
			zap.String("asset", asset),
			zap.Error(err))
		return 0, fmt.Errorf("failed to retrieve balance")
	}

	return balance, nil
}

// GetUserBalances returns all non-zero balances for a user
func (s *LedgerService) GetUserBalances(ctx context.Context, userId string) ([]UserBalance, error) {
	if userId == "" {
		return nil, fmt.Errorf("user_id is required")
	}

	balances, err := s.db.GetAllUserBalancesV2(ctx, userId)
	if err != nil {
		s.logger.Error("Failed to get user balances", zap.String("user_id", userId), zap.Error(err))
		return nil, fmt.Errorf("failed to retrieve balances")
	}

	result := make([]UserBalance, len(balances))
	for i, balance := range balances {
		result[i] = UserBalance{
			Asset:   balance.Asset,
			Balance: balance.Balance,
		}
	}

	return result, nil
}

// GetTransactionHistory returns paginated transaction history for a user and asset
func (s *LedgerService) GetTransactionHistory(ctx context.Context, userId, asset string, limit, offset int) ([]TransactionRecord, error) {
	if userId == "" || asset == "" {
		return nil, fmt.Errorf("user_id and asset are required")
	}

	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	transactions, err := s.db.GetTransactionHistoryV2(ctx, userId, asset, limit, offset)
	if err != nil {
		s.logger.Error("Failed to get transaction history",
			zap.String("user_id", userId),
			zap.String("asset", asset),
			zap.Error(err))
		return nil, fmt.Errorf("failed to retrieve transaction history")
	}

	result := make([]TransactionRecord, len(transactions))
	for i, tx := range transactions {
		result[i] = TransactionRecord{
			Id:          tx.Id,
			Type:        tx.TransactionType,
			Asset:       tx.Asset,
			Amount:      tx.Amount,
			Address:     tx.Address,
			Status:      tx.Status,
			ProcessedAt: tx.ProcessedAt,
		}
	}

	return result, nil
}
