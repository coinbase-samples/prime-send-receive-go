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

package api

import (
	"context"
	"fmt"

	"prime-send-receive-go/internal/models"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// GetUserBalance returns the current balance for a user and specific asset
func (s *LedgerService) GetUserBalance(ctx context.Context, userId, asset string) (decimal.Decimal, error) {
	if userId == "" || asset == "" {
		return decimal.Zero, fmt.Errorf("user_id and asset are required")
	}

	balance, err := s.db.GetUserBalance(ctx, userId, asset)
	if err != nil {
		zap.L().Error("Failed to get user balance",
			zap.String("user_id", userId),
			zap.String("asset_network", asset),
			zap.Error(err))
		return decimal.Zero, fmt.Errorf("failed to retrieve balance")
	}

	return balance, nil
}

// GetUserBalances returns all non-zero balances for a user
func (s *LedgerService) GetUserBalances(ctx context.Context, userId string) ([]models.UserBalance, error) {
	if userId == "" {
		return nil, fmt.Errorf("user_id is required")
	}

	balances, err := s.db.GetAllUserBalances(ctx, userId)
	if err != nil {
		zap.L().Error("Failed to get user balances", zap.String("user_id", userId), zap.Error(err))
		return nil, fmt.Errorf("failed to retrieve balances")
	}

	result := make([]models.UserBalance, len(balances))
	for i, balance := range balances {
		result[i] = models.UserBalance{
			Asset:   balance.Asset,
			Balance: balance.Balance,
		}
	}

	return result, nil
}

// GetTransactionHistory returns paginated transaction history for a user and asset
func (s *LedgerService) GetTransactionHistory(ctx context.Context, userId, asset string, limit, offset int) ([]models.TransactionRecord, error) {
	if userId == "" || asset == "" {
		return nil, fmt.Errorf("user_id and asset are required")
	}

	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	transactions, err := s.db.GetTransactionHistory(ctx, userId, asset, limit, offset)
	if err != nil {
		zap.L().Error("Failed to get transaction history",
			zap.String("user_id", userId),
			zap.String("asset_network", asset),
			zap.Error(err))
		return nil, fmt.Errorf("failed to retrieve transaction history")
	}

	result := make([]models.TransactionRecord, len(transactions))
	for i, tx := range transactions {
		result[i] = models.TransactionRecord{
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
