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
	"errors"
	"fmt"
	"strings"

	"prime-send-receive-go/internal/database"
	"prime-send-receive-go/internal/models"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// ProcessDeposit handles incoming deposit notifications from Prime API
func (s *LedgerService) ProcessDeposit(ctx context.Context, address, asset string, amount decimal.Decimal, externalTxId string) (*models.DepositResult, error) {
	zap.L().Info("Processing deposit from Prime API",
		zap.String("address", address),
		zap.String("asset_network", asset),
		zap.String("amount", amount.String()),
		zap.String("external_tx_id", externalTxId))

	// Validate input
	if address == "" || asset == "" || amount.LessThanOrEqual(decimal.Zero) || externalTxId == "" {
		zap.L().Error("Invalid deposit parameters",
			zap.String("address", address),
			zap.String("asset_network", asset),
			zap.String("amount", amount.String()),
			zap.String("external_tx_id", externalTxId))
		return &models.DepositResult{
			Success: false,
			Error:   "invalid deposit parameters",
		}, nil
	}

	// Process the deposit through subledger
	err := s.db.ProcessDeposit(ctx, address, asset, amount, externalTxId)
	if err != nil {
		if errors.Is(err, database.ErrDuplicateTransaction) {
			zap.L().Info("Duplicate transaction detected in API service",
				zap.String("address", address),
				zap.String("asset_network", asset),
				zap.String("amount", amount.String()),
				zap.String("external_tx_id", externalTxId))
		} else if strings.Contains(err.Error(), "no user found for address") {
			zap.L().Warn("Deposit to unrecognized address",
				zap.String("address", address),
				zap.String("asset_network", asset),
				zap.String("amount", amount.String()),
				zap.String("external_tx_id", externalTxId))
		} else {
			zap.L().Error("Deposit processing failed",
				zap.String("address", address),
				zap.String("asset_network", asset),
				zap.String("amount", amount.String()),
				zap.Error(err))
		}

		return &models.DepositResult{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	user, _, err := s.db.FindUserByAddress(ctx, address)
	if err != nil || user == nil {
		zap.L().Error("User lookup failed after deposit processing",
			zap.String("address", address),
			zap.Error(err))
		return &models.DepositResult{
			Success: false,
			Error:   "user lookup failed after deposit",
		}, nil
	}

	newBalance, err := s.db.GetUserBalance(ctx, user.Id, asset)
	if err != nil {
		zap.L().Error("Failed to get updated balance", zap.Error(err))
		newBalance = decimal.Zero
	}

	zap.L().Info("Deposit processed successfully",
		zap.String("user_id", user.Id),
		zap.String("user_name", user.Name),
		zap.String("asset_network", asset),
		zap.String("amount", amount.String()),
		zap.String("new_balance", newBalance.String()))

	return &models.DepositResult{
		Success:    true,
		UserId:     user.Id,
		Asset:      asset,
		Amount:     amount,
		NewBalance: newBalance,
	}, nil
}

// CreateDepositAddress creates a new deposit address for a user
func (s *LedgerService) CreateDepositAddress(ctx context.Context, userId, asset, network string) (string, error) {
	if userId == "" || asset == "" || network == "" {
		return "", fmt.Errorf("user_id, asset, and network are required")
	}
	return "", fmt.Errorf("address generation requires Prime API integration")
}
