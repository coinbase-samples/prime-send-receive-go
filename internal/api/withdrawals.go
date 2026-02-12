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

	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/store"

	"github.com/shopspring/decimal"

	"go.uber.org/zap"
)

func (s *LedgerService) ProcessWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, externalTxId string) (*models.DepositResult, error) {
	if userId == "" || asset == "" || amount.LessThanOrEqual(decimal.Zero) || externalTxId == "" {
		return &models.DepositResult{
			Success: false,
			Error:   "invalid withdrawal parameters",
		}, nil
	}

	zap.L().Info("Processing withdrawal from Prime API",
		zap.String("user_id", userId),
		zap.String("asset_network", asset),
		zap.String("amount", amount.String()),
		zap.String("external_tx_id", externalTxId))

	err := s.db.ProcessWithdrawal(ctx, userId, asset, amount, externalTxId)
	if err != nil {
		if errors.Is(err, store.ErrDuplicateTransaction) {
			zap.L().Info("Duplicate withdrawal detected in API service",
				zap.String("user_id", userId),
				zap.String("asset_network", asset),
				zap.String("amount", amount.String()),
				zap.String("external_tx_id", externalTxId))
		} else {
			zap.L().Error("Withdrawal processing failed",
				zap.String("user_id", userId),
				zap.String("asset_network", asset),
				zap.String("amount", amount.String()),
				zap.Error(err))
		}

		return &models.DepositResult{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	user, err := s.db.GetUserById(ctx, userId)
	if err != nil {
		zap.L().Error("User lookup failed after withdrawal processing",
			zap.String("user_id", userId),
			zap.Error(err))
		return &models.DepositResult{
			Success: false,
			Error:   "user lookup failed after withdrawal processing",
		}, nil
	}

	newBalance, err := s.db.GetUserBalance(ctx, userId, asset)
	if err != nil {
		zap.L().Error("Balance lookup failed after withdrawal processing",
			zap.String("user_id", userId),
			zap.String("asset_network", asset),
			zap.Error(err))
		return &models.DepositResult{
			Success: false,
			Error:   "balance lookup failed after withdrawal processing",
		}, nil
	}

	zap.L().Info("Withdrawal processed successfully",
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

// ConfirmWithdrawal settles a completed withdrawal (TRANSACTION_DONE).
// For Formance this moves funds from pending to the portfolio wallet.
// For SQLite this is a no-op (balance was already debited by ProcessWithdrawal).
func (s *LedgerService) ConfirmWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, withdrawalRef, externalTxId string) error {
	zap.L().Info("Confirming withdrawal settlement",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.String("amount", amount.String()),
		zap.String("external_tx_id", externalTxId))

	err := s.db.ConfirmWithdrawal(ctx, userId, asset, amount, withdrawalRef, externalTxId)
	if err != nil {
		if errors.Is(err, store.ErrDuplicateTransaction) {
			zap.L().Info("Withdrawal confirmation already processed",
				zap.String("external_tx_id", externalTxId))
			return nil
		}
		zap.L().Error("Withdrawal confirmation failed",
			zap.String("user_id", userId),
			zap.String("external_tx_id", externalTxId),
			zap.Error(err))
		return err
	}
	return nil
}

// CreditBackFailedWithdrawal credits back a withdrawal that failed (e.g., TRANSACTION_FAILED, TRANSACTION_CANCELLED)
func (s *LedgerService) CreditBackFailedWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, originalTxId string) (*models.DepositResult, error) {
	if userId == "" || asset == "" || amount.LessThanOrEqual(decimal.Zero) || originalTxId == "" {
		return &models.DepositResult{
			Success: false,
			Error:   "invalid credit-back parameters",
		}, nil
	}

	zap.L().Info("Crediting back failed withdrawal",
		zap.String("user_id", userId),
		zap.String("asset_network", asset),
		zap.String("amount", amount.String()),
		zap.String("original_tx_id", originalTxId))

	err := s.db.ReverseWithdrawal(ctx, userId, asset, amount, originalTxId)
	if err != nil {
		if errors.Is(err, store.ErrDuplicateTransaction) {
			zap.L().Info("Duplicate credit-back detected in API service",
				zap.String("user_id", userId),
				zap.String("asset_network", asset),
				zap.String("amount", amount.String()),
				zap.String("original_tx_id", originalTxId))
		} else {
			zap.L().Error("Credit-back processing failed",
				zap.String("user_id", userId),
				zap.String("asset_network", asset),
				zap.String("amount", amount.String()),
				zap.Error(err))
		}

		return &models.DepositResult{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	newBalance, err := s.db.GetUserBalance(ctx, userId, asset)
	if err != nil {
		zap.L().Error("Balance lookup failed after credit-back",
			zap.String("user_id", userId),
			zap.String("asset_network", asset),
			zap.Error(err))
		return &models.DepositResult{
			Success: false,
			Error:   "balance lookup failed after credit-back",
		}, nil
	}

	zap.L().Info("Failed withdrawal credited back successfully",
		zap.String("user_id", userId),
		zap.String("asset_network", asset),
		zap.String("amount", amount.String()),
		zap.String("new_balance", newBalance.String()))

	return &models.DepositResult{
		Success:    true,
		UserId:     userId,
		Asset:      asset,
		Amount:     amount,
		NewBalance: newBalance,
	}, nil
}
