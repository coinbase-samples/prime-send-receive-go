package api

import (
	"context"
	"errors"

	"github.com/shopspring/decimal"
	"prime-send-receive-go/internal/database"
	"prime-send-receive-go/internal/models"

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
		if errors.Is(err, database.ErrDuplicateTransaction) {
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
		if errors.Is(err, database.ErrDuplicateTransaction) {
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
