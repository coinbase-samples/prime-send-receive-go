package api

import (
	"context"
	"strings"

	"github.com/shopspring/decimal"
	"prime-send-receive-go/internal/database/models"

	"go.uber.org/zap"
)

func (s *LedgerService) ProcessWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, externalTxId string) (*DepositResult, error) {
	if userId == "" || asset == "" || amount.LessThanOrEqual(decimal.Zero) || externalTxId == "" {
		return &DepositResult{
			Success: false,
			Error:   "invalid withdrawal parameters",
		}, nil
	}

	s.logger.Info("Processing real withdrawal from Prime API",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.String("amount", amount.String()),
		zap.String("external_tx_id", externalTxId))

	err := s.db.ProcessWithdrawal(ctx, userId, asset, amount, externalTxId)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate transaction") {
			s.logger.Info("Duplicate withdrawal detected in API service",
				zap.String("user_id", userId),
				zap.String("asset", asset),
				zap.String("amount", amount.String()),
				zap.String("external_tx_id", externalTxId))
		} else {
			s.logger.Error("Withdrawal processing failed",
				zap.String("user_id", userId),
				zap.String("asset", asset),
				zap.String("amount", amount.String()),
				zap.Error(err))
		}

		return &DepositResult{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	users, err := s.db.GetUsers(ctx)
	if err != nil {
		s.logger.Error("User lookup failed after withdrawal processing",
			zap.String("user_id", userId),
			zap.Error(err))
		return &DepositResult{
			Success: false,
			Error:   "user lookup failed after withdrawal processing",
		}, nil
	}

	var user *models.User
	for _, u := range users {
		if u.Id == userId {
			user = &u
			break
		}
	}

	if user == nil {
		s.logger.Error("User not found after withdrawal processing",
			zap.String("user_id", userId))
		return &DepositResult{
			Success: false,
			Error:   "user not found after withdrawal processing",
		}, nil
	}

	newBalance, err := s.db.GetUserBalance(ctx, userId, asset)
	if err != nil {
		s.logger.Error("Balance lookup failed after withdrawal processing",
			zap.String("user_id", userId),
			zap.String("asset", asset),
			zap.Error(err))
		return &DepositResult{
			Success: false,
			Error:   "balance lookup failed after withdrawal processing",
		}, nil
	}

	s.logger.Info("Real withdrawal processed successfully",
		zap.String("user_id", user.Id),
		zap.String("user_name", user.Name),
		zap.String("asset", asset),
		zap.String("amount", amount.String()),
		zap.String("new_balance", newBalance.String()))

	return &DepositResult{
		Success:    true,
		UserId:     user.Id,
		Asset:      asset,
		Amount:     amount,
		NewBalance: newBalance,
	}, nil
}
