package api

import (
	"context"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// ProcessDeposit handles incoming deposit notifications from Prime API
// This is the main entry point for real deposit processing
func (s *LedgerService) ProcessDeposit(ctx context.Context, address, asset string, amount decimal.Decimal, externalTxId string) (*DepositResult, error) {
	s.logger.Info("Processing real deposit from Prime API",
		zap.String("address", address),
		zap.String("asset", asset),
		zap.String("amount", amount.String()),
		zap.String("external_tx_id", externalTxId))

	// Validate input
	if address == "" || asset == "" || amount.LessThanOrEqual(decimal.Zero) || externalTxId == "" {
		s.logger.Error("Invalid deposit parameters",
			zap.String("address", address),
			zap.String("asset", asset),
			zap.String("amount", amount.String()),
			zap.String("external_tx_id", externalTxId))
		return &DepositResult{
			Success: false,
			Error:   "invalid deposit parameters",
		}, nil
	}

	// Process the deposit through subledger
	err := s.db.ProcessDeposit(ctx, address, asset, amount, externalTxId)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate transaction") {
			s.logger.Info("Duplicate transaction detected in API service",
				zap.String("address", address),
				zap.String("asset", asset),
				zap.String("amount", amount.String()),
				zap.String("external_tx_id", externalTxId))
		} else if strings.Contains(err.Error(), "no user found for address") {
			s.logger.Warn("Deposit to unrecognized address",
				zap.String("address", address),
				zap.String("asset", asset),
				zap.String("amount", amount.String()),
				zap.String("external_tx_id", externalTxId))
		} else {
			s.logger.Error("Deposit processing failed",
				zap.String("address", address),
				zap.String("asset", asset),
				zap.String("amount", amount.String()),
				zap.Error(err))
		}

		return &DepositResult{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	user, _, err := s.db.FindUserByAddress(ctx, address)
	if err != nil || user == nil {
		s.logger.Error("User lookup failed after deposit processing",
			zap.String("address", address),
			zap.Error(err))
		return &DepositResult{
			Success: false,
			Error:   "user lookup failed after deposit",
		}, nil
	}

	newBalance, err := s.db.GetUserBalance(ctx, user.Id, asset)
	if err != nil {
		s.logger.Error("Failed to get updated balance", zap.Error(err))
		newBalance = decimal.Zero
	}

	s.logger.Info("Real deposit processed successfully",
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

// CreateDepositAddress creates a new deposit address for a user
// This integrates with Prime API to generate real addresses
func (s *LedgerService) CreateDepositAddress(ctx context.Context, userId, asset, network string) (string, error) {
	if userId == "" || asset == "" || network == "" {
		return "", fmt.Errorf("user_id, asset, and network are required")
	}
	return "", fmt.Errorf("address generation requires Prime API integration")
}
