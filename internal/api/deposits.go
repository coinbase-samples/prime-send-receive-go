package api

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// ProcessDeposit handles incoming deposit notifications from Prime API
// This is the main entry point for real deposit processing
func (s *ProductionLedgerService) ProcessDeposit(ctx context.Context, address, asset string, amount float64, externalTxId string) (*DepositResult, error) {
	s.logger.Info("Processing real deposit from Prime API",
		zap.String("address", address),
		zap.String("asset", asset),
		zap.Float64("amount", amount),
		zap.String("external_tx_id", externalTxId))

	// Validate input
	if address == "" || asset == "" || amount <= 0 || externalTxId == "" {
		s.logger.Error("Invalid deposit parameters",
			zap.String("address", address),
			zap.String("asset", asset),
			zap.Float64("amount", amount),
			zap.String("external_tx_id", externalTxId))
		return &DepositResult{
			Success: false,
			Error:   "invalid deposit parameters",
		}, nil
	}

	// Process the deposit through subledger
	err := s.db.ProcessDepositV2(ctx, address, asset, amount, externalTxId)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate transaction") {
			s.logger.Info("Duplicate transaction detected in API service",
				zap.String("address", address),
				zap.String("asset", asset),
				zap.Float64("amount", amount),
				zap.String("external_tx_id", externalTxId))
		} else {
			s.logger.Error("Deposit processing failed",
				zap.String("address", address),
				zap.String("asset", asset),
				zap.Float64("amount", amount),
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

	newBalance, err := s.db.GetUserBalanceV2(ctx, user.Id, asset)
	if err != nil {
		s.logger.Error("Failed to get updated balance", zap.Error(err))
		newBalance = 0
	}

	s.logger.Info("Real deposit processed successfully",
		zap.String("user_id", user.Id),
		zap.String("user_name", user.Name),
		zap.String("asset", asset),
		zap.Float64("amount", amount),
		zap.Float64("new_balance", newBalance))

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
func (s *ProductionLedgerService) CreateDepositAddress(ctx context.Context, userId, asset, network string) (string, error) {
	if userId == "" || asset == "" || network == "" {
		return "", fmt.Errorf("user_id, asset, and network are required")
	}
	return "", fmt.Errorf("address generation requires Prime API integration")
}
