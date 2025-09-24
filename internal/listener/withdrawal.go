package listener

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// processWithdrawal processes a withdrawal transaction
func (d *SendReceiveListener) processWithdrawal(ctx context.Context, tx PrimeTransaction, wallet WalletInfo) error {
	if tx.Status != "TRANSACTION_DONE" {
		d.logger.Debug("Skipping non-completed withdrawal - waiting for completion",
			zap.String("transaction_id", tx.Id),
			zap.String("status", tx.Status),
			zap.String("symbol", tx.Symbol),
			zap.String("amount", tx.Amount),
			zap.Time("created_at", tx.CreatedAt))
		return nil
	}

	amount, err := strconv.ParseFloat(tx.Amount, 64)
	if err != nil {
		return fmt.Errorf("invalid amount: %v", err)
	}

	if amount < 0 {
		amount = -amount
	}

	if amount <= 0 {
		d.logger.Debug("Skipping zero amount withdrawal",
			zap.String("transaction_id", tx.Id),
			zap.Float64("amount", amount))
		return nil
	}

	// Find user by matching idempotency key prefix with user Id
	userId, err := d.findUserByIdempotencyKeyPrefix(ctx, tx.IdempotencyKey)
	if err != nil {
		d.logger.Debug("Could not match withdrawal to user via idempotency key",
			zap.String("transaction_id", tx.Id),
			zap.String("idempotency_key", tx.IdempotencyKey),
			zap.Error(err))
		return nil
	}

	d.logger.Info("Processing completed withdrawal",
		zap.String("transaction_id", tx.Id),
		zap.String("user_id", userId),
		zap.String("idempotency_key", tx.IdempotencyKey),
		zap.String("asset", wallet.Asset),
		zap.Float64("amount", amount),
		zap.Time("created_at", tx.CreatedAt),
		zap.Time("completed_at", tx.CompletedAt))

	result, err := d.apiService.ProcessWithdrawal(ctx, userId, wallet.Asset, amount, tx.Id)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate transaction") {
			d.logger.Info("Duplicate withdrawal detected - already processed, marking as handled",
				zap.String("transaction_id", tx.Id))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		return fmt.Errorf("failed to process withdrawal: %v", err)
	}

	if !result.Success {
		if strings.Contains(result.Error, "duplicate transaction") {
			d.logger.Info("Duplicate withdrawal detected - already processed, marking as handled",
				zap.String("transaction_id", tx.Id))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		d.logger.Warn("Withdrawal processing failed",
			zap.String("transaction_id", tx.Id),
			zap.String("error", result.Error))
		return fmt.Errorf("withdrawal processing failed: %s", result.Error)
	}

	d.markTransactionProcessed(tx.Id)

	d.logger.Info("Withdrawal processed successfully - balance debited",
		zap.String("transaction_id", tx.Id),
		zap.String("user_id", result.UserId),
		zap.String("asset", result.Asset),
		zap.Float64("amount", result.Amount),
		zap.Float64("new_balance", result.NewBalance),
		zap.Time("processed_at", time.Now()))

	return nil
}
