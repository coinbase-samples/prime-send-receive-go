package listener

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"prime-send-receive-go/internal/models"
)

// processWithdrawal processes a withdrawal transaction
func (d *SendReceiveListener) processWithdrawal(ctx context.Context, tx models.PrimeTransaction, wallet models.WalletInfo) error {
	if tx.Status != "TRANSACTION_DONE" {
		zap.L().Debug("Skipping non-completed withdrawal - waiting for completion",
			zap.String("transaction_id", tx.Id),
			zap.String("status", tx.Status),
			zap.String("symbol", tx.Symbol),
			zap.String("amount", tx.Amount),
			zap.Time("created_at", tx.CreatedAt))
		return nil
	}

	amount, err := decimal.NewFromString(tx.Amount)
	if err != nil {
		return fmt.Errorf("invalid amount: %v", err)
	}

	if amount.LessThan(decimal.Zero) {
		amount = amount.Neg()
	}

	if amount.LessThanOrEqual(decimal.Zero) {
		zap.L().Debug("Skipping zero amount withdrawal",
			zap.String("transaction_id", tx.Id),
			zap.String("amount", amount.String()))
		return nil
	}

	// Find user by matching idempotency key prefix with user Id
	userId, err := d.findUserByIdempotencyKeyPrefix(ctx, tx.IdempotencyKey)
	if err != nil {
		zap.L().Debug("Could not match withdrawal to user via idempotency key",
			zap.String("transaction_id", tx.Id),
			zap.String("idempotency_key", tx.IdempotencyKey),
			zap.Error(err))
		return nil
	}

	assetNetwork := fmt.Sprintf("%s-%s", tx.Symbol, tx.Network)
	assetNetwork = strings.TrimSuffix(assetNetwork, "-")

	zap.L().Info("Processing completed withdrawal",
		zap.String("transaction_id", tx.Id),
		zap.String("user_id", userId),
		zap.String("idempotency_key", tx.IdempotencyKey),
		zap.String("asset_symbol", tx.Symbol),
		zap.String("network", tx.Network),
		zap.String("asset_network", assetNetwork),
		zap.String("amount", amount.String()),
		zap.Time("created_at", tx.CreatedAt),
		zap.Time("completed_at", tx.CompletedAt))

	// Pass symbol only to ledger - balances are tracked per symbol, not per network
	result, err := d.apiService.ProcessWithdrawal(ctx, userId, tx.Symbol, amount, tx.Id)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate transaction") {
			zap.L().Info("Duplicate withdrawal detected - already processed, marking as handled",
				zap.String("transaction_id", tx.Id))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		return fmt.Errorf("failed to process withdrawal: %v", err)
	}

	if !result.Success {
		if strings.Contains(result.Error, "duplicate transaction") {
			zap.L().Info("Duplicate withdrawal detected - already processed, marking as handled",
				zap.String("transaction_id", tx.Id))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		zap.L().Warn("Withdrawal processing failed",
			zap.String("transaction_id", tx.Id),
			zap.String("error", result.Error))
		return fmt.Errorf("withdrawal processing failed: %s", result.Error)
	}

	d.markTransactionProcessed(tx.Id)

	zap.L().Info("Withdrawal processed successfully - balance debited",
		zap.String("transaction_id", tx.Id),
		zap.String("user_id", result.UserId),
		zap.String("asset", result.Asset),
		zap.String("amount", result.Amount.String()),
		zap.String("new_balance", result.NewBalance.String()),
		zap.Time("processed_at", time.Now()))

	return nil
}
