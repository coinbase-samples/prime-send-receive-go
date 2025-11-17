package listener

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"prime-send-receive-go/internal/database"
	"prime-send-receive-go/internal/models"
)

// processWithdrawal processes a withdrawal transaction
func (d *SendReceiveListener) processWithdrawal(ctx context.Context, tx models.PrimeTransaction, wallet models.WalletInfo) error {
	// Terminal failure statuses that require balance credit-back
	terminalFailures := map[string]bool{
		"TRANSACTION_CANCELLED": true,
		"TRANSACTION_REJECTED":  true,
		"TRANSACTION_FAILED":    true,
		"TRANSACTION_EXPIRED":   true,
	}

	// Check if this is a terminal failure status
	if terminalFailures[tx.Status] {
		zap.L().Warn("Withdrawal failed with terminal status - crediting back",
			zap.String("transaction_id", tx.Id),
			zap.String("status", tx.Status),
			zap.String("symbol", tx.Symbol),
			zap.String("amount", tx.Amount),
			zap.Time("created_at", tx.CreatedAt))
		return d.handleFailedWithdrawal(ctx, tx, wallet)
	}

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
		return fmt.Errorf("invalid amount: %w", err)
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
		zap.L().Debug("Could not match withdrawal to user via idempotency key - skipping",
			zap.String("transaction_id", tx.Id),
			zap.String("idempotency_key", tx.IdempotencyKey),
			zap.Error(err))
		return nil
	}

	// Normalize symbol: Prime API returns network-specific symbols like "BASEUSDC" or "USDC"
	// We need canonical symbol "USDC" for consistent balance tracking across networks
	canonicalSymbol := normalizeSymbol(tx.Symbol)

	assetNetwork := fmt.Sprintf("%s-%s", tx.Symbol, tx.Network)
	assetNetwork = strings.TrimSuffix(assetNetwork, "-")

	zap.L().Info("Processing completed withdrawal",
		zap.String("transaction_id", tx.Id),
		zap.String("user_id", userId),
		zap.String("idempotency_key", tx.IdempotencyKey),
		zap.String("prime_api_symbol", tx.Symbol),
		zap.String("canonical_symbol", canonicalSymbol),
		zap.String("network", tx.Network),
		zap.String("asset_network", assetNetwork),
		zap.String("amount", amount.String()),
		zap.Time("created_at", tx.CreatedAt),
		zap.Time("completed_at", tx.CompletedAt))

	// Check if this withdrawal was already processed by the withdrawal CLI
	// The CLI uses idempotency key as the transaction ID when debiting
	// First try with idempotency key to see if it already exists
	result, err := d.apiService.ProcessWithdrawal(ctx, userId, canonicalSymbol, amount, tx.IdempotencyKey)
	if err != nil {
		if errors.Is(err, database.ErrDuplicateTransaction) {
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		zap.L().Debug("Idempotency key not found, trying with Prime transaction ID",
			zap.String("idempotency_key", tx.IdempotencyKey),
			zap.String("prime_tx_id", tx.Id))

		result, err = d.apiService.ProcessWithdrawal(ctx, userId, canonicalSymbol, amount, tx.Id)
		if err != nil {
			if errors.Is(err, database.ErrDuplicateTransaction) {
				d.markTransactionProcessed(tx.Id)
				return nil
			}
			return fmt.Errorf("failed to process withdrawal: %w", err)
		}
	}

	if !result.Success {
		if strings.Contains(result.Error, "duplicate transaction") {
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

// handleFailedWithdrawal credits back a withdrawal that failed on-chain
func (d *SendReceiveListener) handleFailedWithdrawal(ctx context.Context, tx models.PrimeTransaction, wallet models.WalletInfo) error {
	amount, err := decimal.NewFromString(tx.Amount)
	if err != nil {
		return fmt.Errorf("invalid amount: %w", err)
	}

	if amount.LessThan(decimal.Zero) {
		amount = amount.Neg()
	}

	if amount.LessThanOrEqual(decimal.Zero) {
		zap.L().Debug("Skipping zero amount failed withdrawal",
			zap.String("transaction_id", tx.Id),
			zap.String("amount", amount.String()))
		return nil
	}

	userId, err := d.findUserByIdempotencyKeyPrefix(ctx, tx.IdempotencyKey)
	if err != nil {
		zap.L().Warn("Could not match failed withdrawal to user via idempotency key - may be external withdrawal",
			zap.String("transaction_id", tx.Id),
			zap.String("idempotency_key", tx.IdempotencyKey),
			zap.String("status", tx.Status),
			zap.Error(err))
		d.markTransactionProcessed(tx.Id)
		return nil
	}

	// Normalize symbol: Prime API returns network-specific symbols like "BASEUSDC" or "USDC"
	// We need canonical symbol "USDC" for consistent balance tracking across networks
	canonicalSymbol := normalizeSymbol(tx.Symbol)

	zap.L().Info("Processing failed withdrawal - crediting back to user",
		zap.String("transaction_id", tx.Id),
		zap.String("user_id", userId),
		zap.String("idempotency_key", tx.IdempotencyKey),
		zap.String("status", tx.Status),
		zap.String("prime_api_symbol", tx.Symbol),
		zap.String("canonical_symbol", canonicalSymbol),
		zap.String("amount", amount.String()),
		zap.Time("created_at", tx.CreatedAt))

	// Credit back the amount (deposit to reverse the failed withdrawal)
	// Use idempotency key as original transaction ID for tracking
	result, err := d.apiService.CreditBackFailedWithdrawal(ctx, userId, canonicalSymbol, amount, tx.IdempotencyKey)
	if err != nil {
		return fmt.Errorf("failed to credit back failed withdrawal: %w", err)
	}

	if !result.Success {
		if strings.Contains(result.Error, "duplicate transaction") {
			zap.L().Info("Failed withdrawal reversal already processed - skipping",
				zap.String("transaction_id", tx.Id))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		zap.L().Error("Failed withdrawal credit-back processing failed",
			zap.String("transaction_id", tx.Id),
			zap.String("error", result.Error))
		return fmt.Errorf("failed withdrawal credit-back failed: %s", result.Error)
	}

	d.markTransactionProcessed(tx.Id)

	zap.L().Info("Failed withdrawal credited back successfully",
		zap.String("transaction_id", tx.Id),
		zap.String("user_id", result.UserId),
		zap.String("asset", result.Asset),
		zap.String("amount", result.Amount.String()),
		zap.String("new_balance", result.NewBalance.String()),
		zap.String("status", tx.Status),
		zap.Time("processed_at", time.Now()))

	return nil
}

// symbolMapping maps Prime API's network-specific symbols to canonical symbols
var symbolMapping = map[string]string{
	// USDC variants (canonical + network-specific)
	"USDC":     "USDC",
	"SPLUSDC":  "USDC",
	"AVAUSDC":  "USDC",
	"ARBUSDC":  "USDC",
	"BASEUSDC": "USDC",

	// ETH variants
	"ETH":     "ETH",
	"BASEETH": "ETH",
}

func normalizeSymbol(symbol string) string {
	if canonical, ok := symbolMapping[symbol]; ok {
		return canonical
	}
	return symbol
}
