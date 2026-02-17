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

package listener

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/store"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
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

	// OTHER_TRANSACTION_STATUS: treat as a pending withdrawal.
	// Try to match to a user via idempotency key; if not, use the platform account.
	// Funds move to the withdrawal pending account (same as WITHDRAWAL_INITIATED).
	if tx.Status == "OTHER_TRANSACTION_STATUS" {
		return d.handlePendingWithdrawal(ctx, tx, wallet)
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
	if amount.IsZero() {
		d.markTransactionProcessed(tx.Id)
		return nil
	}

	canonicalSymbol := normalizeSymbol(tx.Symbol)

	// Resolve destination address.
	destAddr := tx.TransferTo.Address
	if destAddr == "" {
		destAddr = tx.TransferTo.Value
	}
	if destAddr == "" {
		destAddr = tx.TransferTo.AccountIdentifier
	}

	// Try to match user: 1) destination address, 2) idempotency key.
	var userId string
	if destAddr != "" {
		user, _, findErr := d.dbService.FindUserByAddress(ctx, destAddr)
		if findErr == nil && user != nil {
			userId = user.Id
		}
	}
	if userId == "" {
		matched, matchErr := d.findUserByIdempotencyKeyPrefix(ctx, tx.IdempotencyKey)
		if matchErr == nil {
			userId = matched
		}
	}
	if userId == "" {
		userId = "prime-platform-" + d.portfolioId
	}

	zap.L().Info("Processing completed withdrawal (TRANSACTION_DONE)",
		zap.String("transaction_id", tx.Id),
		zap.String("user_id", userId),
		zap.String("idempotency_key", tx.IdempotencyKey),
		zap.String("symbol", canonicalSymbol),
		zap.String("amount", amount.String()),
		zap.String("destination", destAddr),
		zap.String("network", tx.Network),
		zap.String("prime_tx_id", tx.TransactionId),
		zap.Time("completed_at", tx.CompletedAt))

	txTime := tx.CompletedAt
	if txTime.IsZero() {
		txTime = tx.CreatedAt
	}

	// Check if a WITHDRAWAL_INITIATED exists for this withdrawal reference.
	// If yes -> confirm from pending (3-phase). If no -> direct debit from user.
	hasPending, _ := d.dbService.HasPendingWithdrawal(ctx, tx.IdempotencyKey)
	if !hasPending {
		// Also check by Prime transaction ID.
		hasPending, _ = d.dbService.HasPendingWithdrawal(ctx, tx.Id)
	}

	if hasPending {
		// Normal 3-phase: pending -> wallet.
		zap.L().Info("Found pending transaction, confirming from pending",
			zap.String("transaction_id", tx.Id))

		err = d.apiService.ConfirmWithdrawal(ctx, userId, canonicalSymbol, amount, tx.IdempotencyKey, tx.Id)
		if err != nil && !errors.Is(err, store.ErrDuplicateTransaction) {
			return fmt.Errorf("failed to confirm withdrawal from pending: %w", err)
		}
	} else {
		// No pending -- direct debit from user to wallet (with overdraft).
		zap.L().Info("No pending transaction found, debiting user directly",
			zap.String("transaction_id", tx.Id),
			zap.String("user_id", userId))

		dErr := d.dbService.ConfirmWithdrawalDirect(ctx, store.WithdrawalConfirmDirectParams{
			UserId:             userId,
			Asset:              canonicalSymbol,
			Amount:             amount,
			WalletId:           wallet.Id,
			ExternalTxId:       tx.Id,
			WithdrawalRef:      tx.IdempotencyKey,
			DestinationAddress: destAddr,
			Network:            tx.Network,
			PrimeTxId:          tx.TransactionId,
			IdempotencyKey:     tx.IdempotencyKey,
			TransactionTime:    txTime,
		})
		if dErr != nil {
			return fmt.Errorf("failed to record confirmed withdrawal: %w", dErr)
		}
	}

	d.markTransactionProcessed(tx.Id)

	zap.L().Info("Withdrawal confirmed successfully",
		zap.String("transaction_id", tx.Id),
		zap.String("user_id", userId),
		zap.String("asset", canonicalSymbol),
		zap.String("amount", amount.String()),
		zap.String("destination", destAddr),
		zap.Time("processed_at", time.Now()))

	return nil
}

// handlePendingWithdrawal processes a withdrawal with OTHER_TRANSACTION_STATUS
// as a pending withdrawal. Tries to match to a user via idempotency key;
// falls back to the platform account. In both cases, funds move to the
// portfolio's withdrawal pending account via ProcessWithdrawal.
func (d *SendReceiveListener) handlePendingWithdrawal(ctx context.Context, tx models.PrimeTransaction, wallet models.WalletInfo) error {
	amount, err := decimal.NewFromString(tx.Amount)
	if err != nil {
		return fmt.Errorf("invalid amount: %w", err)
	}
	if amount.LessThan(decimal.Zero) {
		amount = amount.Neg()
	}
	if amount.IsZero() {
		d.markTransactionProcessed(tx.Id)
		return nil
	}

	canonicalSymbol := normalizeSymbol(tx.Symbol)

	// Resolve destination address.
	destAddr := tx.TransferTo.Address
	if destAddr == "" {
		destAddr = tx.TransferTo.Value
	}
	if destAddr == "" {
		destAddr = tx.TransferTo.AccountIdentifier
	}

	txTime := tx.CompletedAt
	if txTime.IsZero() {
		txTime = tx.CreatedAt
	}

	// Try to find the user in three ways:
	// 1. Look up by destination address (most reliable for withdrawals)
	// 2. Match by idempotency key prefix
	// 3. Fall back to wallet -> pending (platform level)

	var userId string

	// 1. Check if the destination address belongs to a known user.
	if destAddr != "" {
		user, _, findErr := d.dbService.FindUserByAddress(ctx, destAddr)
		if findErr == nil && user != nil {
			userId = user.Id
			zap.L().Info("Pending withdrawal -- matched user by destination address",
				zap.String("transaction_id", tx.Id),
				zap.String("user_id", userId),
				zap.String("destination", destAddr))
		}
	}

	// 2. Try idempotency key prefix match.
	if userId == "" {
		matched, matchErr := d.findUserByIdempotencyKeyPrefix(ctx, tx.IdempotencyKey)
		if matchErr == nil {
			userId = matched
			zap.L().Info("Pending withdrawal -- matched user by idempotency key",
				zap.String("transaction_id", tx.Id),
				zap.String("user_id", userId))
		}
	}

	// If user found, debit their account -> pending.
	if userId != "" {
		zap.L().Info("Pending withdrawal (OTHER_TRANSACTION_STATUS) -- debiting user",
			zap.String("transaction_id", tx.Id),
			zap.String("user_id", userId),
			zap.String("symbol", canonicalSymbol),
			zap.String("amount", amount.String()),
			zap.String("destination", destAddr))

		err = d.dbService.ProcessWithdrawal(ctx, userId, canonicalSymbol, amount, tx.Id)
		if err != nil {
			if errors.Is(err, store.ErrDuplicateTransaction) {
				d.markTransactionProcessed(tx.Id)
				return nil
			}
			// If user doesn't have funds (e.g. deposit not yet processed), fall through to wallet.
			zap.L().Warn("User withdrawal failed (insufficient funds?), falling through to wallet debit",
				zap.String("user_id", userId), zap.Error(err))
		} else {
			d.markTransactionProcessed(tx.Id)
			return nil
		}
	}

	// 3. Fall back: debit from Prime wallet (with overdraft) -> pending.
	zap.L().Info("Pending withdrawal (OTHER_TRANSACTION_STATUS) -- from wallet to pending",
		zap.String("transaction_id", tx.Id),
		zap.String("symbol", canonicalSymbol),
		zap.String("amount", amount.String()),
		zap.String("wallet_id", wallet.Id),
		zap.String("destination", destAddr))

	err = d.dbService.ProcessWithdrawalFromWallet(ctx, store.WithdrawalFromWalletParams{
		TransactionId:      tx.Id,
		Status:             tx.Status,
		Symbol:             canonicalSymbol,
		PrimeApiSymbol:     tx.Symbol,
		Amount:             amount,
		WalletId:           wallet.Id,
		DestinationAddress: destAddr,
		IdempotencyKey:     tx.IdempotencyKey,
		TransactionTime:    txTime,
	})
	if err != nil {
		return fmt.Errorf("failed to process pending withdrawal from wallet: %w", err)
	}
	d.markTransactionProcessed(tx.Id)
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
		zap.L().Warn("Could not match failed withdrawal to user - recording as platform-level failed withdrawal",
			zap.String("transaction_id", tx.Id),
			zap.String("idempotency_key", tx.IdempotencyKey),
			zap.String("status", tx.Status),
			zap.Error(err))

		// Synthetically record initiation + reversal against the platform wallet so
		// the ledger has a complete audit trail (net balance = 0).
		destAddr := tx.TransferTo.Address
		if destAddr == "" {
			destAddr = tx.TransferTo.Value
		}

		if pErr := d.dbService.RecordFailedWithdrawalPlatform(ctx, store.FailedWithdrawalPlatformParams{
			TransactionId:      tx.Id,
			Status:             tx.Status,
			Symbol:             normalizeSymbol(tx.Symbol),
			PrimeApiSymbol:     tx.Symbol,
			Amount:             amount,
			WalletId:           wallet.Id,
			DestinationAddress: destAddr,
			IdempotencyKey:     tx.IdempotencyKey,
			TransactionTime:    tx.CreatedAt,
		}); pErr != nil {
			zap.L().Error("Failed to record platform-level failed withdrawal",
				zap.String("transaction_id", tx.Id),
				zap.Error(pErr))
			return fmt.Errorf("failed to record platform-level failed withdrawal: %w", pErr)
		}

		d.markTransactionProcessed(tx.Id)
		zap.L().Info("Platform-level failed withdrawal recorded (initiation + reversal)",
			zap.String("transaction_id", tx.Id),
			zap.String("status", tx.Status),
			zap.String("asset", normalizeSymbol(tx.Symbol)),
			zap.String("amount", amount.String()),
			zap.String("destination", destAddr))
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

	// Try native revert first (Formance) -- if the CLI already reverted this
	// transaction, the revert call will detect it's already reverted and skip cleanly.
	// If no transaction is found, it also means nothing was reserved, so nothing to undo.
	revertErr := d.dbService.RevertTransaction(ctx, tx.IdempotencyKey)
	if revertErr == nil {
		zap.L().Info("Failed withdrawal reverted via native RevertTransaction",
			zap.String("transaction_id", tx.Id),
			zap.String("idempotency_key", tx.IdempotencyKey))
		d.markTransactionProcessed(tx.Id)

		zap.L().Info("Failed withdrawal credited back successfully",
			zap.String("transaction_id", tx.Id),
			zap.String("user_id", userId),
			zap.String("asset", canonicalSymbol),
			zap.String("amount", amount.String()),
			zap.String("status", tx.Status),
			zap.Time("processed_at", time.Now()))
		return nil
	}

	// For Formance: if revert failed because the transaction wasn't found or was
	// already reverted, there's nothing to compensate -- just mark as processed.
	if strings.Contains(revertErr.Error(), "no transaction found") {
		zap.L().Info("No pending withdrawal transaction found to revert -- skipping",
			zap.String("transaction_id", tx.Id),
			zap.String("idempotency_key", tx.IdempotencyKey))
		d.markTransactionProcessed(tx.Id)
		return nil
	}

	// Fall back to compensating transaction (SQLite only -- RevertTransaction
	// returns "not supported" for SQLite, triggering this path).
	zap.L().Debug("Native revert unavailable, using compensating transaction",
		zap.Error(revertErr))

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
