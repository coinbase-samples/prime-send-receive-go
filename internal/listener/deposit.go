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

	"github.com/shopspring/decimal"
	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/store"

	"go.uber.org/zap"
)

// processDeposit processes a deposit transaction in two phases:
// Phase 1: TRANSACTION_IMPORT_PENDING -- park funds in pending deposits account
// Phase 2: TRANSACTION_IMPORTED -- move from pending to user account
func (d *SendReceiveListener) processDeposit(ctx context.Context, tx models.PrimeTransaction, wallet models.WalletInfo) error {
	if tx.Status == "TRANSACTION_IMPORT_PENDING" {
		return d.processDepositPending(ctx, tx, wallet)
	}

	if tx.Status == "TRANSACTION_IMPORTED" {
		return d.processDepositConfirmed(ctx, tx, wallet)
	}

	zap.L().Debug("Skipping deposit with unhandled status",
		zap.String("transaction_id", tx.Id),
		zap.String("status", tx.Status),
		zap.String("symbol", tx.Symbol),
		zap.String("amount", tx.Amount))
	return nil
}

// processDepositPending handles phase 1: park funds in pending.
func (d *SendReceiveListener) processDepositPending(ctx context.Context, tx models.PrimeTransaction, wallet models.WalletInfo) error {
	amount, err := decimal.NewFromString(tx.Amount)
	if err != nil {
		return fmt.Errorf("invalid amount: %w", err)
	}
	if amount.LessThanOrEqual(decimal.Zero) {
		return nil
	}

	canonicalSymbol := normalizeSymbol(tx.Symbol)

	var lookupAddress string
	if tx.TransferTo.AccountIdentifier != "" {
		lookupAddress = tx.TransferTo.AccountIdentifier
	} else if tx.TransferTo.Address != "" {
		lookupAddress = tx.TransferTo.Address
	}

	err = d.dbService.ProcessDepositPending(ctx, canonicalSymbol, wallet.Id, amount, tx.Id, lookupAddress)
	if err != nil {
		if errors.Is(err, store.ErrDuplicateTransaction) {
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		return fmt.Errorf("failed to record pending deposit: %w", err)
	}

	d.markTransactionProcessed(tx.Id)
	zap.L().Info("Pending deposit recorded",
		zap.String("transaction_id", tx.Id),
		zap.String("symbol", canonicalSymbol),
		zap.String("amount", amount.String()))
	return nil
}

// processDepositConfirmed handles phase 2: move from pending to user (or direct deposit).
func (d *SendReceiveListener) processDepositConfirmed(ctx context.Context, tx models.PrimeTransaction, wallet models.WalletInfo) error {

	amount, err := decimal.NewFromString(tx.Amount)
	if err != nil {
		return fmt.Errorf("invalid amount: %w", err)
	}

	if amount.LessThanOrEqual(decimal.Zero) {
		zap.L().Debug("Skipping zero/negative amount transaction",
			zap.String("transaction_id", tx.Id),
			zap.String("amount", amount.String()))
		return nil
	}

	var lookupAddress string
	if tx.TransferTo.AccountIdentifier != "" {
		lookupAddress = tx.TransferTo.AccountIdentifier
		zap.L().Debug("Using account_identifier for address lookup",
			zap.String("transaction_id", tx.Id),
			zap.String("account_identifier", tx.TransferTo.AccountIdentifier),
			zap.String("address", tx.TransferTo.Address))
	} else {
		lookupAddress = tx.TransferTo.Address
		zap.L().Debug("Using address for lookup",
			zap.String("transaction_id", tx.Id),
			zap.String("address", tx.TransferTo.Address))
	}

	if lookupAddress == "" {
		zap.L().Debug("No address or account_identifier found in transfer_to",
			zap.String("transaction_id", tx.Id),
			zap.String("transfer_to_type", tx.TransferTo.Type),
			zap.String("transfer_to_value", tx.TransferTo.Value))
		return nil
	}

	assetNetwork := fmt.Sprintf("%s-%s", tx.Symbol, tx.Network)
	assetNetwork = strings.TrimSuffix(assetNetwork, "-")

	zap.L().Info("Processing imported deposit",
		zap.String("transaction_id", tx.Id),
		zap.String("lookup_address", lookupAddress),
		zap.String("deposit_address", tx.TransferTo.Address),
		zap.String("account_identifier", tx.TransferTo.AccountIdentifier),
		zap.String("prime_api_symbol", tx.Symbol),
		zap.String("prime_api_network", tx.Network),
		zap.String("asset_network", assetNetwork),
		zap.String("amount", amount.String()),
		zap.Time("created_at", tx.CreatedAt),
		zap.Time("completed_at", tx.CompletedAt))

	// Resolve source address from TransferFrom -- Prime uses Address, Value, or
	// AccountIdentifier depending on the transfer type and network.
	sourceAddr := tx.TransferFrom.Address
	if sourceAddr == "" {
		sourceAddr = tx.TransferFrom.Value
	}
	if sourceAddr == "" {
		sourceAddr = tx.TransferFrom.AccountIdentifier
	}

	// Attach full Prime transaction context for rich metadata in the Formance backend.
	// Use CompletedAt as the effective transaction time; fall back to CreatedAt.
	txTime := tx.CompletedAt
	if txTime.IsZero() {
		txTime = tx.CreatedAt
	}

	depositCtx := models.WithPrimeDepositContext(ctx, &models.PrimeDepositContext{
		TransactionId:   tx.TransactionId,
		SourceAddress:   sourceAddr,
		SourceType:      tx.TransferFrom.Type,
		NetworkFees:     tx.NetworkFees,
		Fees:            tx.Fees,
		BlockchainIds:   tx.BlockchainIds,
		Network:         tx.Network,
		PrimeApiSymbol:  tx.Symbol,
		WalletId:        tx.WalletId,
		CreatedAt:       tx.CreatedAt.UTC().Format(time.RFC3339),
		CompletedAt:     tx.CompletedAt.UTC().Format(time.RFC3339),
		TransactionTime: txTime,
	})

	// Try two-phase: confirm from pending -> user (if pending phase was recorded).
	confirmErr := d.dbService.ConfirmDeposit(depositCtx, lookupAddress, tx.Symbol, amount, tx.Id)
	if confirmErr == nil {
		d.markTransactionProcessed(tx.Id)
		zap.L().Info("Deposit confirmed (pending -> user)",
			zap.String("transaction_id", tx.Id))
		return nil
	}
	// Fall back to single-phase direct deposit (no pending phase existed).
	result, err := d.apiService.ProcessDeposit(depositCtx, lookupAddress, tx.Symbol, amount, tx.Id)
	if err != nil {
		if errors.Is(err, store.ErrDuplicateTransaction) {
			zap.L().Info("Duplicate transaction detected - already processed, marking as handled",
				zap.String("transaction_id", tx.Id))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		if errors.Is(err, store.ErrUserNotFound) {
			zap.L().Warn("Deposit to unrecognized address - marking as processed to avoid repeated errors",
				zap.String("transaction_id", tx.Id),
				zap.String("address", lookupAddress),
				zap.String("asset_network", assetNetwork),
				zap.String("amount", amount.String()))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		return fmt.Errorf("failed to process deposit: %w", err)
	}

	if !result.Success {
		// Check if this is a duplicate transaction error (result.Error is a plain string)
		if result.Error == store.ErrDuplicateTransaction.Error() {
			zap.L().Info("Duplicate transaction detected - already processed, marking as handled",
				zap.String("transaction_id", tx.Id))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		// Check if this is an unrecognized address
		if result.Error == store.ErrUserNotFound.Error() {
			zap.L().Warn("Deposit to unrecognized address - marking as processed to avoid repeated errors",
				zap.String("transaction_id", tx.Id),
				zap.String("error", result.Error))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		zap.L().Warn("Deposit processing failed",
			zap.String("transaction_id", tx.Id),
			zap.String("error", result.Error))
		return fmt.Errorf("deposit processing failed: %s", result.Error)
	}

	d.markTransactionProcessed(tx.Id)

	zap.L().Info("Deposit processed successfully - balance updated",
		zap.String("transaction_id", tx.Id),
		zap.String("user_id", result.UserId),
		zap.String("asset", result.Asset),
		zap.String("amount", result.Amount.String()),
		zap.String("new_balance", result.NewBalance.String()),
		zap.Time("processed_at", time.Now()))

	return nil
}
