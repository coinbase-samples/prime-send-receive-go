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
	"go.uber.org/zap"
	"prime-send-receive-go/internal/database"
	"prime-send-receive-go/internal/models"
)

// processDeposit processes a deposit transaction
func (d *SendReceiveListener) processDeposit(ctx context.Context, tx models.PrimeTransaction, wallet models.WalletInfo) error {
	if tx.Status != "TRANSACTION_IMPORTED" {
		zap.L().Debug("Skipping non-imported deposit - waiting for completion",
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

	// Pass Prime API symbol to ledger - ProcessDeposit will use canonical symbol from address lookup
	// This handles cases where Prime API returns "BASEUSDC" but we store as "USDC" with network="base-mainnet"
	result, err := d.apiService.ProcessDeposit(ctx, lookupAddress, tx.Symbol, amount, tx.Id)
	if err != nil {
		if errors.Is(err, database.ErrDuplicateTransaction) {
			zap.L().Info("Duplicate transaction detected - already processed, marking as handled",
				zap.String("transaction_id", tx.Id))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		if errors.Is(err, database.ErrUserNotFound) {
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
		if result.Error == database.ErrDuplicateTransaction.Error() {
			zap.L().Info("Duplicate transaction detected - already processed, marking as handled",
				zap.String("transaction_id", tx.Id))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		// Check if this is an unrecognized address
		if result.Error == database.ErrUserNotFound.Error() {
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
