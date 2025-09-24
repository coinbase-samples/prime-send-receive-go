package listener

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// processDeposit processes a deposit transaction
func (d *SendReceiveListener) processDeposit(ctx context.Context, tx PrimeTransaction, wallet WalletInfo) error {
	if tx.Status != "TRANSACTION_IMPORTED" {
		d.logger.Debug("Skipping non-imported deposit - waiting for completion",
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

	if amount <= 0 {
		d.logger.Debug("Skipping zero/negative amount transaction",
			zap.String("transaction_id", tx.Id),
			zap.Float64("amount", amount))
		return nil
	}

	var lookupAddress string
	if tx.TransferTo.AccountIdentifier != "" {
		lookupAddress = tx.TransferTo.AccountIdentifier
		d.logger.Debug("Using account_identifier for address lookup",
			zap.String("transaction_id", tx.Id),
			zap.String("account_identifier", tx.TransferTo.AccountIdentifier),
			zap.String("address", tx.TransferTo.Address))
	} else {
		lookupAddress = tx.TransferTo.Address
		d.logger.Debug("Using address for lookup",
			zap.String("transaction_id", tx.Id),
			zap.String("address", tx.TransferTo.Address))
	}

	if lookupAddress == "" {
		d.logger.Debug("No address or account_identifier found in transfer_to",
			zap.String("transaction_id", tx.Id),
			zap.String("transfer_to_type", tx.TransferTo.Type),
			zap.String("transfer_to_value", tx.TransferTo.Value))
		return nil
	}

	d.logger.Info("Processing imported deposit",
		zap.String("transaction_id", tx.Id),
		zap.String("lookup_address", lookupAddress),
		zap.String("deposit_address", tx.TransferTo.Address),
		zap.String("account_identifier", tx.TransferTo.AccountIdentifier),
		zap.String("asset", wallet.Asset),
		zap.Float64("amount", amount),
		zap.Time("created_at", tx.CreatedAt),
		zap.Time("completed_at", tx.CompletedAt))

	result, err := d.apiService.ProcessDeposit(ctx, lookupAddress, wallet.Asset, amount, tx.Id)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate transaction") {
			d.logger.Info("Duplicate transaction detected - already processed, marking as handled",
				zap.String("transaction_id", tx.Id))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		if strings.Contains(err.Error(), "no user found for address") {
			d.logger.Warn("Deposit to unrecognized address - marking as processed to avoid repeated errors",
				zap.String("transaction_id", tx.Id),
				zap.String("address", lookupAddress),
				zap.String("asset", wallet.Asset),
				zap.Float64("amount", amount))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		return fmt.Errorf("failed to process deposit: %v", err)
	}

	if !result.Success {
		// Check if this is a duplicate transaction error
		if strings.Contains(result.Error, "duplicate transaction") {
			d.logger.Info("Duplicate transaction detected - already processed, marking as handled",
				zap.String("transaction_id", tx.Id))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		// Check if this is an unrecognized address
		if strings.Contains(result.Error, "no user found for address") {
			d.logger.Warn("Deposit to unrecognized address - marking as processed to avoid repeated errors",
				zap.String("transaction_id", tx.Id),
				zap.String("error", result.Error))
			d.markTransactionProcessed(tx.Id)
			return nil
		}
		d.logger.Warn("Deposit processing failed",
			zap.String("transaction_id", tx.Id),
			zap.String("error", result.Error))
		return fmt.Errorf("deposit processing failed: %s", result.Error)
	}

	d.markTransactionProcessed(tx.Id)

	d.logger.Info("Deposit processed successfully - balance updated",
		zap.String("transaction_id", tx.Id),
		zap.String("user_id", result.UserId),
		zap.String("asset", result.Asset),
		zap.Float64("amount", result.Amount),
		zap.Float64("new_balance", result.NewBalance),
		zap.Time("processed_at", time.Now()))

	return nil
}
