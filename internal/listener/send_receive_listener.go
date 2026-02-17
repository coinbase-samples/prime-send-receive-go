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
	"fmt"
	"sync"
	"time"

	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/store"

	"go.uber.org/zap"
)

// Start begins the deposit monitoring process
func (d *SendReceiveListener) Start(ctx context.Context, assetsFile string) error {
	zap.L().Info("Starting deposit listener")

	// Load monitored wallets
	if err := d.LoadMonitoredWallets(ctx, assetsFile); err != nil {
		return fmt.Errorf("failed to load monitored wallets: %w", err)
	}

	if len(d.monitoredWallets) == 0 {
		zap.L().Warn("No wallets to monitor - make sure addresses have been created")
		return fmt.Errorf("no wallets to monitor")
	}

	// Perform startup recovery to catch any missed transactions
	if err := d.performStartupRecovery(ctx); err != nil {
		zap.L().Error("Startup recovery failed", zap.Error(err))
		return fmt.Errorf("startup recovery failed: %w", err)
	}

	go d.pollLoop(ctx)
	go d.cleanupLoop(ctx)

	zap.L().Info("Deposit listener started successfully",
		zap.Duration("polling_interval", d.pollingInterval),
		zap.Duration("lookback_window", d.lookbackWindow))

	return nil
}

// Stop gracefully stops the deposit listener
func (d *SendReceiveListener) Stop() {
	zap.L().Info("Stopping deposit listener")
	close(d.stopChan)
	<-d.doneChan
	zap.L().Info("Deposit listener stopped")
}

// pollLoop runs the main polling loop
func (d *SendReceiveListener) pollLoop(ctx context.Context) {
	defer close(d.doneChan)

	ticker := time.NewTicker(d.pollingInterval)
	defer ticker.Stop()

	d.pollWallets(ctx)

	for {
		select {
		case <-ticker.C:
			d.pollWallets(ctx)
		case <-d.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

// ANSI color helpers for console output.
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[90m"
)

// pollWallets polls all monitored wallets for new transactions
func (d *SendReceiveListener) pollWallets(ctx context.Context) {
	since := time.Now().UTC().Add(-d.lookbackWindow)

	fmt.Printf("\n%s[%s] Polling %d wallets (lookback: %s)%s\n",
		colorCyan, time.Now().Format("15:04:05"), len(d.monitoredWallets), d.lookbackWindow, colorReset)

	var wg sync.WaitGroup

	for _, wallet := range d.monitoredWallets {
		wg.Add(1)

		go func(w models.WalletInfo) {
			defer wg.Done()

			if err := d.pollWallet(ctx, w, since); err != nil {
				fmt.Printf("  %s✗ %s (%s): %s%s\n", colorRed, w.AssetSymbol, w.Id[:8], err, colorReset)
				zap.L().Error("Failed to poll wallet",
					zap.String("wallet_id", w.Id),
					zap.String("asset_symbol", w.AssetSymbol),
					zap.Error(err))
			}
		}(wallet)
	}

	wg.Wait()
}

// pollWallet polls a specific wallet for new transactions
func (d *SendReceiveListener) pollWallet(ctx context.Context, wallet models.WalletInfo, since time.Time) error {
	transactions, err := d.fetchWalletTransactions(ctx, wallet.Id, since)
	if err != nil {
		return fmt.Errorf("failed to fetch transactions: %w", err)
	}

	newCount := 0
	for _, tx := range transactions {
		if d.isTransactionProcessed(tx.Id) {
			continue
		}
		newCount++

		txIdShort := tx.Id
		if len(txIdShort) > 12 {
			txIdShort = txIdShort[:12] + "..."
		}

		if err := d.processTransaction(ctx, tx, wallet); err != nil {
			fmt.Printf("  %s✗ %s %s %s %s | %s %s | %s%s\n",
				colorRed, wallet.AssetSymbol, tx.Type, tx.Status, tx.Amount,
				txIdShort, tx.Network, err, colorReset)
			zap.L().Error("Failed to process transaction",
				zap.String("transaction_id", tx.Id),
				zap.String("wallet_id", wallet.Id),
				zap.Error(err))
		} else {
			color := colorGreen
			symbol := "✓"
			if tx.Type != "DEPOSIT" && tx.Type != "WITHDRAWAL" {
				color = colorYellow
				symbol = "~"
			}
			fmt.Printf("  %s%s %s %s %s %s | %s %s%s\n",
				color, symbol, wallet.AssetSymbol, tx.Type, tx.Status, tx.Amount,
				txIdShort, tx.Network, colorReset)
		}
	}

	if newCount == 0 && len(transactions) > 0 {
		zap.L().Debug("All transactions already processed",
			zap.String("wallet_id", wallet.Id),
			zap.String("asset", wallet.AssetSymbol),
			zap.Int("total", len(transactions)))
	}

	return nil
}

// processTransaction processes a single Prime transaction
func (d *SendReceiveListener) processTransaction(ctx context.Context, tx models.PrimeTransaction, wallet models.WalletInfo) error {
	if d.isTransactionProcessed(tx.Id) {
		return nil
	}

	switch tx.Type {
	case "DEPOSIT":
		return d.processDeposit(ctx, tx, wallet)
	case "WITHDRAWAL":
		return d.processWithdrawal(ctx, tx, wallet)
	case "CONVERSION":
		return d.processConversion(ctx, tx, wallet)
	default:
		txTime := tx.CompletedAt
		if txTime.IsZero() {
			txTime = tx.CreatedAt
		}
		err := d.dbService.RecordPlatformTransaction(ctx, store.PlatformTransactionParams{
			TransactionId:   tx.Id,
			Type:            tx.Type,
			Status:          tx.Status,
			Symbol:          tx.Symbol,
			Amount:          tx.Amount,
			Network:         tx.Network,
			WalletId:        wallet.Id,
			TransactionTime: txTime,
			Metadata: map[string]string{
				"idempotency_key": tx.IdempotencyKey,
				"transaction_id":  tx.TransactionId,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to record %s transaction: %w", tx.Type, err)
		}
		d.markTransactionProcessed(tx.Id)
		return nil
	}
}

// processConversion handles a CONVERSION transaction (e.g. USD -> USDC).
// tx.Symbol is the source asset, tx.DestinationSymbol is the target asset.
// The wallets are resolved by looking up each asset on the Prime portfolio,
// not by assuming the polling wallet is the source.
func (d *SendReceiveListener) processConversion(ctx context.Context, tx models.PrimeTransaction, pollingWallet models.WalletInfo) error {
	if tx.Status != "TRANSACTION_DONE" {
		zap.L().Debug("Skipping non-completed conversion",
			zap.String("transaction_id", tx.Id),
			zap.String("status", tx.Status))
		return nil
	}

	sourceSymbol := tx.Symbol
	destSymbol := tx.DestinationSymbol
	if destSymbol == "" {
		destSymbol = sourceSymbol
	}

	// Resolve the SOURCE wallet by the source asset symbol.
	sourceWalletId := pollingWallet.Id // default if lookup fails
	if sourceSymbol == pollingWallet.AssetSymbol {
		sourceWalletId = pollingWallet.Id
	} else {
		srcWallets, err := d.primeService.ListWallets(ctx, d.portfolioId, "TRADING", []string{sourceSymbol})
		if err == nil && len(srcWallets) > 0 {
			sourceWalletId = srcWallets[0].Id
		}
	}

	// Resolve the DESTINATION wallet by the destination asset symbol.
	destWalletId := pollingWallet.Id // default if lookup fails
	if destSymbol == pollingWallet.AssetSymbol {
		destWalletId = pollingWallet.Id
	} else {
		dstWallets, err := d.primeService.ListWallets(ctx, d.portfolioId, "TRADING", []string{destSymbol})
		if err == nil && len(dstWallets) > 0 {
			destWalletId = dstWallets[0].Id
		} else {
			zap.L().Warn("Could not find destination wallet for conversion",
				zap.String("destination_symbol", destSymbol))
		}
	}

	zap.L().Info("Processing conversion",
		zap.String("transaction_id", tx.Id),
		zap.String("source", sourceSymbol),
		zap.String("destination", destSymbol),
		zap.String("source_wallet", sourceWalletId),
		zap.String("dest_wallet", destWalletId),
		zap.String("amount", tx.Amount))

	txTime := tx.CompletedAt
	if txTime.IsZero() {
		txTime = tx.CreatedAt
	}

	err := d.dbService.RecordConversion(ctx, store.ConversionParams{
		TransactionId:     tx.Id,
		Status:            tx.Status,
		SourceSymbol:      sourceSymbol,
		SourceAmount:      tx.Amount,
		DestinationSymbol: destSymbol,
		DestinationAmount: tx.Amount,
		SourceWalletId:    sourceWalletId,
		DestWalletId:      destWalletId,
		Network:           tx.Network,
		Fees:              tx.Fees,
		FeeSymbol:         tx.FeeSymbol,
		TransactionTime:   txTime,
	})
	if err != nil {
		return fmt.Errorf("failed to record conversion: %w", err)
	}

	d.markTransactionProcessed(tx.Id)
	return nil
}

// performStartupRecovery checks for missed transactions during downtime
func (d *SendReceiveListener) performStartupRecovery(ctx context.Context) error {
	zap.L().Info("Starting startup recovery process")

	// Get the most recent transaction timestamp from our database
	mostRecentTime, err := d.dbService.GetMostRecentTransactionTime(ctx)
	if err != nil {
		return fmt.Errorf("failed to get most recent transaction time: %w", err)
	}

	now := time.Now().UTC() // Ensure we work in UTC
	recoveryStart := now.Add(-d.lookbackWindow)

	zap.L().Info("Recovery window calculated",
		zap.Time("most_recent_tx", mostRecentTime),
		zap.Time("current_time", now),
		zap.Time("recovery_start", recoveryStart),
		zap.Duration("lookback_window", d.lookbackWindow))

	// Poll all wallets for transactions in the recovery window
	var totalRecovered int
	var failedWallets []string
	for _, wallet := range d.monitoredWallets {
		recovered, err := d.recoverWalletTransactions(ctx, wallet, recoveryStart)
		if err != nil {
			zap.L().Error("Failed to recover transactions for wallet",
				zap.String("wallet_id", wallet.Id),
				zap.String("asset_symbol", wallet.AssetSymbol),
				zap.Error(err))
			failedWallets = append(failedWallets, fmt.Sprintf("%s(%s)", wallet.AssetSymbol, wallet.Id))
			// Continue with other wallets
			continue
		}
		totalRecovered += recovered
	}

	// Log summary with warnings if some wallets failed
	if len(failedWallets) > 0 {
		zap.L().Warn("Startup recovery completed with some failures",
			zap.Int("total_transactions_recovered", totalRecovered),
			zap.Int("total_wallets", len(d.monitoredWallets)),
			zap.Int("failed_wallets", len(failedWallets)),
			zap.Strings("failed_wallet_details", failedWallets))

		// If more than half the wallets failed, consider this a critical issue
		if len(failedWallets) > len(d.monitoredWallets)/2 {
			return fmt.Errorf("recovery failed for majority of wallets (%d/%d): %v",
				len(failedWallets), len(d.monitoredWallets), failedWallets)
		}
	} else {
		zap.L().Info("Startup recovery completed successfully",
			zap.Int("total_transactions_recovered", totalRecovered),
			zap.Int("total_wallets", len(d.monitoredWallets)))
	}

	return nil
}

// recoverWalletTransactions recovers transactions for a specific wallet
func (d *SendReceiveListener) recoverWalletTransactions(ctx context.Context, wallet models.WalletInfo, since time.Time) (int, error) {
	zap.L().Debug("Recovering transactions for wallet",
		zap.String("wallet_id", wallet.Id),
		zap.String("asset_symbol", wallet.AssetSymbol),
		zap.Time("since", since))

	// Fetch transactions from Prime API
	transactions, err := d.fetchWalletTransactions(ctx, wallet.Id, since)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch wallet transactions during recovery: %w", err)
	}

	zap.L().Debug("Fetched transactions for recovery",
		zap.String("wallet_id", wallet.Id),
		zap.String("asset_symbol", wallet.AssetSymbol),
		zap.Int("transaction_count", len(transactions)))

	var recovered int
	for _, tx := range transactions {
		// Skip if already processed
		if d.isTransactionProcessed(tx.Id) {
			zap.L().Debug("Transaction already processed during recovery, skipping",
				zap.String("transaction_id", tx.Id))
			continue
		}

		// Process transaction (duplicate prevention is handled in ProcessDepositV2)
		if err := d.processTransaction(ctx, tx, wallet); err != nil {
			// Log error but continue - the transaction might already exist
			zap.L().Debug("Transaction processing during recovery",
				zap.String("transaction_id", tx.Id),
				zap.String("wallet_id", wallet.Id),
				zap.Error(err))
		} else {
			recovered++
			zap.L().Info("Recovered transaction",
				zap.String("transaction_id", tx.Id),
				zap.String("asset_symbol", tx.Symbol),
				zap.String("network", tx.Network),
				zap.String("amount", tx.Amount))
		}
	}

	return recovered, nil
}
