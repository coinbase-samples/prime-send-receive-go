package listener

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"prime-send-receive-go/internal/models"
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

// pollWallets polls all monitored wallets for new transactions
func (d *SendReceiveListener) pollWallets(ctx context.Context) {
	zap.L().Info("Starting wallet polling cycle",
		zap.Int("wallet_count", len(d.monitoredWallets)),
		zap.Duration("lookback_window", d.lookbackWindow))

	// Calculate time window for this poll
	since := time.Now().UTC().Add(-d.lookbackWindow)
	zap.L().Info("Polling for transactions since",
		zap.Time("since", since))

	var wg sync.WaitGroup

	for _, wallet := range d.monitoredWallets {
		wg.Add(1)

		// Poll each wallet concurrently
		go func(w models.WalletInfo) {
			defer wg.Done()

			if err := d.pollWallet(ctx, w, since); err != nil {
				zap.L().Error("Failed to poll wallet",
					zap.String("wallet_id", w.Id),
					zap.String("asset_symbol", w.AssetSymbol),
					zap.Error(err))
			}
		}(wallet)
	}

	wg.Wait()

	zap.L().Info("Wallet polling cycle complete")
}

// pollWallet polls a specific wallet for new transactions
func (d *SendReceiveListener) pollWallet(ctx context.Context, wallet models.WalletInfo, since time.Time) error {
	zap.L().Info("Polling wallet for transactions",
		zap.String("wallet_id", wallet.Id),
		zap.String("asset_symbol", wallet.AssetSymbol),
		zap.Time("since", since))

	// Fetch transactions from Prime API
	transactions, err := d.fetchWalletTransactions(ctx, wallet.Id, since)
	if err != nil {
		return fmt.Errorf("failed to fetch wallet transactions: %w", err)
	}

	zap.L().Info("Fetched wallet transactions",
		zap.String("wallet_id", wallet.Id),
		zap.String("asset_symbol", wallet.AssetSymbol),
		zap.Int("transaction_count", len(transactions)))

	for i, tx := range transactions {
		if d.isTransactionProcessed(tx.Id) {
			continue
		}

		zap.L().Info("Processing transaction",
			zap.Int("tx_index", i+1),
			zap.Int("total_txs", len(transactions)),
			zap.String("transaction_id", tx.Id),
			zap.String("type", tx.Type),
			zap.String("status", tx.Status),
			zap.String("symbol", tx.Symbol),
			zap.String("amount", tx.Amount))

		if err := d.processTransaction(ctx, tx, wallet); err != nil {
			zap.L().Error("Failed to process transaction",
				zap.String("transaction_id", tx.Id),
				zap.String("wallet_id", wallet.Id),
				zap.Error(err))
		}
	}

	return nil
}

// processTransaction processes a single Prime transaction (deposit or withdrawal)
func (d *SendReceiveListener) processTransaction(ctx context.Context, tx models.PrimeTransaction, wallet models.WalletInfo) error {
	if d.isTransactionProcessed(tx.Id) {
		zap.L().Debug("Transaction already processed, skipping",
			zap.String("transaction_id", tx.Id))
		return nil
	}

	if tx.Type == "DEPOSIT" {
		return d.processDeposit(ctx, tx, wallet)
	} else if tx.Type == "WITHDRAWAL" {
		return d.processWithdrawal(ctx, tx, wallet)
	} else {
		zap.L().Debug("Skipping unsupported transaction type",
			zap.String("transaction_id", tx.Id),
			zap.String("type", tx.Type))
		return nil
	}
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
