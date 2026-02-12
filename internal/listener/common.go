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
	"strings"
	"sync"
	"time"

	"prime-send-receive-go/internal/api"
	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/prime"
	"prime-send-receive-go/internal/store"

	"go.uber.org/zap"
)

// SendReceiveListenerConfig contains configuration for SendReceiveListener
type SendReceiveListenerConfig struct {
	PrimeService    *prime.Service
	ApiService      *api.LedgerService
	DbService       store.LedgerStore
	PortfolioId     string
	LookbackWindow  time.Duration
	PollingInterval time.Duration
	CleanupInterval time.Duration
}

// SendReceiveListener polls Prime API for new deposits and processes them
type SendReceiveListener struct {
	primeService *prime.Service
	apiService   *api.LedgerService
	dbService    store.LedgerStore

	// State management for processed transactions
	processedTxIds  map[string]time.Time
	mutex           sync.RWMutex
	lookbackWindow  time.Duration
	pollingInterval time.Duration
	cleanupInterval time.Duration

	// Monitoring configuration
	portfolioId      string
	monitoredWallets []models.WalletInfo

	// Control channels
	stopChan chan struct{}
	doneChan chan struct{}
}

// NewSendReceiveListener creates a new deposit listener
func NewSendReceiveListener(cfg SendReceiveListenerConfig) *SendReceiveListener {
	return &SendReceiveListener{
		primeService:    cfg.PrimeService,
		apiService:      cfg.ApiService,
		dbService:       cfg.DbService,
		processedTxIds:  make(map[string]time.Time),
		lookbackWindow:  cfg.LookbackWindow,
		pollingInterval: cfg.PollingInterval,
		cleanupInterval: cfg.CleanupInterval,
		portfolioId:     cfg.PortfolioId,
		stopChan:        make(chan struct{}),
		doneChan:        make(chan struct{}),
	}
}

func getUniqueAssetSymbols(assetConfigs []common.AssetConfig) map[string]bool {
	assetSymbols := make(map[string]bool)
	for _, assetConfig := range assetConfigs {
		assetSymbols[assetConfig.Symbol] = true
	}
	return assetSymbols
}

func getUserAddresses(ctx context.Context, dbService store.LedgerStore, userId string) ([]models.Address, error) {
	addresses, err := dbService.GetAllUserAddresses(ctx, userId)
	if err != nil {
		return nil, fmt.Errorf("failed to get addresses: %w", err)
	}
	return addresses, nil
}

func extractWalletsFromAddresses(addresses []models.Address, assetSymbols map[string]bool) map[string]models.WalletInfo {
	walletMap := make(map[string]models.WalletInfo)
	for _, addr := range addresses {
		if assetSymbols[addr.Asset] && addr.WalletId != "" {
			walletMap[addr.WalletId] = models.WalletInfo{
				Id:          addr.WalletId,
				AssetSymbol: addr.Asset,
			}
		}
	}
	return walletMap
}

func collectWalletsFromAllUsers(ctx context.Context, dbService store.LedgerStore, users []models.User, assetSymbols map[string]bool) map[string]models.WalletInfo {
	allWallets := make(map[string]models.WalletInfo)

	for _, user := range users {
		addresses, err := getUserAddresses(ctx, dbService, user.Id)
		if err != nil {
			zap.L().Error("Failed to get addresses for user",
				zap.String("user_id", user.Id),
				zap.Error(err))
			continue
		}

		userWallets := extractWalletsFromAddresses(addresses, assetSymbols)
		for walletId, wallet := range userWallets {
			allWallets[walletId] = wallet
		}
	}

	return allWallets
}

// LoadMonitoredWallets discovers trading wallets to monitor.
// If assetsFile is empty, discovers ALL wallets from the Prime portfolio.
// If assetsFile is provided, only monitors wallets for the assets listed in that file.
func (d *SendReceiveListener) LoadMonitoredWallets(ctx context.Context, assetsFile string) error {
	if assetsFile != "" {
		return d.loadFilteredWallets(ctx, assetsFile)
	}
	return d.loadAllWallets(ctx, assetsFile)
}

// loadAllWallets discovers ALL trading wallets from Prime.
func (d *SendReceiveListener) loadAllWallets(ctx context.Context, assetsFileFallback string) error {
	zap.L().Info("Discovering ALL wallets from Prime portfolio",
		zap.String("portfolio_id", d.portfolioId))

	allWallets, err := d.primeService.ListWallets(ctx, d.portfolioId, "TRADING", nil)
	if err == nil && len(allWallets) > 0 {
		d.monitoredWallets = make([]models.WalletInfo, 0, len(allWallets))
		seen := make(map[string]bool)
		for _, w := range allWallets {
			if !seen[w.Id] {
				seen[w.Id] = true
				d.monitoredWallets = append(d.monitoredWallets, models.WalletInfo{
					Id:          w.Id,
					AssetSymbol: w.Symbol,
				})
			}
		}

		zap.L().Info("Monitoring ALL Prime wallets",
			zap.Int("count", len(d.monitoredWallets)))
		for _, w := range d.monitoredWallets {
			zap.L().Info("  Wallet",
				zap.String("id", w.Id),
				zap.String("asset", w.AssetSymbol))
		}
		return nil
	}

	// Fallback to assets.yaml + local store if Prime call failed.
	if err != nil {
		zap.L().Warn("Could not discover wallets from Prime, falling back to local store",
			zap.Error(err))
	}
	return d.loadFilteredWallets(ctx, assetsFileFallback)
}

// loadFilteredWallets monitors only wallets matching assets in the given file.
func (d *SendReceiveListener) loadFilteredWallets(ctx context.Context, assetsFile string) error {
	if assetsFile == "" {
		assetsFile = "assets.yaml"
	}

	zap.L().Info("Loading filtered wallets from assets file",
		zap.String("file", assetsFile))

	assetConfigs, err := common.LoadAssetConfig(assetsFile)
	if err != nil {
		return fmt.Errorf("failed to load assets from %s: %w", assetsFile, err)
	}

	// Try Prime first with specific symbols.
	symbols := make([]string, 0, len(assetConfigs))
	seen := make(map[string]bool)
	for _, ac := range assetConfigs {
		if !seen[ac.Symbol] {
			seen[ac.Symbol] = true
			symbols = append(symbols, ac.Symbol)
		}
	}

	wallets, err := d.primeService.ListWallets(ctx, d.portfolioId, "TRADING", symbols)
	if err == nil && len(wallets) > 0 {
		d.monitoredWallets = make([]models.WalletInfo, 0, len(wallets))
		walletSeen := make(map[string]bool)
		for _, w := range wallets {
			if !walletSeen[w.Id] {
				walletSeen[w.Id] = true
				d.monitoredWallets = append(d.monitoredWallets, models.WalletInfo{
					Id:          w.Id,
					AssetSymbol: w.Symbol,
				})
			}
		}
		zap.L().Info("Monitoring filtered Prime wallets",
			zap.Int("count", len(d.monitoredWallets)),
			zap.Strings("symbols", symbols))
		return nil
	}

	// Fallback: local store.
	assetSymbols := getUniqueAssetSymbols(assetConfigs)
	users, userErr := d.dbService.GetUsers(ctx)
	if userErr != nil {
		return fmt.Errorf("failed to get users: %w", userErr)
	}

	walletMap := collectWalletsFromAllUsers(ctx, d.dbService, users, assetSymbols)
	d.monitoredWallets = make([]models.WalletInfo, 0, len(walletMap))
	for _, wallet := range walletMap {
		d.monitoredWallets = append(d.monitoredWallets, wallet)
	}

	zap.L().Info("Loaded monitored wallets from local store (fallback)",
		zap.Int("count", len(d.monitoredWallets)))
	return nil
}

// fetchWalletTransactions calls Prime API to get wallet transactions
func (d *SendReceiveListener) fetchWalletTransactions(ctx context.Context, walletId string, since time.Time) ([]models.PrimeTransaction, error) {
	zap.L().Debug("Fetching wallet transactions from Prime API",
		zap.String("wallet_id", walletId),
		zap.Time("since", since))

	// Call Prime SDK (automatically paginates through all pages)
	primeTxns, err := d.primeService.ListWalletTransactions(ctx, d.portfolioId, walletId, since)
	if err != nil {
		return nil, fmt.Errorf("Prime API call failed: %w", err)
	}

	// Convert Prime SDK response to our internal format
	transactions := make([]models.PrimeTransaction, 0, len(primeTxns))

	for _, tx := range primeTxns {
		// Transaction times are already time.Time in the SDK
		createdAt := tx.Created
		completedAt := tx.Completed

		// Convert to our internal format
		primeTransaction := models.PrimeTransaction{
			Id:                tx.Id,
			WalletId:          tx.WalletId,
			Type:              tx.Type,
			Status:            tx.Status,
			Symbol:            tx.Symbol,
			DestinationSymbol: tx.DestinationSymbol,
			Amount:            tx.Amount,
			CreatedAt:         createdAt,
			CompletedAt:       completedAt,
			TransactionId:     tx.TransactionId,
			Network:           tx.Network,
			NetworkFees:       tx.NetworkFees,
			Fees:              tx.Fees,
			FeeSymbol:         tx.FeeSymbol,
			BlockchainIds:     tx.BlockchainIds,
			IdempotencyKey:    tx.IdempotencyKey,
		}

		if tx.TransferFrom != nil {
			primeTransaction.TransferFrom.Type = tx.TransferFrom.Type
			primeTransaction.TransferFrom.Value = tx.TransferFrom.Value
			primeTransaction.TransferFrom.Address = tx.TransferFrom.Address
			primeTransaction.TransferFrom.AccountIdentifier = tx.TransferFrom.AccountIdentifier
		}
		if tx.TransferTo != nil {
			primeTransaction.TransferTo.Type = tx.TransferTo.Type
			primeTransaction.TransferTo.Value = tx.TransferTo.Value
			primeTransaction.TransferTo.Address = tx.TransferTo.Address
			primeTransaction.TransferTo.AccountIdentifier = tx.TransferTo.AccountIdentifier
		}

		transactions = append(transactions, primeTransaction)
	}

	zap.L().Debug("Converted Prime transactions",
		zap.String("wallet_id", walletId),
		zap.Int("count", len(transactions)))

	return transactions, nil
}

// isTransactionProcessed checks if we've already processed this transaction
func (d *SendReceiveListener) isTransactionProcessed(txId string) bool {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	_, exists := d.processedTxIds[txId]
	return exists
}

// markTransactionProcessed marks a transaction as processed
func (d *SendReceiveListener) markTransactionProcessed(txId string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.processedTxIds[txId] = time.Now()
}

// cleanupLoop periodically cleans old processed transaction IDs
func (d *SendReceiveListener) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(d.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.cleanupProcessedTransactions()
		case <-d.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

// cleanupProcessedTransactions removes old entries from processed transactions map
func (d *SendReceiveListener) cleanupProcessedTransactions() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	cutoff := time.Now().UTC().Add(-d.lookbackWindow)
	cleaned := 0

	for txId, processedTime := range d.processedTxIds {
		if processedTime.Before(cutoff) {
			delete(d.processedTxIds, txId)
			cleaned++
		}
	}

	if cleaned > 0 {
		zap.L().Debug("Cleaned up old processed transactions",
			zap.Int("cleaned", cleaned),
			zap.Int("remaining", len(d.processedTxIds)))
	}
}

// findUserByIdempotencyKeyPrefix finds a user whose Id matches the prefix of the idempotency key
func (d *SendReceiveListener) findUserByIdempotencyKeyPrefix(ctx context.Context, idempotencyKey string) (string, error) {
	if idempotencyKey == "" {
		return "", fmt.Errorf("empty idempotency key")
	}

	// Extract the first UUID segment from idempotency key (before first hyphen)
	parts := strings.Split(idempotencyKey, "-")
	if len(parts) == 0 {
		return "", fmt.Errorf("invalid idempotency key format: %s", idempotencyKey)
	}
	idempotencyPrefix := parts[0]

	// Get all users from database
	users, err := d.dbService.GetUsers(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get users: %w", err)
	}

	// Look for a user whose Id starts with the same prefix
	for _, user := range users {
		userParts := strings.Split(user.Id, "-")
		if len(userParts) > 0 && userParts[0] == idempotencyPrefix {
			zap.L().Debug("Matched withdrawal to user by UUID prefix",
				zap.String("user_id", user.Id),
				zap.String("idempotency_key", idempotencyKey),
				zap.String("matched_prefix", idempotencyPrefix))
			return user.Id, nil
		}
	}

	return "", fmt.Errorf("no user found with UUID prefix matching idempotency key prefix %s: %s", idempotencyPrefix, idempotencyKey)
}
