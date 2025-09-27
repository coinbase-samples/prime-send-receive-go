package listener

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"prime-send-receive-go/internal/api"
	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/database"
	"prime-send-receive-go/internal/listener/models"
	"prime-send-receive-go/internal/prime"

	"go.uber.org/zap"
)

// SendReceiveListener polls Prime API for new deposits and processes them
type SendReceiveListener struct {
	primeService *prime.Service
	apiService   *api.LedgerService
	dbService    *database.Service
	logger       *zap.Logger

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
func NewSendReceiveListener(
	primeService *prime.Service,
	apiService *api.LedgerService,
	dbService *database.Service,
	logger *zap.Logger,
	portfolioId string,
	lookbackWindow time.Duration,
	pollingInterval time.Duration,
	cleanupInterval time.Duration,
) *SendReceiveListener {
	return &SendReceiveListener{
		primeService:    primeService,
		apiService:      apiService,
		dbService:       dbService,
		logger:          logger,
		processedTxIds:  make(map[string]time.Time),
		lookbackWindow:  lookbackWindow,
		pollingInterval: pollingInterval,
		cleanupInterval: cleanupInterval,
		portfolioId:     portfolioId,
		stopChan:        make(chan struct{}),
		doneChan:        make(chan struct{}),
	}
}

// LoadMonitoredWallets loads the list of trading wallets from the database
func (d *SendReceiveListener) LoadMonitoredWallets(ctx context.Context, assetsFile string) error {
	d.logger.Info("Loading monitored wallets from database")

	// Query all addresses to get unique wallet IDs
	users, err := d.dbService.GetUsers(ctx)
	if err != nil {
		return fmt.Errorf("failed to get users: %v", err)
	}

	walletMap := make(map[string]models.WalletInfo)

	// Load assets from file
	assets, err := common.LoadAssetSymbols(assetsFile)
	if err != nil {
		return fmt.Errorf("failed to load assets from YAML: %v", err)
	}

	d.logger.Info("Loaded assets from file",
		zap.String("file", assetsFile),
		zap.Strings("symbols", assets))

	for _, user := range users {
		for _, asset := range assets {
			addresses, err := d.dbService.GetAddresses(ctx, user.Id, asset)
			if err != nil {
				d.logger.Error("Failed to get addresses for user/asset",
					zap.String("user_id", user.Id),
					zap.String("asset", asset),
					zap.Error(err))
				continue
			}

			for _, addr := range addresses {
				if addr.WalletId != "" {
					walletMap[addr.WalletId] = models.WalletInfo{
						Id:      addr.WalletId,
						Asset:   addr.Asset,
						Network: addr.Network,
					}
				}
			}
		}
	}

	// Convert map to slice
	d.monitoredWallets = make([]models.WalletInfo, 0, len(walletMap))
	for _, wallet := range walletMap {
		d.monitoredWallets = append(d.monitoredWallets, wallet)
	}

	d.logger.Info("Loaded monitored wallets",
		zap.Int("count", len(d.monitoredWallets)),
		zap.Any("wallets", d.monitoredWallets))

	return nil
}

// fetchWalletTransactions calls Prime API to get wallet transactions
func (d *SendReceiveListener) fetchWalletTransactions(ctx context.Context, walletId string, since time.Time) ([]models.PrimeTransaction, error) {
	d.logger.Debug("Fetching wallet transactions from Prime API",
		zap.String("wallet_id", walletId),
		zap.Time("since", since))

	// Call Prime SDK
	response, err := d.primeService.ListWalletTransactions(ctx, d.portfolioId, walletId, since)
	if err != nil {
		return nil, fmt.Errorf("Prime API call failed: %v", err)
	}

	// Convert Prime SDK response to our internal format
	transactions := make([]models.PrimeTransaction, 0)

	for _, tx := range response.Transactions {
		// Transaction times are already time.Time in the SDK
		createdAt := tx.Created
		completedAt := tx.Completed

		// Convert to our internal format
		primeTransaction := models.PrimeTransaction{
			Id:             tx.Id,
			WalletId:       tx.WalletId,
			Type:           tx.Type,
			Status:         tx.Status,
			Symbol:         tx.Symbol,
			Amount:         tx.Amount,
			CreatedAt:      createdAt,
			CompletedAt:    completedAt,
			TransactionId:  tx.TransactionId,
			Network:        tx.Network,
			IdempotencyKey: tx.IdempotencyKey,
		}

		// Extract transfer_to information
		if tx.TransferTo != nil {
			primeTransaction.TransferTo.Type = tx.TransferTo.Type
			primeTransaction.TransferTo.Value = tx.TransferTo.Value
			primeTransaction.TransferTo.Address = tx.TransferTo.Address
			primeTransaction.TransferTo.AccountIdentifier = tx.TransferTo.AccountIdentifier
		}

		transactions = append(transactions, primeTransaction)
	}

	d.logger.Debug("Converted Prime transactions",
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
		d.logger.Debug("Cleaned up old processed transactions",
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
		return "", fmt.Errorf("failed to get users: %v", err)
	}

	// Look for a user whose Id starts with the same prefix
	for _, user := range users {
		userParts := strings.Split(user.Id, "-")
		if len(userParts) > 0 && userParts[0] == idempotencyPrefix {
			d.logger.Debug("Matched withdrawal to user by UUID prefix",
				zap.String("user_id", user.Id),
				zap.String("idempotency_key", idempotencyKey),
				zap.String("matched_prefix", idempotencyPrefix))
			return user.Id, nil
		}
	}

	return "", fmt.Errorf("no user found with UUID prefix matching idempotency key prefix %s: %s", idempotencyPrefix, idempotencyKey)
}
