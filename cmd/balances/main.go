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

package main

import (
	"context"
	"flag"
	"fmt"

	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"
	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/store"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type balanceStats struct {
	totalUsers        int
	totalBalances     int
	usersWithBalances int
}

func formatTransactionId(txId string) string {
	if txId == "" {
		return "none"
	}
	if len(txId) > 16 {
		return txId[:16] + "..."
	}
	return txId
}

func printBalance(balance models.AccountBalance, isLast bool) {
	prefix := common.BoxPrefix(isLast)

	if balance.Network != "" {
		// Formance: per-network balance with last transaction reference.
		lastTx := formatTransactionId(balance.LastTransactionId)
		fmt.Printf("%s %-6s on %-20s: %15s  (last_tx: %s, updated: %s)\n",
			prefix,
			balance.Asset,
			balance.Network,
			balance.Balance.String(),
			lastTx,
			balance.UpdatedAt.Format("2006-01-02 15:04:05"))
	} else {
		// SQLite: aggregated balance with version info.
		lastTx := formatTransactionId(balance.LastTransactionId)
		fmt.Printf("%s %-15s: %20s (v%d, last_tx: %s, updated: %s)\n",
			prefix,
			balance.Asset,
			balance.Balance.String(),
			balance.Version,
			lastTx,
			balance.UpdatedAt.Format("2006-01-02 15:04:05"))
	}
}

func printBalances(balances []models.AccountBalance) {
	// Group by asset to show per-network detail then total.
	type assetGroup struct {
		entries []models.AccountBalance
		total   decimal.Decimal
	}
	groups := make(map[string]*assetGroup)
	var assetOrder []string

	for _, b := range balances {
		g, ok := groups[b.Asset]
		if !ok {
			g = &assetGroup{}
			groups[b.Asset] = g
			assetOrder = append(assetOrder, b.Asset)
		}
		g.entries = append(g.entries, b)
		g.total = g.total.Add(b.Balance)
	}

	for ai, asset := range assetOrder {
		g := groups[asset]
		isLastAsset := ai == len(assetOrder)-1

		for _, b := range g.entries {
			printBalance(b, false)
		}
		// Print total for this asset.
		prefix := common.BoxPrefix(isLastAsset)
		if len(g.entries) > 1 || g.entries[0].Network != "" {
			fmt.Printf("%s TOTAL: %s %s\n", prefix, g.total.String(), asset)
		}
	}
}

// countUniqueAssets returns the number of distinct asset symbols.
func countUniqueAssets(balances []models.AccountBalance) int {
	seen := make(map[string]bool)
	for _, b := range balances {
		seen[b.Asset] = true
	}
	return len(seen)
}

// hasNetworkDetail returns true if any balance has network info (Formance).
func hasNetworkDetail(balances []models.AccountBalance) bool {
	for _, b := range balances {
		if b.Network != "" {
			return true
		}
	}
	return false
}

func printUserHeader(user common.UserInfo, balances []models.AccountBalance) {
	uniqueAssets := countUniqueAssets(balances)
	fmt.Printf("\n┌─ User: %s (%s)\n", user.Name, user.Email)
	fmt.Printf("│  ID: %s\n", user.Id)
	if hasNetworkDetail(balances) {
		fmt.Printf("│  Assets: %d (across %d accounts)\n", uniqueAssets, len(balances))
	} else {
		fmt.Printf("│  Assets: %d\n", uniqueAssets)
	}
	common.PrintBoxSeparator(78)
}

func processUser(ctx context.Context, user common.UserInfo, dbService store.LedgerStore, logger *zap.Logger) (int, error) {
	balances, err := dbService.GetAllUserBalances(ctx, user.Id)
	if err != nil {
		return 0, fmt.Errorf("failed to get balances: %w", err)
	}

	if len(balances) == 0 {
		return 0, nil
	}

	printUserHeader(user, balances)
	printBalances(balances)

	return len(balances), nil
}

func processUsersAndGenerateReport(ctx context.Context, users []common.UserInfo, dbService store.LedgerStore, logger *zap.Logger) balanceStats {
	stats := balanceStats{}

	for _, user := range users {
		stats.totalUsers++

		balanceCount, err := processUser(ctx, user, dbService, logger)
		if err != nil {
			logger.Error("Failed to process user",
				zap.String("user_id", user.Id),
				zap.String("user_name", user.Name),
				zap.Error(err))
			continue
		}

		if balanceCount > 0 {
			stats.usersWithBalances++
			stats.totalBalances += balanceCount
		}
	}

	return stats
}

func main() {
	ctx := context.Background()

	logger, loggerCleanup := common.InitializeLogger()
	defer loggerCleanup()

	// Parse command line flags
	emailFlag := flag.String("email", "", "Filter by specific user email (optional)")
	flag.Parse()

	logger.Info("Starting balance query")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}

	// Initialize database service (no need for Prime API for read-only operations)
	logger.Info("Connecting to database", zap.String("path", cfg.Database.Path))
	dbService, err := common.InitializeDatabaseOnly(ctx, cfg)
	if err != nil {
		logger.Fatal("Failed to initialize database", zap.Error(err))
	}
	defer dbService.Close()

	// Initialize users based on filter
	users, err := common.InitializeUsers(ctx, dbService, *emailFlag, logger)
	if err != nil {
		logger.Fatal("Failed to initialize users", zap.Error(err))
	}

	// Print header
	common.PrintHeader("USER BALANCE REPORT", common.DefaultWidth)

	// Process users and generate report
	stats := processUsersAndGenerateReport(ctx, users, dbService, logger)

	// Print footer summary
	summary := fmt.Sprintf("SUMMARY: %d users with balances (%d total balances across %d users queried)",
		stats.usersWithBalances, stats.totalBalances, stats.totalUsers)
	common.PrintFooter(summary, common.DefaultWidth)

	logger.Info("Balance query completed",
		zap.Int("users_queried", stats.totalUsers),
		zap.Int("users_with_balances", stats.usersWithBalances),
		zap.Int("total_balances", stats.totalBalances))
}
