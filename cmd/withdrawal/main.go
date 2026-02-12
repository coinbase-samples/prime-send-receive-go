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
	"errors"
	"flag"
	"fmt"
	"strings"

	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"
	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/store"
	"prime-send-receive-go/internal/prime"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type withdrawalRequest struct {
	email       string
	asset       string
	amount      decimal.Decimal
	destination string
}

type assetInfo struct {
	symbol  string
	network string
}

func parseAndValidateFlags() (*withdrawalRequest, error) {
	emailFlag := flag.String("email", "", "User email (required)")
	assetFlag := flag.String("asset", "", "Asset symbol (e.g., BTC, ETH) (required)")
	amountFlag := flag.String("amount", "", "Amount to withdraw (required)")
	destinationFlag := flag.String("destination", "", "Destination address (required)")
	flag.Parse()

	if *emailFlag == "" || *assetFlag == "" || *amountFlag == "" || *destinationFlag == "" {
		return nil, fmt.Errorf("all flags are required: --email, --asset, --amount, --destination")
	}

	amount, err := decimal.NewFromString(*amountFlag)
	if err != nil {
		return nil, fmt.Errorf("invalid amount format: %w", err)
	}

	if amount.LessThanOrEqual(decimal.Zero) {
		return nil, fmt.Errorf("amount must be greater than zero")
	}

	return &withdrawalRequest{
		email:       *emailFlag,
		asset:       *assetFlag,
		amount:      amount,
		destination: *destinationFlag,
	}, nil
}

func parseAsset(assetStr string) (*assetInfo, error) {
	parts := strings.SplitN(assetStr, "-", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid asset format, expected: SYMBOL-network-type (e.g., ETH-ethereum-mainnet)")
	}
	return &assetInfo{
		symbol:  parts[0],
		network: parts[1],
	}, nil
}

func verifyBalance(ctx context.Context, services *common.Services, user *models.User, symbol, network string, amount decimal.Decimal) (decimal.Decimal, error) {
	// First try per-network balance (Formance returns per-network; SQLite ignores network).
	balances, err := services.DbService.GetAllUserBalances(ctx, user.Id)
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to get user balances: %w", err)
	}

	// Look for a balance matching this asset AND network.
	networkBalance := decimal.NewFromInt(-1) // sentinel: not found
	totalBalance := decimal.Zero
	for _, b := range balances {
		if b.Asset == symbol {
			totalBalance = totalBalance.Add(b.Balance)
			if b.Network == network {
				networkBalance = b.Balance
			}
		}
	}

	// Use network-specific balance if available (Formance), otherwise aggregated (SQLite).
	checkBalance := totalBalance
	if networkBalance.GreaterThanOrEqual(decimal.Zero) {
		checkBalance = networkBalance
	}

	if checkBalance.LessThan(amount) {
		return checkBalance, fmt.Errorf("insufficient balance: current=%s, requested=%s, shortfall=%s",
			checkBalance.String(), amount.String(), amount.Sub(checkBalance).String())
	}

	zap.L().Info("Balance verification successful",
		zap.String("user", user.Email),
		zap.String("symbol", symbol),
		zap.String("network", network),
		zap.String("balance", checkBalance.String()),
		zap.String("total_across_networks", totalBalance.String()))

	return checkBalance, nil
}

func getWalletForAsset(ctx context.Context, services *common.Services, userId string, asset *assetInfo) (string, error) {
	addresses, err := services.DbService.GetAddresses(ctx, userId, asset.symbol, asset.network)
	if err != nil {
		return "", fmt.Errorf("failed to get wallet for asset: %w", err)
	}

	if len(addresses) == 0 {
		return "", fmt.Errorf("no wallet found for asset %s-%s", asset.symbol, asset.network)
	}

	return addresses[0].WalletId, nil
}

func generateIdempotencyKey(userId string) string {
	userIdSegments := strings.Split(userId, "-")
	uuidSegments := strings.Split(uuid.New().String(), "-")
	return userIdSegments[0] + "-" + strings.Join(uuidSegments[1:], "-")
}

func checkExistingWithdrawal(ctx context.Context, services *common.Services, userId, symbol, idempotencyKey string) (bool, error) {
	existingTxs, err := services.DbService.GetTransactionHistory(ctx, userId, symbol, 1000, 0)
	if err != nil {
		return false, fmt.Errorf("failed to check transaction history: %w", err)
	}

	for _, tx := range existingTxs {
		if tx.ExternalTransactionId == idempotencyKey && tx.TransactionType == "withdrawal" {
			zap.L().Info("Idempotency key already used - returning existing withdrawal",
				zap.String("idempotency_key", idempotencyKey),
				zap.String("transaction_id", tx.Id),
				zap.String("amount", tx.Amount.String()),
				zap.Time("processed_at", tx.ProcessedAt))

			fmt.Println("\n✅ Withdrawal already processed (idempotent)")
			fmt.Printf("   Original transaction ID: %s\n", tx.Id)
			fmt.Printf("   Amount: %s %s\n", tx.Amount.Neg().String(), symbol)
			fmt.Printf("   Processed at: %s\n\n", tx.ProcessedAt.Format("2006-01-02 15:04:05"))

			return true, nil
		}
	}

	return false, nil
}

func reserveFunds(ctx context.Context, services *common.Services, userId, symbol string, amount decimal.Decimal, idempotencyKey string) error {
	fmt.Println("🔄 Reserving funds (debiting local balance)...")
	zap.L().Info("Debiting balance before withdrawal",
		zap.String("user_id", userId),
		zap.String("asset", symbol),
		zap.String("amount", amount.String()),
		zap.String("idempotency_key", idempotencyKey))

	err := services.DbService.ProcessWithdrawal(ctx, userId, symbol, amount, idempotencyKey)
	if err != nil {
		if errors.Is(err, store.ErrConcurrentModification) {
			return fmt.Errorf("balance was modified by another withdrawal - please retry")
		}
		if errors.Is(err, store.ErrDuplicateTransaction) {
			return fmt.Errorf("withdrawal with this idempotency key is already being processed - please retry in a moment")
		}
		return fmt.Errorf("failed to debit balance: %w", err)
	}

	fmt.Println("Funds reserved - balance debited locally")
	return nil
}

func executeWithdrawal(ctx context.Context, services *common.Services, req *withdrawalRequest, walletId, idempotencyKey string) error {
	fmt.Println("Creating withdrawal via Prime API...")
	zap.L().Info("Creating withdrawal",
		zap.String("portfolio_id", services.DefaultPortfolio.Id),
		zap.String("wallet_id", walletId),
		zap.String("amount", req.amount.String()),
		zap.String("destination", req.destination))

	withdrawal, err := services.PrimeService.CreateWithdrawal(ctx, prime.CreateWithdrawalParams{
		PortfolioId:        services.DefaultPortfolio.Id,
		WalletId:           walletId,
		DestinationAddress: req.destination,
		Amount:             req.amount.String(),
		Asset:              req.asset,
		IdempotencyKey:     idempotencyKey,
	})
	if err != nil {
		return fmt.Errorf("Prime API withdrawal failed: %w", err)
	}

	fmt.Printf("✅ Withdrawal created successfully!\n")
	fmt.Printf("   Activity ID: %s\n", withdrawal.ActivityId)
	fmt.Printf("   Amount:      %s %s\n", withdrawal.Amount, withdrawal.Asset)
	fmt.Printf("   Destination: %s\n\n", withdrawal.Destination)

	return nil
}

func rollbackWithdrawal(ctx context.Context, services *common.Services, userId, symbol string, amount decimal.Decimal, idempotencyKey string) error {
	zap.L().Error("Prime API withdrawal failed - rolling back",
		zap.String("user_id", userId),
		zap.String("asset", symbol),
		zap.String("amount", amount.String()))

	// Prefer native revert (Formance) -- atomically undoes the original transaction.
	// Falls back to ReverseWithdrawal (creates a compensating transaction) for SQLite.
	err := services.DbService.RevertTransaction(ctx, idempotencyKey)
	if err != nil {
		zap.L().Warn("Native revert not available or failed, using compensating transaction",
			zap.Error(err))
		err = services.DbService.ReverseWithdrawal(ctx, userId, symbol, amount, idempotencyKey)
		if err != nil {
			return fmt.Errorf("CRITICAL: Failed to rollback withdrawal - manual intervention required: %w", err)
		}
	}

	fmt.Println("✅ Balance restored (rollback successful)")
	return nil
}

func printWithdrawalSummary(user *models.User, asset string, currentBalance, amount decimal.Decimal, destination string) {
	parts := strings.SplitN(asset, "-", 2)
	symbol := parts[0]

	common.PrintHeader("WITHDRAWAL REQUEST", common.DefaultWidth)
	fmt.Printf("User:              %s (%s)\n", user.Name, user.Email)
	fmt.Printf("Asset:             %s\n", asset)
	fmt.Printf("Current Balance:   %s %s\n", currentBalance.String(), symbol)
	fmt.Printf("Withdrawal Amount: %s %s\n", amount.String(), symbol)
	fmt.Printf("Remaining Balance: %s %s\n", currentBalance.Sub(amount).String(), symbol)
	fmt.Printf("Destination:       %s\n", destination)
	common.PrintSeparator("=", common.DefaultWidth)
	fmt.Println("\n✅ Balance verification PASSED")
	fmt.Println()
}

func main() {
	ctx := context.Background()

	_, loggerCleanup := common.InitializeLogger()
	defer loggerCleanup()

	// Parse and validate command line flags
	req, err := parseAndValidateFlags()
	if err != nil {
		zap.L().Fatal("Invalid flags", zap.Error(err))
	}

	zap.L().Info("Starting withdrawal process",
		zap.String("email", req.email),
		zap.String("asset", req.asset),
		zap.String("amount", req.amount.String()),
		zap.String("destination", req.destination))

	// Load configuration and initialize services
	cfg, err := config.Load()
	if err != nil {
		zap.L().Fatal("Failed to load config", zap.Error(err))
	}

	zap.L().Info("Initializing services")
	services, err := common.InitializeServices(ctx, cfg)
	if err != nil {
		zap.L().Fatal("Failed to initialize services", zap.Error(err))
	}
	defer services.Close()

	// Find user by email
	targetUser, err := services.DbService.GetUserByEmail(ctx, req.email)
	if err != nil {
		common.PrintHeader("WITHDRAWAL FAILED", common.DefaultWidth)
		fmt.Printf("Error: User not found for email %s\n", req.email)
		common.PrintSeparator("=", common.DefaultWidth)
		zap.L().Fatal("User not found", zap.String("email", req.email), zap.Error(err))
	}

	// Parse asset to extract symbol and network
	asset, err := parseAsset(req.asset)
	if err != nil {
		common.PrintHeader("WITHDRAWAL FAILED", common.DefaultWidth)
		fmt.Printf("Error: Invalid asset format: %s\n", req.asset)
		fmt.Printf("Expected: SYMBOL-network-type (e.g. USDC-base-mainnet)\n")
		common.PrintSeparator("=", common.DefaultWidth)
		zap.L().Fatal("Invalid asset format", zap.String("asset", req.asset), zap.Error(err))
	}

	// Verify balance (checks per-network balance for Formance, aggregated for SQLite)
	currentBalance, err := verifyBalance(ctx, services, targetUser, asset.symbol, asset.network, req.amount)
	if err != nil {
		common.PrintHeader("WITHDRAWAL FAILED", common.DefaultWidth)
		fmt.Printf("User:              %s (%s)\n", targetUser.Name, targetUser.Email)
		fmt.Printf("Asset:             %s on %s\n", asset.symbol, asset.network)
		fmt.Printf("Balance (%s):  %s %s\n", asset.network, currentBalance.String(), asset.symbol)
		fmt.Printf("Requested Amount:  %s %s\n", req.amount.String(), asset.symbol)
		fmt.Printf("Shortfall:         %s %s\n", req.amount.Sub(currentBalance).String(), asset.symbol)
		fmt.Printf("Destination:       %s\n", req.destination)
		common.PrintSeparator("=", common.DefaultWidth)
		fmt.Println("\n❌ Insufficient balance on this network")
		zap.L().Fatal("Balance verification failed", zap.Error(err))
	}

	// Print summary
	printWithdrawalSummary(targetUser, req.asset, currentBalance, req.amount, req.destination)

	// Get wallet ID
	walletId, err := getWalletForAsset(ctx, services, targetUser.Id, asset)
	if err != nil {
		zap.L().Fatal("Failed to get wallet", zap.Error(err))
	}

	// Generate idempotency key
	idempotencyKey := generateIdempotencyKey(targetUser.Id)

	// Check if withdrawal already exists (idempotent)
	exists, err := checkExistingWithdrawal(ctx, services, targetUser.Id, asset.symbol, idempotencyKey)
	if err != nil {
		zap.L().Fatal("Failed to check existing withdrawal", zap.Error(err))
	}
	if exists {
		return
	}

	// Reserve funds
	fmt.Println("🔄 Reserving funds...")
	err = reserveFunds(ctx, services, targetUser.Id, asset.symbol, req.amount, idempotencyKey)
	if err != nil {
		fmt.Println("❌ Failed to reserve funds")
		zap.L().Fatal("Failed to reserve funds", zap.Error(err))
	}
	fmt.Printf("   Funds reserved. New balance: %s %s\n\n", currentBalance.Sub(req.amount).String(), asset.symbol)

	// Execute withdrawal via Prime API
	fmt.Println("🔄 Creating withdrawal via Prime API...")
	err = executeWithdrawal(ctx, services, req, walletId, idempotencyKey)
	if err != nil {
		fmt.Println("\n❌ Prime API withdrawal failed -- rolling back...")
		rollbackErr := rollbackWithdrawal(ctx, services, targetUser.Id, asset.symbol, req.amount, idempotencyKey)
		if rollbackErr != nil {
			fmt.Println("❌ CRITICAL: Rollback failed -- manual intervention required")
			zap.L().Fatal("CRITICAL: Rollback failed", zap.Error(rollbackErr))
		}
		fmt.Printf("✅ Balance restored to %s %s\n", currentBalance.String(), asset.symbol)
		zap.L().Fatal("Prime API withdrawal failed (local balance rolled back)", zap.Error(err))
	}

	zap.L().Info("Withdrawal completed successfully",
		zap.String("user_id", targetUser.Id),
		zap.String("asset", asset.symbol),
		zap.String("amount", req.amount.String()))
}
