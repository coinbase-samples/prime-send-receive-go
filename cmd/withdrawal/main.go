package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"
	"prime-send-receive-go/internal/database"
	"prime-send-receive-go/internal/models"
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

func verifyBalance(ctx context.Context, services *common.Services, user *models.User, symbol string, amount decimal.Decimal) (decimal.Decimal, error) {
	currentBalance, err := services.DbService.GetUserBalance(ctx, user.Id, symbol)
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to get user balance: %w", err)
	}

	if currentBalance.LessThan(amount) {
		return currentBalance, fmt.Errorf("insufficient balance: current=%s, requested=%s, shortfall=%s",
			currentBalance.String(), amount.String(), amount.Sub(currentBalance).String())
	}

	zap.L().Info("✅ Balance verification successful",
		zap.String("user", user.Email),
		zap.String("current_balance", currentBalance.String()),
		zap.String("withdrawal_amount", amount.String()),
		zap.String("remaining_balance", currentBalance.Sub(amount).String()))

	return currentBalance, nil
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
		if errors.Is(err, database.ErrConcurrentModification) {
			return fmt.Errorf("balance was modified by another withdrawal - please retry")
		}
		if errors.Is(err, database.ErrDuplicateTransaction) {
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
	zap.L().Error("Prime API withdrawal failed - rolling back local debit",
		zap.String("user_id", userId),
		zap.String("asset", symbol),
		zap.String("amount", amount.String()))

	fmt.Println("\n❌ Prime API withdrawal failed - rolling back...")

	err := services.DbService.ReverseWithdrawal(ctx, userId, symbol, amount, idempotencyKey)
	if err != nil {
		return fmt.Errorf("CRITICAL: Failed to rollback withdrawal - manual intervention required: %w", err)
	}

	fmt.Println("✅ Local balance restored (rollback successful)")
	return nil
}

func printWithdrawalSummary(user *models.User, asset string, currentBalance, amount decimal.Decimal, destination string) {
	common.PrintHeader("WITHDRAWAL REQUEST", common.DefaultWidth)
	fmt.Printf("User:              %s (%s)\n", user.Name, user.Email)
	fmt.Printf("Asset:             %s\n", asset)
	fmt.Printf("Current Balance:   %s\n", currentBalance.String())
	fmt.Printf("Withdrawal Amount: %s\n", amount.String())
	fmt.Printf("Remaining Balance: %s\n", currentBalance.Sub(amount).String())
	fmt.Printf("Destination:       %s\n", destination)
	common.PrintSeparator("=", common.DefaultWidth)
	fmt.Println("\n✅ Balance verification PASSED - user has sufficient funds")
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
	zap.L().Info("Looking up user by email", zap.String("email", req.email))
	targetUser, err := services.DbService.GetUserByEmail(ctx, req.email)
	if err != nil {
		zap.L().Fatal("User not found", zap.String("email", req.email), zap.Error(err))
	}

	zap.L().Info("User found",
		zap.String("user_id", targetUser.Id),
		zap.String("user_name", targetUser.Name),
		zap.String("user_email", targetUser.Email))

	// Parse asset to extract symbol and network
	asset, err := parseAsset(req.asset)
	if err != nil {
		zap.L().Fatal("Invalid asset format", zap.String("asset", req.asset), zap.Error(err))
	}

	// Verify balance
	zap.L().Info("Checking user balance",
		zap.String("user_id", targetUser.Id),
		zap.String("symbol", asset.symbol))

	currentBalance, err := verifyBalance(ctx, services, targetUser, asset.symbol, req.amount)
	if err != nil {
		zap.L().Fatal("Balance verification failed", zap.Error(err))
	}

	// Print summary
	printWithdrawalSummary(targetUser, req.asset, currentBalance, req.amount, req.destination)

	// Get wallet ID
	zap.L().Info("Looking up wallet ID for asset",
		zap.String("asset", asset.symbol),
		zap.String("network", asset.network))

	walletId, err := getWalletForAsset(ctx, services, targetUser.Id, asset)
	if err != nil {
		zap.L().Fatal("Failed to get wallet", zap.Error(err))
	}

	zap.L().Info("Found wallet for asset",
		zap.String("wallet_id", walletId),
		zap.String("asset", req.asset))

	// Generate idempotency key
	idempotencyKey := generateIdempotencyKey(targetUser.Id)
	zap.L().Info("Generated idempotency key",
		zap.String("user_id", targetUser.Id),
		zap.String("idempotency_key", idempotencyKey))

	// Check if withdrawal already exists (idempotent)
	exists, err := checkExistingWithdrawal(ctx, services, targetUser.Id, asset.symbol, idempotencyKey)
	if err != nil {
		zap.L().Fatal("Failed to check existing withdrawal", zap.Error(err))
	}
	if exists {
		zap.L().Info("Returning existing withdrawal (idempotent)",
			zap.String("idempotency_key", idempotencyKey),
			zap.String("user_id", targetUser.Id),
			zap.String("asset", asset.symbol))
		return
	}

	// Reserve funds locally
	err = reserveFunds(ctx, services, targetUser.Id, asset.symbol, req.amount, idempotencyKey)
	if err != nil {
		zap.L().Fatal("Failed to reserve funds", zap.Error(err))
	}

	fmt.Printf("   New balance: %s\n\n", currentBalance.Sub(req.amount).String())

	// Execute withdrawal via Prime API
	err = executeWithdrawal(ctx, services, req, walletId, idempotencyKey)
	if err != nil {
		// Rollback on failure
		rollbackErr := rollbackWithdrawal(ctx, services, targetUser.Id, asset.symbol, req.amount, idempotencyKey)
		if rollbackErr != nil {
			zap.L().Fatal("CRITICAL: Rollback failed", zap.Error(rollbackErr))
		}
		zap.L().Fatal("Prime API withdrawal failed (local balance rolled back)", zap.Error(err))
	}

	zap.L().Info("Withdrawal completed successfully",
		zap.String("user_id", targetUser.Id),
		zap.String("asset", asset.symbol),
		zap.String("amount", req.amount.String()))
}
