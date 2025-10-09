package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()

	_, loggerCleanup := common.InitializeLogger()
	defer loggerCleanup()

	// Parse command line flags
	emailFlag := flag.String("email", "", "User email (required)")
	assetFlag := flag.String("asset", "", "Asset symbol (e.g., BTC, ETH) (required)")
	amountFlag := flag.String("amount", "", "Amount to withdraw (required)")
	destinationFlag := flag.String("destination", "", "Destination address (required)")
	flag.Parse()

	// Validate required flags
	if *emailFlag == "" || *assetFlag == "" || *amountFlag == "" || *destinationFlag == "" {
		zap.L().Fatal("All flags are required: --email, --asset, --amount, --destination")
	}

	zap.L().Info("Starting withdrawal process",
		zap.String("email", *emailFlag),
		zap.String("asset", *assetFlag),
		zap.String("amount", *amountFlag),
		zap.String("destination", *destinationFlag))

	// Parse amount
	amount, err := decimal.NewFromString(*amountFlag)
	if err != nil {
		zap.L().Fatal("Invalid amount format", zap.String("amount", *amountFlag), zap.Error(err))
	}

	// Validate amount is positive
	if amount.LessThanOrEqual(decimal.Zero) {
		zap.L().Fatal("Amount must be greater than zero", zap.String("amount", amount.String()))
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		zap.L().Fatal("Failed to load config", zap.Error(err))
	}

	// Initialize services (both database and Prime API)
	zap.L().Info("Initializing services")
	services, err := common.InitializeServices(ctx, cfg)
	if err != nil {
		zap.L().Fatal("Failed to initialize services", zap.Error(err))
	}
	defer services.Close()

	// Step 1: Find user by email
	zap.L().Info("Looking up user by email", zap.String("email", *emailFlag))
	targetUser, err := services.DbService.GetUserByEmail(ctx, *emailFlag)
	if err != nil {
		zap.L().Fatal("User not found", zap.String("email", *emailFlag), zap.Error(err))
	}

	zap.L().Info("User found",
		zap.String("user_id", targetUser.Id),
		zap.String("user_name", targetUser.Name),
		zap.String("user_email", targetUser.Email))

	// Step 2: Check current balance
	zap.L().Info("Checking user balance",
		zap.String("user_id", targetUser.Id),
		zap.String("asset", *assetFlag))

	currentBalance, err := services.DbService.GetUserBalance(ctx, targetUser.Id, *assetFlag)
	if err != nil {
		zap.L().Fatal("Failed to get user balance",
			zap.String("user_id", targetUser.Id),
			zap.String("asset", *assetFlag),
			zap.Error(err))
	}

	zap.L().Info("Current balance retrieved",
		zap.String("user_id", targetUser.Id),
		zap.String("asset", *assetFlag),
		zap.String("balance", currentBalance.String()))

	// Step 3: Verify sufficient balance
	if currentBalance.LessThan(amount) {
		zap.L().Fatal("Insufficient balance",
			zap.String("user", targetUser.Email),
			zap.String("asset", *assetFlag),
			zap.String("current_balance", currentBalance.String()),
			zap.String("requested_amount", amount.String()),
			zap.String("shortfall", amount.Sub(currentBalance).String()))
	}

	zap.L().Info("✅ Balance verification successful",
		zap.String("user", targetUser.Email),
		zap.String("asset", *assetFlag),
		zap.String("current_balance", currentBalance.String()),
		zap.String("withdrawal_amount", amount.String()),
		zap.String("remaining_balance", currentBalance.Sub(amount).String()))

	// Print summary
	common.PrintHeader("WITHDRAWAL REQUEST", common.DefaultWidth)
	fmt.Printf("User:              %s (%s)\n", targetUser.Name, targetUser.Email)
	fmt.Printf("Asset:             %s\n", *assetFlag)
	fmt.Printf("Current Balance:   %s\n", currentBalance.String())
	fmt.Printf("Withdrawal Amount: %s\n", amount.String())
	fmt.Printf("Remaining Balance: %s\n", currentBalance.Sub(amount).String())
	fmt.Printf("Destination:       %s\n", *destinationFlag)
	common.PrintSeparator("=", common.DefaultWidth)
	fmt.Println("\n✅ Balance verification PASSED - user has sufficient funds")
	fmt.Println()

	// Step 4: Get wallet ID from address database
	// Parse asset flag (format: SYMBOL-network-type, e.g., ETH-ethereum-mainnet)
	assetParts := strings.SplitN(*assetFlag, "-", 2)
	if len(assetParts) != 2 {
		zap.L().Fatal("Invalid asset format. Expected format: SYMBOL-network-type (e.g., ETH-ethereum-mainnet)",
			zap.String("asset", *assetFlag))
	}
	symbol := assetParts[0]
	network := assetParts[1]

	zap.L().Info("Looking up wallet ID for asset",
		zap.String("asset", symbol),
		zap.String("network", network))
	addresses, err := services.DbService.GetAddresses(ctx, targetUser.Id, symbol, network)
	if err != nil {
		zap.L().Fatal("Failed to get wallet for asset",
			zap.String("user_id", targetUser.Id),
			zap.String("asset", symbol),
			zap.String("network", network),
			zap.Error(err))
	}

	if len(addresses) == 0 {
		zap.L().Fatal("No wallet found for asset",
			zap.String("user_id", targetUser.Id),
			zap.String("asset", symbol),
			zap.String("network", network))
	}

	walletId := addresses[0].WalletId
	zap.L().Info("Found wallet for asset",
		zap.String("wallet_id", walletId),
		zap.String("asset", *assetFlag))

	// Generate idempotency key using format: {user_id_first_segment}-{uuid_fragment_without_first_segment}
	// Example: user_id "abc-123-def-456" + UUID "111-222-333-444-555" = "abc-222-333-444-555"
	userIdSegments := strings.Split(targetUser.Id, "-")
	uuidSegments := strings.Split(uuid.New().String(), "-")
	idempotencyKey := userIdSegments[0] + "-" + strings.Join(uuidSegments[1:], "-")

	zap.L().Info("Generated idempotency key",
		zap.String("user_id", targetUser.Id),
		zap.String("idempotency_key", idempotencyKey))

	// Step 5: Create withdrawal via Prime API
	fmt.Println("🔄 Creating withdrawal via Prime API...")
	zap.L().Info("Creating withdrawal",
		zap.String("portfolio_id", services.DefaultPortfolio.Id),
		zap.String("wallet_id", walletId),
		zap.String("amount", amount.String()),
		zap.String("destination", *destinationFlag))

	withdrawal, err := services.PrimeService.CreateWithdrawal(
		ctx,
		services.DefaultPortfolio.Id,
		walletId,
		*destinationFlag,
		amount.String(),
		*assetFlag,
		idempotencyKey,
	)
	if err != nil {
		zap.L().Fatal("Failed to create withdrawal via Prime API", zap.Error(err))
	}

	fmt.Printf("✅ Withdrawal created successfully!\n")
	fmt.Printf("   Activity ID: %s\n", withdrawal.ActivityId)
	fmt.Printf("   Amount:      %s %s\n", withdrawal.Amount, withdrawal.Asset)
	fmt.Printf("   Destination: %s\n\n", withdrawal.Destination)

	/* Let listener handle this
	// Step 6: Record withdrawal in database (negative amount)
	zap.L().Info("Recording withdrawal in database")
	err = services.DbService.ProcessWithdrawal(ctx, targetUser.Id, *assetFlag, amount, withdrawal.ActivityId)
	if err != nil {
		zap.L().Fatal("Failed to record withdrawal in database",
			zap.String("activity_id", withdrawal.ActivityId),
			zap.Error(err))
	}

	fmt.Println("✅ Withdrawal recorded in local database")
	common.PrintSeparator("=", common.DefaultWidth)
	fmt.Println()
	*/

	zap.L().Info("Withdrawal completed successfully",
		zap.String("activity_id", withdrawal.ActivityId),
		zap.String("user_id", targetUser.Id),
		zap.String("asset", *assetFlag),
		zap.String("amount", amount.String()))
}
