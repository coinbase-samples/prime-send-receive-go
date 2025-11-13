package main

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"strings"

	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)

func validateEmail(email string) error {
	if email == "" {
		return fmt.Errorf("email cannot be empty")
	}
	if !emailRegex.MatchString(email) {
		return fmt.Errorf("invalid email format: %s", email)
	}
	return nil
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	if len(name) < 2 {
		return fmt.Errorf("name must be at least 2 characters")
	}
	return nil
}

func main() {
	ctx := context.Background()

	_, loggerCleanup := common.InitializeLogger()
	defer loggerCleanup()

	// Parse command line flags
	nameFlag := flag.String("name", "", "User's full name (required)")
	emailFlag := flag.String("email", "", "User's email address (required)")
	flag.Parse()

	// Validate required flags
	if *nameFlag == "" || *emailFlag == "" {
		zap.L().Fatal("Both flags are required: --name and --email")
	}

	// Validate name
	if err := validateName(*nameFlag); err != nil {
		zap.L().Fatal("Invalid name", zap.Error(err))
	}

	// Validate email
	if err := validateEmail(*emailFlag); err != nil {
		zap.L().Fatal("Invalid email", zap.Error(err))
	}

	zap.L().Info("Starting user creation process",
		zap.String("name", *nameFlag),
		zap.String("email", *emailFlag))

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		zap.L().Fatal("Failed to load config", zap.Error(err))
	}

	// Initialize services (both database and Prime API for address generation)
	zap.L().Info("Initializing services")
	services, err := common.InitializeServices(ctx, cfg)
	if err != nil {
		zap.L().Fatal("Failed to initialize services", zap.Error(err))
	}
	defer services.Close()

	// Generate UUID for the new user
	userId := uuid.New().String()

	// Create user in database
	zap.L().Info("Creating user in database",
		zap.String("id", userId),
		zap.String("name", *nameFlag),
		zap.String("email", *emailFlag))

	user, err := services.DbService.CreateUser(ctx, userId, *nameFlag, *emailFlag)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			zap.L().Fatal("User already exists with this email", zap.String("email", *emailFlag))
		}
		zap.L().Fatal("Failed to create user", zap.Error(err))
	}

	fmt.Println()
	common.PrintHeader("USER CREATED", common.DefaultWidth)
	fmt.Printf("ID:    %s\n", user.Id)
	fmt.Printf("Name:  %s\n", user.Name)
	fmt.Printf("Email: %s\n", user.Email)
	common.PrintSeparator("=", common.DefaultWidth)
	fmt.Println()

	zap.L().Info("User created successfully", zap.String("id", user.Id))

	// Load asset configuration
	zap.L().Info("Loading asset configuration for address generation")
	assetConfigs, err := common.LoadAssetConfig("assets.yaml")
	if err != nil {
		zap.L().Fatal("Failed to load asset config", zap.Error(err))
	}
	zap.L().Info("Asset configuration loaded", zap.Int("count", len(assetConfigs)))

	if len(assetConfigs) == 0 {
		fmt.Println("No assets configured in assets.yaml")
		fmt.Println("User created but no deposit addresses generated")
		fmt.Println("Configure assets.yaml and run: go run cmd/setup/main.go")
		return
	}

	// Generate deposit addresses for all configured assets
	fmt.Printf("Generating deposit addresses for %d assets...\n\n", len(assetConfigs))

	successCount := 0
	failedAssets := []string{}

	for _, assetConfig := range assetConfigs {
		zap.L().Info("Processing asset",
			zap.String("asset", assetConfig.Symbol),
			zap.String("network", assetConfig.Network))

		// Check if address already exists
		existingAddresses, err := services.DbService.GetAddresses(ctx, user.Id, assetConfig.Symbol, assetConfig.Network)
		if err != nil {
			zap.L().Error("Error checking existing addresses",
				zap.String("asset", assetConfig.Symbol),
				zap.Error(err))
			failedAssets = append(failedAssets, assetConfig.Symbol)
			continue
		}

		if len(existingAddresses) > 0 {
			fmt.Printf("✓ %s-%s: Address already exists\n", assetConfig.Symbol, assetConfig.Network)
			successCount++
			continue
		}

		// Get or create wallet
		wallets, err := services.PrimeService.ListWallets(ctx, services.DefaultPortfolio.Id, "TRADING", []string{assetConfig.Symbol})
		if err != nil {
			zap.L().Error("Error listing wallets",
				zap.String("asset", assetConfig.Symbol),
				zap.Error(err))
			failedAssets = append(failedAssets, assetConfig.Symbol)
			continue
		}

		var walletId string
		if len(wallets) > 0 {
			walletId = wallets[0].Id
			zap.L().Info("Using existing wallet",
				zap.String("asset", assetConfig.Symbol),
				zap.String("wallet_id", walletId))
		} else {
			walletName := fmt.Sprintf("%s Trading Wallet", assetConfig.Symbol)
			zap.L().Info("Creating new wallet",
				zap.String("asset", assetConfig.Symbol),
				zap.String("wallet_name", walletName))
			newWallet, err := services.PrimeService.CreateWallet(ctx, services.DefaultPortfolio.Id, walletName, assetConfig.Symbol, "TRADING")
			if err != nil {
				zap.L().Error("Error creating wallet",
					zap.String("asset", assetConfig.Symbol),
					zap.Error(err))
				failedAssets = append(failedAssets, assetConfig.Symbol)
				continue
			}
			walletId = newWallet.Id
		}

		// Create deposit address
		zap.L().Info("Creating deposit address",
			zap.String("asset", assetConfig.Symbol),
			zap.String("network", assetConfig.Network),
			zap.String("wallet_id", walletId))

		depositAddress, err := services.PrimeService.CreateDepositAddress(ctx, services.DefaultPortfolio.Id, walletId, assetConfig.Symbol, assetConfig.Network)
		if err != nil {
			zap.L().Error("Error creating deposit address",
				zap.String("asset", assetConfig.Symbol),
				zap.String("network", assetConfig.Network),
				zap.Error(err))
			failedAssets = append(failedAssets, assetConfig.Symbol)
			fmt.Printf("✗ %s-%s: Failed to create address\n", assetConfig.Symbol, assetConfig.Network)
			continue
		}

		// Store address in database
		storedAddress, err := services.DbService.StoreAddress(ctx, user.Id, assetConfig.Symbol, assetConfig.Network, depositAddress.Address, walletId, depositAddress.Id)
		if err != nil {
			zap.L().Error("Error storing address to database",
				zap.String("asset", assetConfig.Symbol),
				zap.String("address", depositAddress.Address),
				zap.Error(err))
			failedAssets = append(failedAssets, assetConfig.Symbol)
			fmt.Printf("✗ %s-%s: Failed to store address\n", assetConfig.Symbol, assetConfig.Network)
			continue
		}

		fmt.Printf("✓ %s-%s: %s\n", assetConfig.Symbol, assetConfig.Network, storedAddress.Address)
		successCount++
	}

	// Print summary
	fmt.Println()
	common.PrintHeader("ADDRESS GENERATION SUMMARY", common.DefaultWidth)
	fmt.Printf("Total Assets:      %d\n", len(assetConfigs))
	fmt.Printf("Successful:        %d\n", successCount)
	fmt.Printf("Failed:            %d\n", len(failedAssets))
	if len(failedAssets) > 0 {
		fmt.Printf("Failed Assets:     %s\n", strings.Join(failedAssets, ", "))
	}
	common.PrintSeparator("=", common.DefaultWidth)
	fmt.Println()

	if len(failedAssets) > 0 {
		zap.L().Warn("User created but some addresses failed to generate",
			zap.String("user_id", user.Id),
			zap.Int("successful", successCount),
			zap.Int("failed", len(failedAssets)),
			zap.Strings("failed_assets", failedAssets))
		fmt.Println("User created successfully but some deposit addresses failed to generate")
		fmt.Println("You can re-run setup to retry: go run cmd/setup/main.go")
	} else {
		zap.L().Info("User and all addresses created successfully",
			zap.String("user_id", user.Id),
			zap.Int("addresses_created", successCount))
		fmt.Println("User and all deposit addresses created successfully!")
	}
}
