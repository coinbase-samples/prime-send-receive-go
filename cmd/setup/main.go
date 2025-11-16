package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"

	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"
	"prime-send-receive-go/internal/database"
	"prime-send-receive-go/internal/models"

	"go.uber.org/zap"
)

// checkExistingAddress checks if user already has an address for the given asset
func checkExistingAddress(ctx context.Context, services *common.Services, user models.User, assetConfig common.AssetConfig) (bool, error) {
	existingAddresses, err := services.DbService.GetAddresses(ctx, user.Id, assetConfig.Symbol, assetConfig.Network)
	if err != nil {
		zap.L().Error("Error checking existing addresses",
			zap.String("user_id", user.Id),
			zap.String("asset", assetConfig.Symbol),
			zap.Error(err))
		return false, err
	}

	if len(existingAddresses) > 0 {
		zap.L().Info("User already has addresses for asset",
			zap.String("user_id", user.Id),
			zap.String("asset", assetConfig.Symbol),
			zap.Int("count", len(existingAddresses)),
			zap.String("latest_address", existingAddresses[0].Address))
		return true, nil
	}

	return false, nil
}

// getOrCreateWallet retrieves an existing trading wallet or creates a new one
func getOrCreateWallet(ctx context.Context, services *common.Services, assetSymbol string) (*models.Wallet, error) {
	zap.L().Debug("Listing wallets for asset", zap.String("asset", assetSymbol))
	wallets, err := services.PrimeService.ListWallets(ctx, services.DefaultPortfolio.Id, "TRADING", []string{assetSymbol})
	if err != nil {
		zap.L().Error("Error listing wallets",
			zap.String("asset", assetSymbol),
			zap.Error(err))
		return nil, err
	}

	if len(wallets) > 0 {
		wallet := &wallets[0]
		zap.L().Info("Using existing wallet",
			zap.String("asset", assetSymbol),
			zap.String("wallet_name", wallet.Name),
			zap.String("wallet_id", wallet.Id))
		return wallet, nil
	}

	// Create new wallet
	walletName := fmt.Sprintf("%s Trading Wallet", assetSymbol)
	zap.L().Info("Creating new wallet",
		zap.String("asset", assetSymbol),
		zap.String("wallet_name", walletName))

	wallet, err := services.PrimeService.CreateWallet(ctx, services.DefaultPortfolio.Id, walletName, assetSymbol, "TRADING")
	if err != nil {
		zap.L().Error("Error creating wallet",
			zap.String("asset", assetSymbol),
			zap.Error(err))
		return nil, err
	}

	zap.L().Info("Created new wallet",
		zap.String("asset", assetSymbol),
		zap.String("wallet_name", wallet.Name),
		zap.String("wallet_id", wallet.Id))
	return wallet, nil
}

// createAndStoreAddress creates a deposit address via Prime API and stores it in the database
func createAndStoreAddress(ctx context.Context, services *common.Services, user models.User, assetConfig common.AssetConfig, wallet *models.Wallet) error {
	zap.L().Info("Creating deposit address",
		zap.String("asset", assetConfig.Symbol),
		zap.String("network", assetConfig.Network),
		zap.String("wallet_id", wallet.Id))

	depositAddress, err := services.PrimeService.CreateDepositAddress(ctx, services.DefaultPortfolio.Id, wallet.Id, assetConfig.Symbol, assetConfig.Network)
	if err != nil {
		zap.L().Error("Error creating deposit address",
			zap.String("asset", assetConfig.Symbol),
			zap.String("network", assetConfig.Network),
			zap.Error(err))
		return err
	}

	zap.L().Info("Created deposit address",
		zap.String("asset", assetConfig.Symbol),
		zap.String("network", assetConfig.Network),
		zap.String("address", depositAddress.Address))

	// Store with separate asset and network columns
	storedAddress, err := services.DbService.StoreAddress(ctx, database.StoreAddressParams{
		UserId:            user.Id,
		Asset:             assetConfig.Symbol,
		Network:           assetConfig.Network,
		Address:           depositAddress.Address,
		WalletId:          wallet.Id,
		AccountIdentifier: depositAddress.Id,
	})
	if err != nil {
		zap.L().Error("Error storing address to database",
			zap.String("asset", assetConfig.Symbol),
			zap.String("address", depositAddress.Address),
			zap.Error(err))
		return err
	}

	zap.L().Info("Stored address to database",
		zap.String("id", storedAddress.Id),
		zap.String("asset", assetConfig.Symbol),
		zap.String("address", depositAddress.Address))

	addressOutput, err := json.MarshalIndent(depositAddress, "", "  ")
	if err != nil {
		zap.L().Error("Error marshaling address to JSON", zap.Error(err))
	} else {
		zap.L().Debug("Address details", zap.String("json", string(addressOutput)))
	}

	return nil
}

// processUserAsset processes a single user-asset combination
func processUserAsset(ctx context.Context, services *common.Services, user models.User, assetConfig common.AssetConfig) error {
	zap.L().Info("Processing asset",
		zap.String("user_id", user.Id),
		zap.String("asset", assetConfig.Symbol),
		zap.String("network", assetConfig.Network))

	// Check if address already exists
	exists, err := checkExistingAddress(ctx, services, user, assetConfig)
	if err != nil {
		return err
	}

	// Skip if address already exists
	if exists {
		return nil
	}

	// Get or create wallet
	wallet, err := getOrCreateWallet(ctx, services, assetConfig.Symbol)
	if err != nil {
		return err
	}

	// Create and store address
	return createAndStoreAddress(ctx, services, user, assetConfig, wallet)
}

func generateAddresses(ctx context.Context, services *common.Services) {
	zap.L().Info("Loading asset configuration")
	assetConfigs, err := common.LoadAssetConfig("assets.yaml")
	if err != nil {
		zap.L().Fatal("Failed to load asset config", zap.Error(err))
	}
	zap.L().Info("Asset configuration loaded", zap.Int("count", len(assetConfigs)))

	users, err := services.DbService.GetUsers(ctx)
	if err != nil {
		zap.L().Fatal("Failed to read users from database", zap.Error(err))
	}

	var totalAddresses, failedAddresses int
	var failedAssets []string

	for _, user := range users {
		zap.L().Info("Processing user",
			zap.String("id", user.Id),
			zap.String("name", user.Name),
			zap.String("email", user.Email))

		for _, assetConfig := range assetConfigs {
			err := processUserAsset(ctx, services, user, assetConfig)
			if err != nil {
				failedAddresses++
				failedAssets = append(failedAssets, fmt.Sprintf("%s/%s", user.Name, assetConfig.Symbol))
			} else {
				totalAddresses++
			}
		}
	}

	// Log summary
	if failedAddresses > 0 {
		zap.L().Warn("Address generation completed with some failures",
			zap.Int("total_addresses_created", totalAddresses),
			zap.Int("failed_addresses", failedAddresses),
			zap.Strings("failed_user_assets", failedAssets))
	} else {
		zap.L().Info("Address generation completed successfully",
			zap.Int("total_addresses_created", totalAddresses))
	}
}

func runInit(ctx context.Context, services *common.Services) {
	zap.L().Info("Initializing database and generating addresses")

	zap.L().Info("Setting up SQLite database")

	zap.L().Info("Generating addresses")
	generateAddresses(ctx, services)

	zap.L().Info("Initialization complete")
}

func main() {
	ctx := context.Background()

	_, loggerCleanup := common.InitializeLogger()
	defer loggerCleanup()

	initFlag := flag.Bool("init", false, "Initialize the database")
	flag.Parse()

	// Initialize services at top level
	cfg, err := config.Load()
	if err != nil {
		zap.L().Fatal("Failed to load config", zap.Error(err))
	}

	services, err := common.InitializeServices(ctx, cfg)
	if err != nil {
		zap.L().Fatal("Failed to initialize services", zap.Error(err))
	}
	defer services.Close()

	if *initFlag {
		runInit(ctx, services)
		return
	}

	generateAddresses(ctx, services)
}
