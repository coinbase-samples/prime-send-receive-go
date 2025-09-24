package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/prime"

	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

type AssetConfig struct {
	Symbol  string `yaml:"symbol"`
	Network string `yaml:"network"`
}

type AssetsConfig struct {
	Assets []AssetConfig `yaml:"assets"`
}

func loadAssetConfig() ([]AssetConfig, error) {
	data, err := os.ReadFile("assets.yaml")
	if err != nil {
		return nil, fmt.Errorf("unable to read assets.yaml: %v", err)
	}

	var config AssetsConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("unable to parse assets.yaml: %v", err)
	}

	for i, asset := range config.Assets {
		if asset.Symbol == "" {
			return nil, fmt.Errorf("asset at index %d missing symbol", i)
		}
		if asset.Network == "" {
			return nil, fmt.Errorf("asset at index %d missing network", i)
		}
	}

	return config.Assets, nil
}

func runInit(ctx context.Context, logger *zap.Logger) {
	logger.Info("🚀 Initializing database and generating addresses")

	logger.Info("📊 Setting up SQLite database")

	logger.Info("🏦 Generating addresses")
	generateAddresses(ctx, logger)

	logger.Info("✅ Initialization complete")
}

func main() {
	ctx := context.Background()

	logger, loggerCleanup := common.InitializeLogger()
	defer loggerCleanup()

	initFlag := flag.Bool("init", false, "Initialize the database")
	flag.Parse()

	if *initFlag {
		runInit(ctx, logger)
		return
	}

	generateAddresses(ctx, logger)
}

func generateAddresses(ctx context.Context, logger *zap.Logger) {
	logger.Info("Loading asset configuration")
	assetConfigs, err := loadAssetConfig()
	if err != nil {
		logger.Fatal("Failed to load asset config", zap.Error(err))
	}
	logger.Info("Asset configuration loaded", zap.Int("count", len(assetConfigs)))

	services, err := common.InitializeServices(ctx, logger, "addresses.db")
	if err != nil {
		logger.Fatal("Failed to initialize services", zap.Error(err))
	}
	defer services.Close()

	users, err := services.DBService.GetUsers(ctx)
	if err != nil {
		logger.Fatal("Failed to read users from database", zap.Error(err))
	}

	for _, user := range users {
		logger.Info("Processing user",
			zap.String("id", user.Id),
			zap.String("name", user.Name),
			zap.String("email", user.Email))

		for _, assetConfig := range assetConfigs {
			logger.Info("Processing asset",
				zap.String("user_id", user.Id),
				zap.String("asset", assetConfig.Symbol),
				zap.String("network", assetConfig.Network))

			existingAddresses, err := services.DBService.GetAddresses(ctx, user.Id, assetConfig.Symbol)
			if err != nil {
				logger.Error("Error checking existing addresses",
					zap.String("user_id", user.Id),
					zap.String("asset", assetConfig.Symbol),
					zap.Error(err))
				continue
			}

			if len(existingAddresses) > 0 {
				logger.Info("User already has addresses for asset",
					zap.String("user_id", user.Id),
					zap.String("asset", assetConfig.Symbol),
					zap.Int("count", len(existingAddresses)),
					zap.String("latest_address", existingAddresses[0].Address))
				continue
			}

			logger.Debug("Listing wallets for asset", zap.String("asset", assetConfig.Symbol))
			wallets, err := services.PrimeService.ListWallets(ctx, services.DefaultPortfolio.Id, "TRADING", []string{assetConfig.Symbol})
			if err != nil {
				logger.Error("Error listing wallets",
					zap.String("asset", assetConfig.Symbol),
					zap.Error(err))
				continue
			}

			var targetWallet *prime.Wallet
			if len(wallets) > 0 {
				targetWallet = &wallets[0]
				logger.Info("Using existing wallet",
					zap.String("asset", assetConfig.Symbol),
					zap.String("wallet_name", targetWallet.Name),
					zap.String("wallet_id", targetWallet.Id))
			} else {
				walletName := fmt.Sprintf("%s Trading Wallet", assetConfig.Symbol)
				logger.Info("Creating new wallet",
					zap.String("asset", assetConfig.Symbol),
					zap.String("wallet_name", walletName))
				newWallet, err := services.PrimeService.CreateWallet(ctx, services.DefaultPortfolio.Id, walletName, assetConfig.Symbol, "TRADING")
				if err != nil {
					logger.Error("Error creating wallet",
						zap.String("asset", assetConfig.Symbol),
						zap.Error(err))
					continue
				}
				targetWallet = newWallet
				logger.Info("Created new wallet",
					zap.String("asset", assetConfig.Symbol),
					zap.String("wallet_name", targetWallet.Name),
					zap.String("wallet_id", targetWallet.Id))
			}
			logger.Info("Creating deposit address",
				zap.String("asset", assetConfig.Symbol),
				zap.String("network", assetConfig.Network),
				zap.String("wallet_id", targetWallet.Id))
			depositAddress, err := services.PrimeService.CreateDepositAddress(ctx, services.DefaultPortfolio.Id, targetWallet.Id, assetConfig.Symbol, assetConfig.Network)
			if err != nil {
				logger.Error("Error creating deposit address",
					zap.String("asset", assetConfig.Symbol),
					zap.String("network", assetConfig.Network),
					zap.Error(err))
				continue
			}

			logger.Info("Created deposit address",
				zap.String("asset", assetConfig.Symbol),
				zap.String("network", assetConfig.Network),
				zap.String("address", depositAddress.Address))

			storedAddress, err := services.DBService.StoreAddress(ctx, user.Id, assetConfig.Symbol, assetConfig.Network, depositAddress.Address, targetWallet.Id, depositAddress.Id)
			if err != nil {
				logger.Error("Error storing address to database",
					zap.String("asset", assetConfig.Symbol),
					zap.String("address", depositAddress.Address),
					zap.Error(err))
			} else {
				logger.Info("Stored address to database",
					zap.String("id", storedAddress.Id),
					zap.String("asset", assetConfig.Symbol),
					zap.String("address", depositAddress.Address))
			}

			addressOutput, err := json.MarshalIndent(depositAddress, "", "  ")
			if err != nil {
				logger.Error("Error marshaling address to JSON", zap.Error(err))
			} else {
				logger.Debug("Address details", zap.String("json", string(addressOutput)))
			}
		}
	}

	logger.Info("Address generation complete")
}
