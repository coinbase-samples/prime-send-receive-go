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
	"regexp"
	"strings"

	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"
	"prime-send-receive-go/internal/database"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)

type addressGenerationResult struct {
	success      bool
	assetSymbol  string
	assetNetwork string
	address      string
}

type generationStats struct {
	successCount int
	failedAssets []string
}

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

func checkExistingAddress(ctx context.Context, services *common.Services, userId string, assetConfig common.AssetConfig) (bool, error) {
	existingAddresses, err := services.DbService.GetAddresses(ctx, userId, assetConfig.Symbol, assetConfig.Network)
	if err != nil {
		return false, fmt.Errorf("error checking existing addresses: %w", err)
	}
	return len(existingAddresses) > 0, nil
}

func getOrCreateWallet(ctx context.Context, services *common.Services, assetSymbol string) (string, error) {
	wallets, err := services.PrimeService.ListWallets(ctx, services.DefaultPortfolio.Id, "TRADING", []string{assetSymbol})
	if err != nil {
		return "", fmt.Errorf("error listing wallets: %w", err)
	}

	if len(wallets) > 0 {
		walletId := wallets[0].Id
		zap.L().Info("Using existing wallet",
			zap.String("asset", assetSymbol),
			zap.String("wallet_id", walletId))
		return walletId, nil
	}

	walletName := fmt.Sprintf("%s Trading Wallet", assetSymbol)
	zap.L().Info("Creating new wallet",
		zap.String("asset", assetSymbol),
		zap.String("wallet_name", walletName))

	newWallet, err := services.PrimeService.CreateWallet(ctx, services.DefaultPortfolio.Id, walletName, assetSymbol, "TRADING")
	if err != nil {
		return "", fmt.Errorf("error creating wallet: %w", err)
	}

	return newWallet.Id, nil
}

func generateAndStoreAddress(ctx context.Context, services *common.Services, userId string, assetConfig common.AssetConfig, walletId string) (string, error) {
	zap.L().Info("Creating deposit address",
		zap.String("asset", assetConfig.Symbol),
		zap.String("network", assetConfig.Network),
		zap.String("wallet_id", walletId))

	depositAddress, err := services.PrimeService.CreateDepositAddress(ctx, services.DefaultPortfolio.Id, walletId, assetConfig.Symbol, assetConfig.Network)
	if err != nil {
		return "", fmt.Errorf("error creating deposit address: %w", err)
	}

	storedAddress, err := services.DbService.StoreAddress(ctx, database.StoreAddressParams{
		UserId:            userId,
		Asset:             assetConfig.Symbol,
		Network:           assetConfig.Network,
		Address:           depositAddress.Address,
		WalletId:          walletId,
		AccountIdentifier: depositAddress.Id,
	})
	if err != nil {
		return "", fmt.Errorf("error storing address to database: %w", err)
	}

	return storedAddress.Address, nil
}

func processAsset(ctx context.Context, services *common.Services, userId string, assetConfig common.AssetConfig) addressGenerationResult {
	zap.L().Info("Processing asset",
		zap.String("asset", assetConfig.Symbol),
		zap.String("network", assetConfig.Network))

	result := addressGenerationResult{
		assetSymbol:  assetConfig.Symbol,
		assetNetwork: assetConfig.Network,
	}

	// Check if address already exists
	exists, err := checkExistingAddress(ctx, services, userId, assetConfig)
	if err != nil {
		zap.L().Error("Failed to check existing addresses",
			zap.String("asset", assetConfig.Symbol),
			zap.Error(err))
		return result
	}

	if exists {
		fmt.Printf("✓ %s-%s: Address already exists\n", assetConfig.Symbol, assetConfig.Network)
		result.success = true
		return result
	}

	// Get or create wallet
	walletId, err := getOrCreateWallet(ctx, services, assetConfig.Symbol)
	if err != nil {
		zap.L().Error("Failed to get or create wallet",
			zap.String("asset", assetConfig.Symbol),
			zap.Error(err))
		fmt.Printf("✗ %s-%s: Failed to get wallet\n", assetConfig.Symbol, assetConfig.Network)
		return result
	}

	// Generate and store address
	address, err := generateAndStoreAddress(ctx, services, userId, assetConfig, walletId)
	if err != nil {
		zap.L().Error("Failed to generate or store address",
			zap.String("asset", assetConfig.Symbol),
			zap.Error(err))
		fmt.Printf("✗ %s-%s: Failed to create address\n", assetConfig.Symbol, assetConfig.Network)
		return result
	}

	fmt.Printf("✓ %s-%s: %s\n", assetConfig.Symbol, assetConfig.Network, address)
	result.success = true
	result.address = address
	return result
}

func generateAddressesForUser(ctx context.Context, services *common.Services, userId string, assetConfigs []common.AssetConfig) generationStats {
	fmt.Printf("Generating deposit addresses for %d assets...\n\n", len(assetConfigs))

	stats := generationStats{
		failedAssets: []string{},
	}

	for _, assetConfig := range assetConfigs {
		result := processAsset(ctx, services, userId, assetConfig)

		if result.success {
			stats.successCount++
		} else {
			stats.failedAssets = append(stats.failedAssets, assetConfig.Symbol)
		}
	}

	return stats
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
	stats := generateAddressesForUser(ctx, services, user.Id, assetConfigs)

	// Print summary
	fmt.Println()
	common.PrintHeader("ADDRESS GENERATION SUMMARY", common.DefaultWidth)
	fmt.Printf("Total Assets:      %d\n", len(assetConfigs))
	fmt.Printf("Successful:        %d\n", stats.successCount)
	fmt.Printf("Failed:            %d\n", len(stats.failedAssets))
	if len(stats.failedAssets) > 0 {
		fmt.Printf("Failed Assets:     %s\n", strings.Join(stats.failedAssets, ", "))
	}
	common.PrintSeparator("=", common.DefaultWidth)
	fmt.Println()

	if len(stats.failedAssets) > 0 {
		zap.L().Warn("User created but some addresses failed to generate",
			zap.String("user_id", user.Id),
			zap.Int("successful", stats.successCount),
			zap.Int("failed", len(stats.failedAssets)),
			zap.Strings("failed_assets", stats.failedAssets))
		fmt.Println("User created successfully but some deposit addresses failed to generate")
		fmt.Println("You can re-run setup to retry: go run cmd/setup/main.go")
	} else {
		zap.L().Info("User and all addresses created successfully",
			zap.String("user_id", user.Id),
			zap.Int("addresses_created", stats.successCount))
		fmt.Println("User and all deposit addresses created successfully!")
	}
}
