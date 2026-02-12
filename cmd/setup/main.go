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
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"
	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/store"

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
	storedAddress, err := services.DbService.StoreAddress(ctx, store.StoreAddressParams{
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

// syncAddressesFromPrime fetches ALL addresses from Prime for this wallet/network
// and ensures each one is stored in the local backend.
func syncAddressesFromPrime(ctx context.Context, services *common.Services, user models.User, assetConfig common.AssetConfig, wallet *models.Wallet) (bool, error) {
	primeAddresses, err := services.PrimeService.ListWalletAddresses(ctx, services.DefaultPortfolio.Id, wallet.Id, assetConfig.Network)
	if err != nil {
		zap.L().Debug("Could not list addresses from Prime",
			zap.String("wallet_id", wallet.Id), zap.Error(err))
		return false, nil
	}
	if len(primeAddresses) == 0 {
		return false, nil
	}

	zap.L().Info("Syncing addresses from Prime to local store",
		zap.String("user_id", user.Id),
		zap.String("asset", assetConfig.Symbol),
		zap.String("network", assetConfig.Network),
		zap.Int("count", len(primeAddresses)))

	for _, addr := range primeAddresses {
		_, err = services.DbService.StoreAddress(ctx, store.StoreAddressParams{
			UserId:            user.Id,
			Asset:             assetConfig.Symbol,
			Network:           assetConfig.Network,
			Address:           addr.Address,
			WalletId:          wallet.Id,
			AccountIdentifier: addr.Id,
		})
		if err != nil {
			zap.L().Warn("Failed to sync address",
				zap.String("address", addr.Address), zap.Error(err))
		}
	}

	return true, nil
}

// processUserAsset processes a single user-asset combination.
// Always syncs with Prime to ensure local store metadata is up to date.
func processUserAsset(ctx context.Context, services *common.Services, user models.User, assetConfig common.AssetConfig) error {
	zap.L().Info("Processing asset",
		zap.String("user_id", user.Id),
		zap.String("asset", assetConfig.Symbol),
		zap.String("network", assetConfig.Network))

	// Get or create wallet.
	wallet, err := getOrCreateWallet(ctx, services, assetConfig.Symbol)
	if err != nil {
		return err
	}

	// Always check Prime and sync ALL addresses to local store.
	synced, err := syncAddressesFromPrime(ctx, services, user, assetConfig, wallet)
	if err != nil {
		return err
	}
	if synced {
		return nil
	}

	// Check local store -- if we have it but Prime doesn't (shouldn't happen normally).
	exists, err := checkExistingAddress(ctx, services, user, assetConfig)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	// Nothing on Prime or locally -- create a new address.
	return createAndStoreAddress(ctx, services, user, assetConfig, wallet)
}

// platformAccountID is a well-known user ID for unattributed deposits.
const platformAccountEmail = "platform@prime.internal"
const platformAccountName = "Prime Platform (Unattributed)"

// ensurePlatformAccount creates the platform catch-all account if it doesn't exist.
func ensurePlatformAccount(ctx context.Context, services *common.Services) *models.User {
	existing, err := services.DbService.GetUserByEmail(ctx, platformAccountEmail)
	if err == nil && existing != nil {
		return existing
	}

	userId := "prime-platform-" + services.DefaultPortfolio.Id
	user, err := services.DbService.CreateUser(ctx, userId, platformAccountName, platformAccountEmail)
	if err != nil {
		zap.L().Warn("Could not create platform account", zap.Error(err))
		// Try to fetch it in case of race.
		existing, _ = services.DbService.GetUserByEmail(ctx, platformAccountEmail)
		return existing
	}
	zap.L().Info("Created platform account for unattributed deposits",
		zap.String("id", user.Id))
	return user
}

// discoverAllWallets fetches ALL trading wallets from Prime (not filtered by assets.yaml).
func discoverAllWallets(ctx context.Context, services *common.Services) []models.Wallet {
	allWallets, err := services.PrimeService.ListWallets(ctx, services.DefaultPortfolio.Id, "TRADING", nil)
	if err != nil {
		zap.L().Error("Failed to list all wallets from Prime", zap.Error(err))
		return nil
	}

	zap.L().Info("Discovered wallets from Prime",
		zap.Int("count", len(allWallets)),
		zap.String("portfolio_id", services.DefaultPortfolio.Id))

	for _, w := range allWallets {
		zap.L().Info("  Wallet",
			zap.String("id", w.Id),
			zap.String("symbol", w.Symbol),
			zap.String("name", w.Name))
	}
	return allWallets
}

// syncWalletAddressesToUser syncs all addresses from a Prime wallet to a local user.
func syncWalletAddressesToUser(ctx context.Context, services *common.Services, user *models.User, wallet models.Wallet, network string) int {
	primeAddresses, err := services.PrimeService.ListWalletAddresses(ctx, services.DefaultPortfolio.Id, wallet.Id, network)
	if err != nil {
		zap.L().Debug("Could not list addresses for wallet/network",
			zap.String("wallet_id", wallet.Id),
			zap.String("network", network),
			zap.Error(err))
		return 0
	}

	count := 0
	for _, addr := range primeAddresses {
		_, err := services.DbService.StoreAddress(ctx, store.StoreAddressParams{
			UserId:            user.Id,
			Asset:             wallet.Symbol,
			Network:           network,
			Address:           addr.Address,
			WalletId:          wallet.Id,
			AccountIdentifier: addr.Id,
		})
		if err != nil {
			zap.L().Warn("Failed to store address",
				zap.String("address", addr.Address), zap.Error(err))
		} else {
			count++
		}
	}
	return count
}

// discoverAndSync discovers ALL wallets from Prime and syncs addresses.
// For users that exist locally, syncs their addresses.
// For wallets with addresses that aren't assigned to any user, assigns to the platform account.
func discoverAndSync(ctx context.Context, services *common.Services) {
	fmt.Println("Discovering all wallets from Prime...")
	allWallets := discoverAllWallets(ctx, services)
	if len(allWallets) == 0 {
		fmt.Println("No wallets found on Prime.")
		return
	}

	// Get local users.
	users, err := services.DbService.GetUsers(ctx)
	if err != nil {
		zap.L().Fatal("Failed to read users", zap.Error(err))
	}

	// Build a set of addresses already assigned to real users.
	assignedAddresses := make(map[string]bool)
	for _, user := range users {
		if user.Email == platformAccountEmail {
			continue
		}
		addrs, _ := services.DbService.GetAllUserAddresses(ctx, user.Id)
		for _, a := range addrs {
			assignedAddresses[strings.ToLower(a.Address)] = true
		}
	}

	// Ensure platform account exists for unattributed addresses.
	platformUser := ensurePlatformAccount(ctx, services)

	// Load known networks from assets.yaml (if exists) or use defaults.
	networks := []string{"ethereum-mainnet", "base-mainnet", "bitcoin-mainnet"}
	assetConfigs, err := common.LoadAssetConfig("assets.yaml")
	if err == nil {
		seen := make(map[string]bool)
		for _, ac := range assetConfigs {
			if !seen[ac.Network] {
				networks = append(networks, ac.Network)
				seen[ac.Network] = true
			}
		}
		// Deduplicate.
		unique := make(map[string]bool)
		var deduped []string
		for _, n := range networks {
			if !unique[n] {
				unique[n] = true
				deduped = append(deduped, n)
			}
		}
		networks = deduped
	}

	totalSynced := 0
	for _, wallet := range allWallets {
		for _, network := range networks {
			// First sync to existing real users.
			syncedToUsers := 0
			for _, user := range users {
				if user.Email == platformAccountEmail {
					continue
				}
				syncedToUsers += syncWalletAddressesToUser(ctx, services, &user, wallet, network)
			}

			// Fetch all addresses on this wallet/network from Prime.
			primeAddresses, err := services.PrimeService.ListWalletAddresses(ctx, services.DefaultPortfolio.Id, wallet.Id, network)
			if err != nil || len(primeAddresses) == 0 {
				continue
			}

			// Any addresses not assigned to a real user go to the platform account.
			if platformUser != nil {
				for _, addr := range primeAddresses {
					if !assignedAddresses[strings.ToLower(addr.Address)] {
						_, storeErr := services.DbService.StoreAddress(ctx, store.StoreAddressParams{
							UserId:            platformUser.Id,
							Asset:             wallet.Symbol,
							Network:           network,
							Address:           addr.Address,
							WalletId:          wallet.Id,
							AccountIdentifier: addr.Id,
						})
						if storeErr == nil {
							totalSynced++
							zap.L().Info("Assigned unattributed address to platform account",
								zap.String("address", addr.Address),
								zap.String("asset", wallet.Symbol),
								zap.String("network", network))
						}
					}
				}
			}

			totalSynced += syncedToUsers
		}
	}

	fmt.Printf("\nDiscovery complete: %d wallets, %d addresses synced\n", len(allWallets), totalSynced)
}

func generateAddresses(ctx context.Context, services *common.Services) {
	// Phase 1: Discover ALL wallets from Prime and sync/assign addresses.
	discoverAndSync(ctx, services)

	// Phase 2: For existing users, ensure addresses exist for all assets in assets.yaml.
	assetConfigs, err := common.LoadAssetConfig("assets.yaml")
	if err != nil {
		zap.L().Info("No assets.yaml found, skipping user-specific address generation", zap.Error(err))
		return
	}

	users, err := services.DbService.GetUsers(ctx)
	if err != nil {
		zap.L().Fatal("Failed to read users", zap.Error(err))
	}

	var totalAddresses, failedAddresses int
	for _, user := range users {
		if user.Email == platformAccountEmail {
			continue // skip platform account for user-specific generation
		}
		for _, assetConfig := range assetConfigs {
			err := processUserAsset(ctx, services, user, assetConfig)
			if err != nil {
				failedAddresses++
			} else {
				totalAddresses++
			}
		}
	}

	if failedAddresses > 0 {
		zap.L().Warn("Address generation completed with some failures",
			zap.Int("total", totalAddresses),
			zap.Int("failed", failedAddresses))
	} else {
		zap.L().Info("Address generation completed", zap.Int("total", totalAddresses))
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
