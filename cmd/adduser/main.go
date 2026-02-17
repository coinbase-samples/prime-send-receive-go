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
	"os"
	"regexp"
	"strings"

	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"
	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/store"

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

	storedAddress, err := services.DbService.StoreAddress(ctx, store.StoreAddressParams{
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

	// Get or create wallet
	walletId, err := getOrCreateWallet(ctx, services, assetConfig.Symbol)
	if err != nil {
		zap.L().Error("Failed to get or create wallet",
			zap.String("asset", assetConfig.Symbol),
			zap.Error(err))
		fmt.Printf("✗ %s-%s: Failed to get wallet\n", assetConfig.Symbol, assetConfig.Network)
		return result
	}

	// Always sync ALL addresses from Prime to ensure local store metadata is up to date.
	primeAddresses, primeErr := services.PrimeService.ListWalletAddresses(ctx, services.DefaultPortfolio.Id, walletId, assetConfig.Network)
	if primeErr == nil && len(primeAddresses) > 0 {
		var lastAddr string
		for _, addr := range primeAddresses {
			_, storeErr := services.DbService.StoreAddress(ctx, store.StoreAddressParams{
				UserId:            userId,
				Asset:             assetConfig.Symbol,
				Network:           assetConfig.Network,
				Address:           addr.Address,
				WalletId:          walletId,
				AccountIdentifier: addr.Id,
			})
			if storeErr != nil {
				zap.L().Warn("Failed to store synced address",
					zap.String("address", addr.Address), zap.Error(storeErr))
			} else {
				lastAddr = addr.Address
			}
		}
		if lastAddr != "" {
			label := "synced"
			if !exists {
				label = "imported from Prime"
			}
			fmt.Printf("✓ %s-%s: %d addresses (%s)\n", assetConfig.Symbol, assetConfig.Network, len(primeAddresses), label)
			result.success = true
			result.address = lastAddr
			return result
		}
	}

	// If local store already has it and Prime sync failed/empty, skip.
	if exists {
		fmt.Printf("✓ %s-%s: Address already exists\n", assetConfig.Symbol, assetConfig.Network)
		result.success = true
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

// assignExistingAddress verifies and assigns an existing Prime deposit address to a user.
func assignExistingAddress(ctx context.Context, services *common.Services, user *models.User, depositAddr string) {
	fmt.Printf("\n🔍 Verifying deposit address: %s\n", depositAddr)

	// 1. Check that no one already owns this address in the local store.
	existingUser, _, err := services.DbService.FindUserByAddress(ctx, depositAddr)
	if err == nil && existingUser != nil {
		if existingUser.Id == user.Id {
			fmt.Printf("✓ Address already assigned to this user\n")
			return
		}
		fmt.Printf("❌ Address already belongs to user %s (%s)\n", existingUser.Name, existingUser.Email)
		zap.L().Fatal("Deposit address already assigned to another user",
			zap.String("address", depositAddr),
			zap.String("owner_id", existingUser.Id),
			zap.String("owner_email", existingUser.Email))
	}

	// 2. Verify the address exists on a Prime wallet and find which wallet/asset.
	fmt.Println("🔍 Searching Prime wallets for this address...")
	allWallets, err := services.PrimeService.ListWallets(ctx, services.DefaultPortfolio.Id, "TRADING", nil)
	if err != nil {
		zap.L().Fatal("Failed to list Prime wallets", zap.Error(err))
	}

	// Known networks to search across.
	networks := []string{"ethereum-mainnet", "base-mainnet", "bitcoin-mainnet", "solana-mainnet", "polygon-mainnet", "arbitrum-mainnet", "avalanche-mainnet"}

	var foundWallet *models.Wallet
	var foundNetwork string
	var foundAddr *models.DepositAddress

	for i := range allWallets {
		w := &allWallets[i]
		for _, network := range networks {
			addrs, listErr := services.PrimeService.ListWalletAddresses(ctx, services.DefaultPortfolio.Id, w.Id, network)
			if listErr != nil {
				continue
			}
			for j := range addrs {
				if strings.EqualFold(addrs[j].Address, depositAddr) {
					foundWallet = w
					foundNetwork = network
					foundAddr = &addrs[j]
					break
				}
			}
			if foundAddr != nil {
				break
			}
		}
		if foundAddr != nil {
			break
		}
	}

	if foundAddr == nil {
		fmt.Printf("❌ Address not found on any Prime trading wallet\n")
		zap.L().Fatal("Deposit address not found on Prime portfolio",
			zap.String("address", depositAddr))
	}

	fmt.Printf("✓ Found on Prime: %s wallet (%s) on %s\n", foundWallet.Symbol, foundWallet.Id[:12]+"...", foundNetwork)

	// 3. Store the address mapping.
	_, err = services.DbService.StoreAddress(ctx, store.StoreAddressParams{
		UserId:            user.Id,
		Asset:             foundWallet.Symbol,
		Network:           foundNetwork,
		Address:           foundAddr.Address,
		WalletId:          foundWallet.Id,
		AccountIdentifier: foundAddr.Id,
	})
	if err != nil {
		zap.L().Fatal("Failed to store address", zap.Error(err))
	}

	fmt.Printf("✓ Address assigned to %s (%s)\n\n", user.Name, user.Email)
	zap.L().Info("Deposit address assigned to user",
		zap.String("user_id", user.Id),
		zap.String("address", depositAddr),
		zap.String("asset", foundWallet.Symbol),
		zap.String("network", foundNetwork))
}

// registerDestinationAddress stores an external withdrawal address. Looks up the
// Prime address book to determine the asset, then stores with the correct symbol.
func registerDestinationAddress(ctx context.Context, services *common.Services, user *models.User, destAddr string) {
	// Check if already assigned to someone.
	existingUser, _, err := services.DbService.FindUserByAddress(ctx, destAddr)
	if err == nil && existingUser != nil {
		if existingUser.Id == user.Id {
			fmt.Printf("  ✓ %s (already registered)\n", destAddr)
			return
		}
		fmt.Printf("  ❌ %s -- already belongs to %s (%s)\n", destAddr, existingUser.Name, existingUser.Email)
		return
	}

	// Look up the address in Prime's address book to get the asset symbol.
	asset := "WITHDRAWAL" // default if not found in address book
	fmt.Printf("  🔍 Looking up %s in Prime address book...\n", destAddr)

	entry, lookupErr := services.PrimeService.LookupAddressBook(ctx, services.DefaultPortfolio.Id, destAddr)
	if lookupErr != nil {
		zap.L().Debug("Address book lookup failed, using generic type",
			zap.String("address", destAddr), zap.Error(lookupErr))
	} else if entry != nil {
		asset = entry.Symbol
		fmt.Printf("  ✓ Found in address book: %s (%s, state: %s)\n", entry.Name, entry.Symbol, entry.State)
	} else {
		fmt.Printf("  ⚠ Not found in Prime address book, registering as generic withdrawal address\n")
	}

	_, err = services.DbService.StoreAddress(ctx, store.StoreAddressParams{
		UserId:  user.Id,
		Asset:   asset,
		Network: "external",
		Address: destAddr,
	})
	if err != nil {
		fmt.Printf("  ❌ %s -- failed to register: %v\n", destAddr, err)
		return
	}

	fmt.Printf("  ✓ %s registered (%s)\n", destAddr, asset)
	zap.L().Info("Withdrawal address registered",
		zap.String("user_id", user.Id),
		zap.String("address", destAddr),
		zap.String("asset", asset))
}

func main() {
	ctx := context.Background()

	_, loggerCleanup := common.InitializeLogger()
	defer loggerCleanup()

	// Parse command line flags
	nameFlag := flag.String("name", "", "User's full name (required)")
	emailFlag := flag.String("email", "", "User's email address (required)")
	depositAddrsFlag := flag.String("deposit-addresses", "", "Comma-separated existing Prime deposit addresses to assign (optional)")
	destAddrsFlag := flag.String("withdrawal-addresses", "", "Comma-separated external withdrawal addresses for matching outgoing transactions (optional)")
	flag.Parse()

	// Check for stray positional args (common when comma-separated values have spaces).
	if flag.NArg() > 0 {
		fmt.Println("ERROR: Unexpected arguments detected:", flag.Args())
		fmt.Println()
		fmt.Println("If using comma-separated addresses, wrap in quotes:")
		fmt.Println(`  --deposit-addresses "0xABC...,0xDEF..."`)
		fmt.Println(`  --withdrawal-addresses "0x123...,0x456..."`)
		os.Exit(1)
	}

	// Validate required flags
	if *nameFlag == "" || *emailFlag == "" {
		fmt.Println("Usage: adduser --name \"Full Name\" --email user@example.com [options]")
		fmt.Println()
		fmt.Println("Options:")
		fmt.Println(`  --deposit-addresses "addr1,addr2"      Existing Prime deposit addresses`)
		fmt.Println(`  --withdrawal-addresses "addr1,addr2"   External withdrawal addresses`)
		os.Exit(1)
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

	// Check if a user with this email already exists -- reuse if so.
	user, err := services.DbService.GetUserByEmail(ctx, *emailFlag)
	if err == nil && user != nil {
		fmt.Println()
		common.PrintHeader("EXISTING USER FOUND", common.DefaultWidth)
		fmt.Printf("ID:    %s\n", user.Id)
		fmt.Printf("Name:  %s\n", user.Name)
		fmt.Printf("Email: %s\n", user.Email)
		common.PrintSeparator("=", common.DefaultWidth)
		fmt.Println()

		zap.L().Info("User already exists, continuing with address setup",
			zap.String("id", user.Id), zap.String("email", user.Email))
	} else {
		// Create new user with a fresh UUID.
		userId := uuid.New().String()

		zap.L().Info("Creating user",
			zap.String("id", userId),
			zap.String("name", *nameFlag),
			zap.String("email", *emailFlag))

		user, err = services.DbService.CreateUser(ctx, userId, *nameFlag, *emailFlag)
		if err != nil {
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
	}

	// If --deposit-addresses was provided, assign each one to this user.
	hasExplicitDeposits := *depositAddrsFlag != ""
	if hasExplicitDeposits {
		for _, addr := range strings.Split(*depositAddrsFlag, ",") {
			addr = strings.TrimSpace(addr)
			if addr != "" {
				assignExistingAddress(ctx, services, user, addr)
			}
		}
	}

	// If --withdrawal-addresses was provided, register them for withdrawal matching.
	if *destAddrsFlag != "" {
		fmt.Println("Registering withdrawal addresses...")
		for _, addr := range strings.Split(*destAddrsFlag, ",") {
			addr = strings.TrimSpace(addr)
			if addr != "" {
				registerDestinationAddress(ctx, services, user, addr)
			}
		}
	}

	// Only auto-generate addresses from assets.yaml if no --deposit-addresses were specified.
	if hasExplicitDeposits {
		fmt.Println("\nDeposit addresses specified -- skipping automatic address generation.")
		fmt.Println("Run 'go run cmd/setup/main.go' to discover and sync all addresses from Prime.")
		return
	}

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
