package main

import (
	"context"
	"flag"
	"fmt"

	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"

	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()

	logger, loggerCleanup := common.InitializeLogger()
	defer loggerCleanup()

	// Parse command line flags
	emailFlag := flag.String("email", "", "Filter by specific user email (optional)")
	flag.Parse()

	logger.Info("Starting address query")

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

	var users []struct {
		Id    string
		Name  string
		Email string
	}

	// Get users
	if *emailFlag != "" {
		// Query specific user by email
		logger.Info("Looking up user by email", zap.String("email", *emailFlag))
		user, err := dbService.GetUserByEmail(ctx, *emailFlag)
		if err != nil {
			logger.Fatal("User not found", zap.String("email", *emailFlag), zap.Error(err))
		}
		users = append(users, struct {
			Id    string
			Name  string
			Email string
		}{
			Id:    user.Id,
			Name:  user.Name,
			Email: user.Email,
		})
	} else {
		// Get all users
		allUsers, err := dbService.GetUsers(ctx)
		if err != nil {
			logger.Fatal("Failed to get users", zap.Error(err))
		}
		for _, u := range allUsers {
			users = append(users, struct {
				Id    string
				Name  string
				Email string
			}{
				Id:    u.Id,
				Name:  u.Name,
				Email: u.Email,
			})
		}
	}

	logger.Info("Retrieved users", zap.Int("count", len(users)))

	// Track totals
	totalUsers := 0
	totalAddresses := 0
	usersWithAddresses := 0

	// Print header
	common.PrintHeader("DEPOSIT ADDRESSES REPORT", common.WideWidth)

	// Process each user
	for _, user := range users {
		totalUsers++

		// Get all addresses for this user
		addresses, err := dbService.GetAllUserAddresses(ctx, user.Id)
		if err != nil {
			logger.Error("Failed to get addresses for user",
				zap.String("user_id", user.Id),
				zap.String("user_name", user.Name),
				zap.Error(err))
			continue
		}

		// Skip users with no addresses
		if len(addresses) == 0 {
			continue
		}

		usersWithAddresses++
		totalAddresses += len(addresses)

		// Print user header
		fmt.Printf("\n┌─ User: %s (%s)\n", user.Name, user.Email)
		fmt.Printf("│  ID: %s\n", user.Id)
		fmt.Printf("│  Addresses: %d\n", len(addresses))
		common.PrintBoxSeparator(98)

		// Print each address
		for i, addr := range addresses {
			isLast := i == len(addresses)-1
			symbol := common.BoxPrefix(isLast)

			// Format: Asset-Network (e.g., "ETH-ethereum-mainnet")
			assetNetwork := fmt.Sprintf("%s-%s", addr.Asset, addr.Network)
			fmt.Printf("%s %-30s → %s\n",
				symbol,
				assetNetwork,
				addr.Address)

			// Add extra details on next line if account identifier differs
			if addr.AccountIdentifier != "" && addr.AccountIdentifier != addr.Address {
				detailSymbol := common.BoxDetailPrefix(isLast)
				fmt.Printf("%s   Account ID: %s\n", detailSymbol, addr.AccountIdentifier)
			}
		}
	}

	// Print footer summary
	summary := fmt.Sprintf("SUMMARY: %d users with addresses (%d total addresses across %d users queried)",
		usersWithAddresses, totalAddresses, totalUsers)
	common.PrintFooter(summary, common.WideWidth)

	logger.Info("Address query completed",
		zap.Int("users_queried", totalUsers),
		zap.Int("users_with_addresses", usersWithAddresses),
		zap.Int("total_addresses", totalAddresses))
}
