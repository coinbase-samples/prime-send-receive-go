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

	logger.Info("Starting balance query")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}

	// Initialize database service (no need for Prime API for read-only operations)
	logger.Info("Connecting to database", zap.String("path", cfg.Database.Path))
	dbService, err := common.InitializeDatabaseOnly(ctx, logger, cfg)
	if err != nil {
		logger.Fatal("Failed to initialize database", zap.Error(err))
	}
	defer dbService.Close()

	var users []struct {
		Id    string
		Name  string
		Email string
	}

	// Get users - either specific user by email or all users
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
	totalBalances := 0
	usersWithBalances := 0

	// Print header
	common.PrintHeader("USER BALANCE REPORT", common.DefaultWidth)

	// Process each user
	for _, user := range users {
		totalUsers++

		// Get all balances for this user
		balances, err := dbService.GetAllUserBalances(ctx, user.Id)
		if err != nil {
			logger.Error("Failed to get balances for user",
				zap.String("user_id", user.Id),
				zap.String("user_name", user.Name),
				zap.Error(err))
			continue
		}

		// Skip users with no balances
		if len(balances) == 0 {
			continue
		}

		usersWithBalances++
		totalBalances += len(balances)

		// Print user header
		fmt.Printf("\n┌─ User: %s (%s)\n", user.Name, user.Email)
		fmt.Printf("│  ID: %s\n", user.Id)
		fmt.Printf("│  Assets: %d\n", len(balances))
		common.PrintBoxSeparator(78)

		// Print each balance
		for i, balance := range balances {
			lastTx := balance.LastTransactionId
			if lastTx == "" {
				lastTx = "none"
			} else if len(lastTx) > 8 {
				lastTx = lastTx[:8] + "..."
			}

			isLast := i == len(balances)-1
			symbol := common.BoxPrefix(isLast)

			fmt.Printf("%s %-15s: %20s (v%d, last_tx: %s, updated: %s)\n",
				symbol,
				balance.Asset,
				balance.Balance.String(),
				balance.Version,
				lastTx,
				balance.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
	}

	// Print footer summary
	summary := fmt.Sprintf("SUMMARY: %d users with balances (%d total balances across %d users queried)",
		usersWithBalances, totalBalances, totalUsers)
	common.PrintFooter(summary, common.DefaultWidth)

	logger.Info("Balance query completed",
		zap.Int("users_queried", totalUsers),
		zap.Int("users_with_balances", usersWithBalances),
		zap.Int("total_balances", totalBalances))
}
