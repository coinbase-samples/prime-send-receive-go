package main

import (
	"context"
	"flag"
	"fmt"

	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"
	"prime-send-receive-go/internal/database"
	"prime-send-receive-go/internal/models"

	"go.uber.org/zap"
)

type reportStats struct {
	totalUsers         int
	totalAddresses     int
	usersWithAddresses int
}

func printUserHeader(user common.UserInfo, addressCount int) {
	fmt.Printf("\n┌─ User: %s (%s)\n", user.Name, user.Email)
	fmt.Printf("│  ID: %s\n", user.Id)
	fmt.Printf("│  Addresses: %d\n", addressCount)
	common.PrintBoxSeparator(98)
}

func printAddress(addr models.Address, isLast bool) {
	symbol := common.BoxPrefix(isLast)
	assetNetwork := fmt.Sprintf("%s-%s", addr.Asset, addr.Network)
	fmt.Printf("%s %-30s → %s\n", symbol, assetNetwork, addr.Address)

	if shouldPrintAccountIdentifier(addr) {
		detailSymbol := common.BoxDetailPrefix(isLast)
		fmt.Printf("%s   Account ID: %s\n", detailSymbol, addr.AccountIdentifier)
	}
}

func shouldPrintAccountIdentifier(addr models.Address) bool {
	return addr.AccountIdentifier != "" && addr.AccountIdentifier != addr.Address
}

func printAddresses(addresses []models.Address) {
	for i, addr := range addresses {
		isLast := i == len(addresses)-1
		printAddress(addr, isLast)
	}
}

func processUser(ctx context.Context, user common.UserInfo, dbService *database.Service, logger *zap.Logger) (int, error) {
	addresses, err := dbService.GetAllUserAddresses(ctx, user.Id)
	if err != nil {
		return 0, fmt.Errorf("failed to get addresses: %w", err)
	}

	if len(addresses) == 0 {
		return 0, nil
	}

	printUserHeader(user, len(addresses))
	printAddresses(addresses)

	return len(addresses), nil
}

func processUsersAndGenerateReport(ctx context.Context, users []common.UserInfo, dbService *database.Service, logger *zap.Logger) reportStats {
	stats := reportStats{}

	for _, user := range users {
		stats.totalUsers++

		addressCount, err := processUser(ctx, user, dbService, logger)
		if err != nil {
			logger.Error("Failed to process user",
				zap.String("user_id", user.Id),
				zap.String("user_name", user.Name),
				zap.Error(err))
			continue
		}

		if addressCount > 0 {
			stats.usersWithAddresses++
			stats.totalAddresses += addressCount
		}
	}

	return stats
}

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

	users, err := common.InitializeUsers(ctx, dbService, *emailFlag, logger)
	if err != nil {
		logger.Fatal("Failed to initialize users", zap.Error(err))
	}

	// Print header
	common.PrintHeader("DEPOSIT ADDRESSES REPORT", common.WideWidth)

	// Process users and generate report
	stats := processUsersAndGenerateReport(ctx, users, dbService, logger)

	// Print footer summary
	summary := fmt.Sprintf("SUMMARY: %d users with addresses (%d total addresses across %d users queried)",
		stats.usersWithAddresses, stats.totalAddresses, stats.totalUsers)
	common.PrintFooter(summary, common.WideWidth)

	logger.Info("Address query completed",
		zap.Int("users_queried", stats.totalUsers),
		zap.Int("users_with_addresses", stats.usersWithAddresses),
		zap.Int("total_addresses", stats.totalAddresses))
}
