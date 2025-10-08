package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"prime-send-receive-go/internal/database"
	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/prime"

	"github.com/coinbase-samples/prime-sdk-go/credentials"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

// init loads environment variables from .env file if it exists
func init() {
	// Try to load .env file - if it doesn't exist, that's okay
	// Environment variables can be set via other means (shell export, docker, etc.)
	if err := godotenv.Load(); err != nil {
		// Only log if the file exists but couldn't be read
		// (godotenv returns an error if .env doesn't exist)
		log.Printf("Note: No .env file found or unable to load it: %v\n", err)
		log.Println("Make sure to set environment variables via export or other means")
	} else {
		log.Println("✓ Loaded environment variables from .env file")
	}
}

type Services struct {
	Logger           *zap.Logger
	DbService        *database.Service
	PrimeService     *prime.Service
	DefaultPortfolio *models.Portfolio
}

func InitializeLogger() (*zap.Logger, func()) {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	cleanup := func() {
		if err := logger.Sync(); err != nil {
			if !isIgnorableSyncError(err) {
				log.Printf("Failed to sync logger: %v\n", err)
			}
		}
	}

	return logger, cleanup
}

func InitializeServices(ctx context.Context, logger *zap.Logger, cfg *models.Config) (*Services, error) {
	dbService, err := database.NewService(ctx, logger, cfg.Database)
	if err != nil {
		return nil, err
	}

	logger.Info("Loading Prime API credentials")
	creds, err := loadPrimeCredentials()
	if err != nil {
		dbService.Close()
		return nil, err
	}

	primeService, err := prime.NewService(creds, logger)
	if err != nil {
		dbService.Close()
		return nil, err
	}

	logger.Info("Finding default portfolio")
	defaultPortfolio, err := primeService.FindDefaultPortfolio(ctx)
	if err != nil {
		dbService.Close()
		return nil, err
	}
	logger.Info("Using default portfolio",
		zap.String("name", defaultPortfolio.Name),
		zap.String("id", defaultPortfolio.Id))

	return &Services{
		Logger:           logger,
		DbService:        dbService,
		PrimeService:     primeService,
		DefaultPortfolio: defaultPortfolio,
	}, nil
}

// InitializeDatabaseOnly initializes just the database service without Prime API
// Useful for read-only operations like querying balances
func InitializeDatabaseOnly(ctx context.Context, logger *zap.Logger, cfg *models.Config) (*database.Service, error) {
	dbService, err := database.NewService(ctx, logger, cfg.Database)
	if err != nil {
		return nil, err
	}
	return dbService, nil
}

func (cs *Services) Close() {
	if cs.DbService != nil {
		cs.DbService.Close()
	}
}

func loadPrimeCredentials() (*credentials.Credentials, error) {
	accessKey := os.Getenv("PRIME_ACCESS_KEY")
	passphrase := os.Getenv("PRIME_PASSPHRASE")
	signingKey := os.Getenv("PRIME_SIGNING_KEY")

	if accessKey == "" || passphrase == "" || signingKey == "" {
		fmt.Printf("Missing required Prime API credentials: PRIME_ACCESS_KEY: %s, PRIME_PASSPHRASE: %s, PRIME_SIGNING_KEY: %s", accessKey, passphrase, signingKey)
		return nil, fmt.Errorf("missing required Prime API credentials: PRIME_ACCESS_KEY, PRIME_PASSPHRASE, PRIME_SIGNING_KEY")
	}

	return &credentials.Credentials{
		AccessKey:  accessKey,
		Passphrase: passphrase,
		SigningKey: signingKey,
	}, nil
}

func isIgnorableSyncError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "sync /dev/stderr: inappropriate ioctl for device") ||
		strings.Contains(msg, "sync /dev/stdout: inappropriate ioctl for device")
}
