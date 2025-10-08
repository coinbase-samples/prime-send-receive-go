package common

import (
	"context"
	"log"
	"strings"

	"prime-send-receive-go/internal/config"
	"prime-send-receive-go/internal/database"
	"prime-send-receive-go/internal/prime"
	"prime-send-receive-go/internal/prime/models"

	"github.com/coinbase-samples/prime-sdk-go/credentials"
	"go.uber.org/zap"
)

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

func InitializeServices(ctx context.Context, logger *zap.Logger, cfg *config.Config) (*Services, error) {
	dbService, err := database.NewService(ctx, logger, cfg.Database)
	if err != nil {
		return nil, err
	}

	logger.Info("Loading Prime API credentials")
	creds, err := credentials.ReadEnvCredentials("PRIME_CREDENTIALS")
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

func (cs *Services) Close() {
	if cs.DbService != nil {
		cs.DbService.Close()
	}
}

func isIgnorableSyncError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "sync /dev/stderr: inappropriate ioctl for device") ||
		strings.Contains(msg, "sync /dev/stdout: inappropriate ioctl for device")
}
