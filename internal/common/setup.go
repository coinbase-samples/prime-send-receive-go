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

package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"prime-send-receive-go/internal/database"
	"prime-send-receive-go/internal/formance"
	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/prime"
	"prime-send-receive-go/internal/store"

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
	DbService        store.LedgerStore
	PrimeService     *prime.Service
	DefaultPortfolio *models.Portfolio
	Portfolios       []models.Portfolio
}

func InitializeLogger() (*zap.Logger, func()) {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	zap.ReplaceGlobals(logger)

	cleanup := func() {
		if err := logger.Sync(); err != nil {
			if !isIgnorableSyncError(err) {
				log.Printf("Failed to sync logger: %v\n", err)
			}
		}
	}

	return logger, cleanup
}

func InitializeServices(ctx context.Context, cfg *models.Config) (*Services, error) {
	ledger, err := initLedgerStore(ctx, cfg)
	if err != nil {
		return nil, err
	}

	zap.L().Info("Loading Prime API credentials")
	creds, err := loadPrimeCredentials()
	if err != nil {
		ledger.Close()
		return nil, err
	}

	primeService, err := prime.NewService(creds)
	if err != nil {
		ledger.Close()
		return nil, err
	}

	zap.L().Info("Discovering portfolios")
	allPortfolios, err := primeService.ListPortfolios(ctx)
	if err != nil {
		ledger.Close()
		return nil, fmt.Errorf("failed to list portfolios: %w", err)
	}
	for i, p := range allPortfolios {
		zap.L().Info("  Portfolio",
			zap.Int("index", i),
			zap.String("id", p.Id),
			zap.String("name", p.Name))
	}

	defaultPortfolio, err := primeService.FindDefaultPortfolio(ctx)
	if err != nil {
		ledger.Close()
		return nil, err
	}
	zap.L().Info("Using default portfolio",
		zap.String("name", defaultPortfolio.Name),
		zap.String("id", defaultPortfolio.Id))

	// If the backend is Formance, inject the portfolio ID for Numscript account paths.
	if fSvc, ok := ledger.(*formance.Service); ok {
		fSvc.SetPortfolioID(defaultPortfolio.Id)
	}

	return &Services{
		DbService:        ledger,
		PrimeService:     primeService,
		DefaultPortfolio: defaultPortfolio,
		Portfolios:       allPortfolios,
	}, nil
}

// InitializeDatabaseOnly initializes just the ledger store without Prime API.
// Useful for read-only operations like querying balances.
func InitializeDatabaseOnly(ctx context.Context, cfg *models.Config) (store.LedgerStore, error) {
	return initLedgerStore(ctx, cfg)
}

// initLedgerStore selects and initialises the backend based on BACKEND_TYPE.
func initLedgerStore(ctx context.Context, cfg *models.Config) (store.LedgerStore, error) {
	switch strings.ToLower(cfg.BackendType) {
	case "formance":
		zap.L().Info("Using Formance backend", zap.String("stack_url", cfg.Formance.StackURL))
		return formance.NewService(ctx, cfg.Formance)
	default:
		zap.L().Info("Using SQLite backend", zap.String("db_path", cfg.Database.Path))
		return database.NewService(ctx, cfg.Database)
	}
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
