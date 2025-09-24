package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"prime-send-receive-go/internal/api"
	"prime-send-receive-go/internal/config"
	"prime-send-receive-go/internal/database"
	"prime-send-receive-go/internal/listener"
	"prime-send-receive-go/internal/prime"

	"github.com/coinbase-samples/prime-sdk-go/credentials"
	"go.uber.org/zap"
)

func main() {
	cfg := config.Load()

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer func(logger *zap.Logger) {
		if err := logger.Sync(); err != nil {
			if !isIgnorableSyncError(err) {
				log.Printf("Failed to sync logger: %v", err)
			}
		}
	}(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger.Info("Starting Prime Send/Receive Listener")

	dbService, err := database.NewService(ctx, logger, cfg.Database.Path)
	if err != nil {
		logger.Fatal("Failed to initialize database service", zap.Error(err))
	}
	defer dbService.Close()

	apiService := api.NewProductionLedgerService(dbService, logger)

	logger.Info("Loading Prime API credentials")
	creds, err := credentials.ReadEnvCredentials("PRIME_CREDENTIALS")
	if err != nil {
		logger.Fatal("Failed to read Prime credentials", zap.Error(err))
	}

	primeService, err := prime.NewService(creds, logger)
	if err != nil {
		logger.Fatal("Failed to initialize Prime service", zap.Error(err))
	}

	logger.Info("Finding default portfolio")
	defaultPortfolio, err := primeService.FindDefaultPortfolio(ctx)
	if err != nil {
		logger.Fatal("Failed to find default portfolio", zap.Error(err))
	}
	logger.Info("Using default portfolio",
		zap.String("name", defaultPortfolio.Name),
		zap.String("id", defaultPortfolio.Id))

	sendReceiveListener := listener.NewSendReceiveListener(
		primeService,
		apiService,
		dbService,
		logger,
		defaultPortfolio.Id,
		cfg.Listener.LookbackWindow,
		cfg.Listener.PollingInterval,
		cfg.Listener.CleanupInterval,
	)

	if err := sendReceiveListener.Start(ctx, cfg.Listener.AssetsFile); err != nil {
		logger.Fatal("Failed to start send/receive listener", zap.Error(err))
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("Send/Receive listener running - waiting for transactions...")
	logger.Info("Press Ctrl+C to stop")

	<-sigChan
	logger.Info("Shutdown signal received, stopping send/receive listener...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		sendReceiveListener.Stop()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("Send/Receive listener stopped gracefully")
	case <-shutdownCtx.Done():
		logger.Warn("Forced shutdown after timeout")
	}
}

func isIgnorableSyncError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "sync /dev/stderr: inappropriate ioctl for device") ||
		strings.Contains(msg, "sync /dev/stdout: inappropriate ioctl for device")
}
