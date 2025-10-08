package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"prime-send-receive-go/internal/api"
	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"
	"prime-send-receive-go/internal/listener"

	"go.uber.org/zap"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		logger, _ := zap.NewProduction()
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	logger, loggerCleanup := common.InitializeLogger()
	defer loggerCleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger.Info("Starting Prime Send/Receive Listener")

	services, err := common.InitializeServices(ctx, logger, cfg)
	if err != nil {
		logger.Fatal("Failed to initialize services", zap.Error(err))
	}
	defer services.Close()

	apiService := api.NewLedgerService(services.DbService, logger)

	sendReceiveListener := listener.NewSendReceiveListener(
		services.PrimeService,
		apiService,
		services.DbService,
		logger,
		services.DefaultPortfolio.Id,
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
