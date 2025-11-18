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
		_, _ = zap.NewProduction()
		zap.L().Fatal("Failed to load configuration", zap.Error(err))
	}

	_, loggerCleanup := common.InitializeLogger()
	defer loggerCleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	zap.L().Info("Starting Prime Send/Receive Listener")

	services, err := common.InitializeServices(ctx, cfg)
	if err != nil {
		zap.L().Fatal("Failed to initialize services", zap.Error(err))
	}
	defer services.Close()

	apiService := api.NewLedgerService(services.DbService)

	sendReceiveListener := listener.NewSendReceiveListener(listener.SendReceiveListenerConfig{
		PrimeService:    services.PrimeService,
		ApiService:      apiService,
		DbService:       services.DbService,
		PortfolioId:     services.DefaultPortfolio.Id,
		LookbackWindow:  cfg.Listener.LookbackWindow,
		PollingInterval: cfg.Listener.PollingInterval,
		CleanupInterval: cfg.Listener.CleanupInterval,
	})

	if err := sendReceiveListener.Start(ctx, cfg.Listener.AssetsFile); err != nil {
		zap.L().Fatal("Failed to start send/receive listener", zap.Error(err))
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	zap.L().Info("Send/Receive listener running - waiting for transactions...")
	zap.L().Info("Press Ctrl+C to stop")

	<-sigChan
	zap.L().Info("Shutdown signal received, stopping send/receive listener...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		sendReceiveListener.Stop()
		close(done)
	}()

	select {
	case <-done:
		zap.L().Info("Send/Receive listener stopped gracefully")
	case <-shutdownCtx.Done():
		zap.L().Warn("Forced shutdown after timeout")
	}
}
