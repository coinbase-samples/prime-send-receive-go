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
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"prime-send-receive-go/internal/api"
	"prime-send-receive-go/internal/common"
	"prime-send-receive-go/internal/config"
	"prime-send-receive-go/internal/formance"
	"prime-send-receive-go/internal/listener"
	"prime-send-receive-go/internal/models"

	"go.uber.org/zap"
)

func main() {
	assetsFilter := flag.String("assets", "", "Optional path to assets.yaml to limit monitoring to specific assets (default: monitor ALL Prime wallets)")
	allPortfolios := flag.Bool("all", false, "Monitor ALL discovered portfolios (default: only Default Portfolio)")
	flag.Parse()

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

	assetsFile := *assetsFilter
	if assetsFile != "" {
		zap.L().Info("Filtering monitored wallets by assets file", zap.String("file", assetsFile))
	} else {
		zap.L().Info("Monitoring ALL Prime wallets (no --assets filter)")
	}

	services, err := common.InitializeServices(ctx, cfg)
	if err != nil {
		zap.L().Fatal("Failed to initialize services", zap.Error(err))
	}
	defer services.Close()

	// Decide which portfolios to monitor.
	var portfolios []models.Portfolio
	if *allPortfolios {
		portfolios = services.Portfolios
		zap.L().Info("Monitoring ALL portfolios", zap.Int("count", len(portfolios)))
	} else {
		portfolios = []models.Portfolio{*services.DefaultPortfolio}
	}

	// Start one listener per portfolio.
	listeners := make([]*listener.SendReceiveListener, 0, len(portfolios))
	for _, p := range portfolios {
		dbSvc := services.DbService
		// For Formance: create a portfolio-scoped copy so each listener writes
		// to the correct account namespace. Shared HTTP client, no extra cost.
		if fSvc, ok := dbSvc.(*formance.Service); ok {
			dbSvc = fSvc.WithPortfolioID(p.Id)
		}

		apiSvc := api.NewLedgerService(dbSvc)
		l := listener.NewSendReceiveListener(listener.SendReceiveListenerConfig{
			PrimeService:    services.PrimeService,
			ApiService:      apiSvc,
			DbService:       dbSvc,
			PortfolioId:     p.Id,
			LookbackWindow:  cfg.Listener.LookbackWindow,
			PollingInterval: cfg.Listener.PollingInterval,
			CleanupInterval: cfg.Listener.CleanupInterval,
		})

		zap.L().Info("Starting listener for portfolio",
			zap.String("portfolio_id", p.Id),
			zap.String("portfolio_name", p.Name))

		if err := l.Start(ctx, assetsFile); err != nil {
			zap.L().Error("Failed to start listener for portfolio",
				zap.String("portfolio_id", p.Id),
				zap.String("portfolio_name", p.Name),
				zap.Error(err))
			continue
		}
		listeners = append(listeners, l)
	}

	if len(listeners) == 0 {
		zap.L().Fatal("No listeners started successfully")
	}

	zap.L().Info("All listeners running",
		zap.Int("active", len(listeners)),
		zap.Int("portfolios", len(portfolios)))
	zap.L().Info("Press Ctrl+C to stop")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	zap.L().Info("Shutdown signal received, stopping all listeners...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, l := range listeners {
			wg.Add(1)
			go func(l *listener.SendReceiveListener) {
				defer wg.Done()
				l.Stop()
			}(l)
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		zap.L().Info("All listeners stopped gracefully")
	case <-shutdownCtx.Done():
		zap.L().Warn("Forced shutdown after timeout")
	}
}
