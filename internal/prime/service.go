package prime

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"prime-send-receive-go/internal/models"

	"github.com/coinbase-samples/prime-sdk-go/client"
	"github.com/coinbase-samples/prime-sdk-go/credentials"
	"github.com/coinbase-samples/prime-sdk-go/model"
	"github.com/coinbase-samples/prime-sdk-go/portfolios"
	"github.com/coinbase-samples/prime-sdk-go/transactions"
	"github.com/coinbase-samples/prime-sdk-go/wallets"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

type Service struct {
	client          client.RestClient
	portfoliosSvc   portfolios.PortfoliosService
	walletsSvc      wallets.WalletsService
	transactionsSvc transactions.TransactionsService
}

func NewService(creds *credentials.Credentials) (*Service, error) {
	httpClient, err := createCustomHttpClient()
	if err != nil {
		return nil, fmt.Errorf("unable to create custom http client: %w", err)
	}

	restClient := client.NewRestClient(creds, httpClient)

	return &Service{
		client:          restClient,
		portfoliosSvc:   portfolios.NewPortfoliosService(restClient),
		walletsSvc:      wallets.NewWalletsService(restClient),
		transactionsSvc: transactions.NewTransactionsService(restClient),
	}, nil
}

func createCustomHttpClient() (http.Client, error) {
	tr := &http.Transport{
		ResponseHeaderTimeout: 30 * time.Second,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: 30 * time.Second,
			DualStack: true,
			Timeout:   15 * time.Second,
		}).DialContext,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		MaxIdleConnsPerHost:   5,
		ExpectContinueTimeout: 5 * time.Second,
	}

	if err := http2.ConfigureTransport(tr); err != nil {
		return http.Client{}, err
	}

	return http.Client{
		Transport: tr,
		Timeout:   60 * time.Second,
	}, nil
}

func (s *Service) ListPortfolios(ctx context.Context) ([]models.Portfolio, error) {
	request := &portfolios.ListPortfoliosRequest{}

	response, err := s.portfoliosSvc.ListPortfolios(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("unable to list portfolios: %w", err)
	}

	portfolioList := make([]models.Portfolio, len(response.Portfolios))
	for i, p := range response.Portfolios {
		portfolioList[i] = models.Portfolio{
			Id:   p.Id,
			Name: p.Name,
		}
	}

	return portfolioList, nil
}

func (s *Service) FindDefaultPortfolio(ctx context.Context) (*models.Portfolio, error) {
	portfolioList, err := s.ListPortfolios(ctx)
	if err != nil {
		return nil, err
	}

	for _, portfolio := range portfolioList {
		if portfolio.Name == "Default Portfolio" {
			return &portfolio, nil
		}
	}

	return nil, fmt.Errorf("default portfolio not found")
}

func (s *Service) ListWallets(ctx context.Context, portfolioId, walletType string, symbols []string) ([]models.Wallet, error) {
	request := &wallets.ListWalletsRequest{
		PortfolioId: portfolioId,
		Type:        walletType,
		Symbols:     symbols,
	}

	response, err := s.walletsSvc.ListWallets(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("unable to list wallets: %w", err)
	}

	walletList := make([]models.Wallet, len(response.Wallets))
	for i, w := range response.Wallets {
		walletList[i] = models.Wallet{
			Id:     w.Id,
			Name:   w.Name,
			Symbol: w.Symbol,
			Type:   w.Type,
		}
	}

	return walletList, nil
}

func (s *Service) CreateDepositAddress(ctx context.Context, portfolioId, walletId, asset, network string) (*models.DepositAddress, error) {
	request := &wallets.CreateWalletAddressRequest{
		PortfolioId: portfolioId,
		WalletId:    walletId,
		NetworkId:   network,
	}

	response, err := s.walletsSvc.CreateWalletAddress(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("unable to create wallet address: %w", err)
	}

	return &models.DepositAddress{
		Id:      response.AccountIdentifier,
		Address: response.Address,
		Network: network,
		Asset:   asset,
	}, nil
}

func (s *Service) CreateWallet(ctx context.Context, portfolioId, name, symbol, walletType string) (*models.Wallet, error) {
	request := &wallets.CreateWalletRequest{
		PortfolioId:    portfolioId,
		Name:           name,
		Symbol:         symbol,
		Type:           walletType,
		IdempotencyKey: uuid.New().String(),
	}

	response, err := s.walletsSvc.CreateWallet(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("unable to create wallet: %w", err)
	}

	return &models.Wallet{
		Id:     response.ActivityId,
		Name:   response.Name,
		Symbol: response.Symbol,
		Type:   response.Type,
	}, nil
}

// CreateWithdrawalParams contains parameters for creating a withdrawal
type CreateWithdrawalParams struct {
	PortfolioId        string
	WalletId           string
	DestinationAddress string
	Amount             string
	Asset              string
	IdempotencyKey     string
}

// CreateWithdrawal creates a withdrawal from a wallet
func (s *Service) CreateWithdrawal(ctx context.Context, params CreateWithdrawalParams) (*models.Withdrawal, error) {
	zap.L().Info("Creating withdrawal via Prime API",
		zap.String("portfolio_id", params.PortfolioId),
		zap.String("wallet_id", params.WalletId),
		zap.String("asset", params.Asset),
		zap.String("amount", params.Amount),
		zap.String("destination", params.DestinationAddress))

	// Parse asset string: ETH-ethereum-mainnet --> ETH, ethereum, mainnet
	// Or just: ETH --> ETH (defaults to ethereum-mainnet in Prime API)
	parts := strings.Split(params.Asset, "-")
	symbol := parts[0]

	blockchainAddr := &model.BlockchainAddress{
		Address: params.DestinationAddress,
	}

	// If network is specified, include it in the request
	if len(parts) >= 3 {
		networkId := parts[1]
		networkType := parts[2]
		blockchainAddr.Network = &model.NetworkDetails{
			Id:   networkId,
			Type: networkType,
		}
		zap.L().Debug("Including network details in withdrawal",
			zap.String("network_id", networkId),
			zap.String("network_type", networkType))
	}

	request := &transactions.CreateWalletWithdrawalRequest{
		PortfolioId:       params.PortfolioId,
		SourceWalletId:    params.WalletId,
		Amount:            params.Amount,
		IdempotencyKey:    params.IdempotencyKey,
		Symbol:            symbol,
		DestinationType:   "DESTINATION_BLOCKCHAIN",
		BlockchainAddress: blockchainAddr,
	}
	// Debug: Log the request structure
	zap.L().Debug("Withdrawal request details",
		zap.String("portfolio_id", request.PortfolioId),
		zap.String("wallet_id", request.SourceWalletId),
		zap.String("amount", request.Amount),
		zap.String("destination_type", request.DestinationType),
		zap.String("idempotency_key", request.IdempotencyKey),
		zap.Any("blockchain_address", request.BlockchainAddress))

	response, err := s.transactionsSvc.CreateWalletWithdrawal(ctx, request)
	if err != nil {
		zap.L().Error("Failed to create withdrawal",
			zap.String("wallet_id", params.WalletId),
			zap.String("amount", params.Amount),
			zap.String("asset", params.Asset),
			zap.Error(err))
		return nil, fmt.Errorf("unable to create withdrawal: %w", err)
	}

	zap.L().Info("Withdrawal created successfully",
		zap.String("activity_id", response.ActivityId),
		zap.String("wallet_id", params.WalletId),
		zap.String("amount", params.Amount),
		zap.String("asset", params.Asset))

	return &models.Withdrawal{
		ActivityId:     response.ActivityId,
		Asset:          params.Asset,
		Amount:         params.Amount,
		Destination:    params.DestinationAddress,
		IdempotencyKey: params.IdempotencyKey,
	}, nil
}

// ListWalletTransactions fetches transactions for a specific wallet
func (s *Service) ListWalletTransactions(ctx context.Context, portfolioId, walletId string, startTime time.Time) (*transactions.ListWalletTransactionsResponse, error) {
	zap.L().Debug("Making Prime API request",
		zap.String("portfolio_id", portfolioId),
		zap.String("wallet_id", walletId),
		zap.Time("start_time", startTime),
		zap.String("start_time_formatted", startTime.UTC().Format("2006-01-02T15:04:05Z")),
		zap.Strings("types", []string{"DEPOSIT", "WITHDRAWAL"}))

	request := &transactions.ListWalletTransactionsRequest{
		PortfolioId: portfolioId,
		WalletId:    walletId,
		Start:       startTime,
		Types:       []string{"DEPOSIT", "WITHDRAWAL"},
		Pagination: &model.PaginationParams{
			Limit: 500,
		},
	}

	response, err := s.transactionsSvc.ListWalletTransactions(ctx, request)
	if err != nil {
		zap.L().Error("Failed to list wallet transactions",
			zap.String("wallet_id", walletId),
			zap.Error(err))
		return nil, fmt.Errorf("unable to list wallet transactions: %w", err)
	}

	zap.L().Debug("Prime API response received",
		zap.String("wallet_id", walletId),
		zap.Int("count", len(response.Transactions)))

	// Log details of each transaction for debugging
	for i, tx := range response.Transactions {
		zap.L().Debug("Transaction details",
			zap.Int("index", i),
			zap.String("id", tx.Id),
			zap.String("type", tx.Type),
			zap.String("status", tx.Status),
			zap.String("symbol", tx.Symbol),
			zap.Time("created", tx.Created),
			zap.String("amount", tx.Amount))
	}

	return response, nil
}
