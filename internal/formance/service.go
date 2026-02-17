package formance

import (
	"context"
	"errors"
	"fmt"

	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/store"

	v3 "github.com/formancehq/formance-sdk-go/v3"
	"github.com/formancehq/formance-sdk-go/v3/pkg/models/operations"
	"github.com/formancehq/formance-sdk-go/v3/pkg/models/sdkerrors"
	"github.com/formancehq/formance-sdk-go/v3/pkg/models/shared"
	"go.uber.org/zap"
)

// Compile-time check: *Service must satisfy store.LedgerStore.
var _ store.LedgerStore = (*Service)(nil)

// assetPrecision maps canonical asset symbols to their decimal precision.
var assetPrecision = map[string]int{
	"USD":  2,
	"USDC": 6,
	"USDT": 6,
	"BTC":  8,
	"ETH":  18,
	"SOL":  9,
}

// Service implements store.LedgerStore backed by a Formance Stack ledger.
type Service struct {
	client      *v3.Formance
	ledger      string
	portfolioID string // Coinbase Prime portfolio ID, set via SetPortfolioID after init
}

// NewService creates a Formance-backed LedgerStore.
// It connects to the stack, creates the ledger if it doesn't already exist, and returns ready to use.
func NewService(ctx context.Context, cfg models.FormanceConfig) (*Service, error) {
	if cfg.StackURL == "" || cfg.ClientID == "" || cfg.ClientSecret == "" {
		return nil, fmt.Errorf("formance config requires StackURL, ClientID, and ClientSecret")
	}
	if cfg.LedgerName == "" {
		cfg.LedgerName = "coinbase-prime-send-receive"
	}

	zap.L().Info("Connecting to Formance Stack",
		zap.String("stack_url", cfg.StackURL),
		zap.String("ledger", cfg.LedgerName))

	client := v3.New(
		v3.WithServerURL(cfg.StackURL),
		v3.WithSecurity(shared.Security{
			ClientID:     v3.Pointer(cfg.ClientID),
			ClientSecret: v3.Pointer(cfg.ClientSecret),
		}),
	)

	svc := &Service{client: client, ledger: cfg.LedgerName}

	if err := svc.ensureLedger(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure ledger exists: %w", err)
	}

	zap.L().Info("Formance service initialized", zap.String("ledger", cfg.LedgerName))
	return svc, nil
}

// ensureLedger creates the ledger if it does not already exist.
func (s *Service) ensureLedger(ctx context.Context) error {
	_, err := s.client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
		Ledger: s.ledger,
		V2CreateLedgerRequest: shared.V2CreateLedgerRequest{
			Metadata: map[string]string{
				"application": "coinbase-prime-send-receive",
			},
		},
	})
	if err != nil {
		var apiErr *sdkerrors.V2ErrorResponse
		if errors.As(err, &apiErr) && apiErr.ErrorCode == shared.V2ErrorsEnumLedgerAlreadyExists {
			zap.L().Info("Ledger already exists", zap.String("ledger", s.ledger))
			return nil
		}
		return err
	}
	zap.L().Info("Ledger created", zap.String("ledger", s.ledger))
	return nil
}

// SetPortfolioID sets the Coinbase Prime portfolio ID used in account paths.
// Called by common.InitializeServices after the default portfolio is resolved.
func (s *Service) SetPortfolioID(id string) { s.portfolioID = id }

// WithPortfolioID returns a shallow copy scoped to a different portfolio.
// Shares the same HTTP client and ledger -- only the Numscript $portfolio_id changes.
func (s *Service) WithPortfolioID(id string) *Service {
	return &Service{client: s.client, ledger: s.ledger, portfolioID: id}
}

// Close is a no-op for the Formance backend (HTTP client needs no teardown).
func (s *Service) Close() {}

// ---------- helpers ----------

// formanceAsset returns the Formance UMN notation, e.g. "USDC/6".
func formanceAsset(symbol string) string {
	if p, ok := assetPrecision[symbol]; ok {
		return fmt.Sprintf("%s/%d", symbol, p)
	}
	return fmt.Sprintf("%s/6", symbol) // default precision 6
}

// isConflictError checks whether a Formance SDK error is a CONFLICT (duplicate reference).
func isConflictError(err error) bool {
	var apiErr *sdkerrors.V2ErrorResponse
	return errors.As(err, &apiErr) && apiErr.ErrorCode == shared.V2ErrorsEnumConflict
}

// isNotFoundError checks whether a Formance SDK error is NOT_FOUND.
func isNotFoundError(err error) bool {
	var apiErr *sdkerrors.V2ErrorResponse
	return errors.As(err, &apiErr) && apiErr.ErrorCode == shared.V2ErrorsEnumNotFound
}

// isAlreadyRevertedError checks whether a Formance SDK error is ALREADY_REVERT.
func isAlreadyRevertedError(err error) bool {
	var apiErr *sdkerrors.V2ErrorResponse
	return errors.As(err, &apiErr) && apiErr.ErrorCode == shared.V2ErrorsEnumAlreadyRevert
}
