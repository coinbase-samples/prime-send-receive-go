package api

import (
	"context"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"prime-send-receive-go/internal/database"

	"go.uber.org/zap"
)

// LedgerService provides minimal API
type LedgerService struct {
	db     *database.Service
	logger *zap.Logger
}

// UserBalance represents a user's balance for a specific asset
type UserBalance struct {
	Asset   string          `json:"asset"`
	Balance decimal.Decimal `json:"balance"`
}

// TransactionRecord represents a transaction in the user's history
type TransactionRecord struct {
	Id          string          `json:"id"`
	Type        string          `json:"type"` // "deposit", "withdrawal"
	Asset       string          `json:"asset"`
	Amount      decimal.Decimal `json:"amount"`
	Address     string          `json:"address,omitempty"`
	Status      string          `json:"status"`
	ProcessedAt time.Time       `json:"processed_at"`
}

// DepositResult represents the result of processing a deposit
type DepositResult struct {
	Success    bool            `json:"success"`
	UserId     string          `json:"user_id,omitempty"`
	Asset      string          `json:"asset,omitempty"`
	Amount     decimal.Decimal `json:"amount,omitempty"`
	NewBalance decimal.Decimal `json:"new_balance,omitempty"`
	Error      string          `json:"error,omitempty"`
}

func NewLedgerService(db *database.Service, logger *zap.Logger) *LedgerService {
	return &LedgerService{
		db:     db,
		logger: logger,
	}
}

func (s *LedgerService) HealthCheck(ctx context.Context) error {
	_, err := s.db.GetUsers(ctx)
	if err != nil {
		return fmt.Errorf("database health check failed: %v", err)
	}
	return nil
}
