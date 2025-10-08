package api

import (
	"context"
	"fmt"

	"prime-send-receive-go/internal/database"

	"go.uber.org/zap"
)

// LedgerService provides minimal API
type LedgerService struct {
	db     *database.Service
	logger *zap.Logger
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
