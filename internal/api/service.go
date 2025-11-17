package api

import (
	"context"
	"fmt"

	"prime-send-receive-go/internal/database"
)

// LedgerService provides minimal API
type LedgerService struct {
	db *database.Service
}

func NewLedgerService(db *database.Service) *LedgerService {
	return &LedgerService{
		db: db,
	}
}

func (s *LedgerService) HealthCheck(ctx context.Context) error {
	_, err := s.db.GetUsers(ctx)
	if err != nil {
		return fmt.Errorf("database health check failed: %w", err)
	}
	return nil
}
