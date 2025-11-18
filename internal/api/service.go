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
