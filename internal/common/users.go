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

package common

import (
	"context"
	"fmt"

	"prime-send-receive-go/internal/store"

	"go.uber.org/zap"
)

// UserInfo represents simplified user information for command-line utilities
type UserInfo struct {
	Id    string
	Name  string
	Email string
}

// InitializeUsers retrieves users based on an optional email filter.
// If emailFilter is provided, returns a single user with that email.
// If emailFilter is empty, returns all users.
func InitializeUsers(ctx context.Context, dbService store.LedgerStore, emailFilter string, logger *zap.Logger) ([]UserInfo, error) {
	var users []UserInfo

	if emailFilter != "" {
		logger.Info("Looking up user by email", zap.String("email", emailFilter))
		user, err := dbService.GetUserByEmail(ctx, emailFilter)
		if err != nil {
			return nil, fmt.Errorf("user not found: %w", err)
		}
		users = append(users, UserInfo{
			Id:    user.Id,
			Name:  user.Name,
			Email: user.Email,
		})
	} else {
		allUsers, err := dbService.GetUsers(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get users: %w", err)
		}
		for _, u := range allUsers {
			users = append(users, UserInfo{
				Id:    u.Id,
				Name:  u.Name,
				Email: u.Email,
			})
		}
	}

	logger.Info("Retrieved users", zap.Int("count", len(users)))
	return users, nil
}
