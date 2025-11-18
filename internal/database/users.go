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

package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"prime-send-receive-go/internal/models"

	"go.uber.org/zap"
)

func (s *Service) GetUsers(ctx context.Context) ([]models.User, error) {
	zap.L().Debug("Querying active users")

	rows, err := s.db.QueryContext(ctx, queryGetActiveUsers)
	if err != nil {
		zap.L().Error("Failed to query users", zap.Error(err))
		return nil, fmt.Errorf("unable to query users: %w", err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			zap.L().Warn("Failed to close rows", zap.Error(err))
		}
	}(rows)

	var users []models.User
	for rows.Next() {
		var user models.User
		err := rows.Scan(&user.Id, &user.Name, &user.Email, &user.CreatedAt, &user.UpdatedAt)
		if err != nil {
			zap.L().Error("Failed to scan user row", zap.Error(err))
			return nil, fmt.Errorf("unable to scan user row: %w", err)
		}

		users = append(users, user)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		zap.L().Error("Error during user row iteration", zap.Error(err))
		return nil, fmt.Errorf("error iterating user rows: %w", err)
	}

	zap.L().Info("Retrieved users", zap.Int("count", len(users)))
	return users, nil
}

func (s *Service) GetUserById(ctx context.Context, userId string) (*models.User, error) {
	zap.L().Debug("Querying user by ID", zap.String("user_id", userId))

	var user models.User
	err := s.db.QueryRowContext(ctx, queryGetUserById, userId).Scan(
		&user.Id, &user.Name, &user.Email, &user.CreatedAt, &user.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("user not found: %s", userId)
		}
		zap.L().Error("Failed to query user by ID", zap.String("user_id", userId), zap.Error(err))
		return nil, fmt.Errorf("unable to query user by ID: %w", err)
	}

	zap.L().Debug("Retrieved user by ID", zap.String("user_id", userId), zap.String("name", user.Name))
	return &user, nil
}

func (s *Service) GetUserByEmail(ctx context.Context, email string) (*models.User, error) {
	zap.L().Debug("Querying user by email", zap.String("email", email))

	var user models.User
	err := s.db.QueryRowContext(ctx, queryGetUserByEmail, email).Scan(
		&user.Id, &user.Name, &user.Email, &user.CreatedAt, &user.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("user not found: %s", email)
		}
		zap.L().Error("Failed to query user by email", zap.String("email", email), zap.Error(err))
		return nil, fmt.Errorf("unable to query user by email: %w", err)
	}

	zap.L().Debug("Retrieved user by email", zap.String("email", email), zap.String("name", user.Name))
	return &user, nil
}

func (s *Service) CreateUser(ctx context.Context, userId, name, email string) (*models.User, error) {
	zap.L().Info("Creating user", zap.String("id", userId), zap.String("name", name), zap.String("email", email))

	result, err := s.db.ExecContext(ctx, queryInsertUser, userId, name, email)
	if err != nil {
		zap.L().Error("Failed to insert user", zap.String("email", email), zap.Error(err))
		return nil, fmt.Errorf("unable to insert user: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		zap.L().Error("Failed to get rows affected", zap.Error(err))
		return nil, fmt.Errorf("unable to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return nil, fmt.Errorf("user with email %s already exists", email)
	}

	zap.L().Info("User created successfully", zap.String("id", userId), zap.String("name", name), zap.String("email", email))

	// Return the created user
	return s.GetUserByEmail(ctx, email)
}
