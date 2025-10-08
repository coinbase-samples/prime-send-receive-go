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
	s.logger.Debug("Querying active users")

	rows, err := s.db.QueryContext(ctx, queryGetActiveUsers)
	if err != nil {
		s.logger.Error("Failed to query users", zap.Error(err))
		return nil, fmt.Errorf("unable to query users: %v", err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			s.logger.Warn("Failed to close rows", zap.Error(err))
		}
	}(rows)

	var users []models.User
	for rows.Next() {
		var user models.User
		err := rows.Scan(&user.Id, &user.Name, &user.Email, &user.CreatedAt, &user.UpdatedAt)
		if err != nil {
			s.logger.Error("Failed to scan user row", zap.Error(err))
			return nil, fmt.Errorf("unable to scan user row: %v", err)
		}

		users = append(users, user)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		s.logger.Error("Error during user row iteration", zap.Error(err))
		return nil, fmt.Errorf("error iterating user rows: %v", err)
	}

	s.logger.Info("Retrieved users", zap.Int("count", len(users)))
	return users, nil
}

func (s *Service) GetUserById(ctx context.Context, userId string) (*models.User, error) {
	s.logger.Debug("Querying user by ID", zap.String("user_id", userId))

	var user models.User
	err := s.db.QueryRowContext(ctx, queryGetUserById, userId).Scan(
		&user.Id, &user.Name, &user.Email, &user.CreatedAt, &user.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("user not found: %s", userId)
		}
		s.logger.Error("Failed to query user by ID", zap.String("user_id", userId), zap.Error(err))
		return nil, fmt.Errorf("unable to query user by ID: %v", err)
	}

	s.logger.Debug("Retrieved user by ID", zap.String("user_id", userId), zap.String("name", user.Name))
	return &user, nil
}

func (s *Service) GetUserByEmail(ctx context.Context, email string) (*models.User, error) {
	s.logger.Debug("Querying user by email", zap.String("email", email))

	var user models.User
	err := s.db.QueryRowContext(ctx, queryGetUserByEmail, email).Scan(
		&user.Id, &user.Name, &user.Email, &user.CreatedAt, &user.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("user not found: %s", email)
		}
		s.logger.Error("Failed to query user by email", zap.String("email", email), zap.Error(err))
		return nil, fmt.Errorf("unable to query user by email: %v", err)
	}

	s.logger.Debug("Retrieved user by email", zap.String("email", email), zap.String("name", user.Name))
	return &user, nil
}
