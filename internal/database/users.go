package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"prime-send-receive-go/internal/database/models"
)

func (s *Service) GetUsers(ctx context.Context) ([]models.User, error) {
	s.logger.Debug("Querying active users")

	rows, err := s.db.QueryContext(ctx, queryGetActiveUsers)
	if err != nil {
		s.logger.Error("Failed to query users", zap.Error(err))
		return nil, fmt.Errorf("unable to query users: %v", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {

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
