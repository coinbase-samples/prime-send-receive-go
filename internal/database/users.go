package database

import (
	"context"
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
	defer rows.Close()

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
