package database

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

func (s *Service) GetUsers(ctx context.Context) ([]User, error) {
	s.logger.Debug("Querying active users")
	query := `
		SELECT id, name, email, created_at, updated_at
		FROM users
		WHERE active = 1
		ORDER BY created_at
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		s.logger.Error("Failed to query users", zap.Error(err))
		return nil, fmt.Errorf("unable to query users: %v", err)
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
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
