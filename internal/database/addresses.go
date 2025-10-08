package database

import (
	"context"
	"database/sql"
	"fmt"

	"prime-send-receive-go/internal/models"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

func (s *Service) StoreAddress(ctx context.Context, userId string, asset, network, address, walletId, accountIdentifier string) (*models.Address, error) {
	s.logger.Info("Storing address",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.String("network", network),
		zap.String("address", address))

	// Generate UUID for the address
	addressId := uuid.New().String()

	addr := &models.Address{}
	err := s.db.QueryRowContext(ctx, queryInsertAddress, addressId, userId, asset, network, address, walletId, accountIdentifier).Scan(
		&addr.Id, &addr.UserId, &addr.Asset, &addr.Network, &addr.Address, &addr.WalletId, &addr.AccountIdentifier, &addr.CreatedAt,
	)
	if err != nil {
		s.logger.Error("Failed to insert address",
			zap.String("user_id", userId),
			zap.String("asset", asset),
			zap.Error(err))
		return nil, fmt.Errorf("unable to insert address: %v", err)
	}

	s.logger.Info("Address stored successfully", zap.String("id", addressId))
	return addr, nil
}

func (s *Service) GetAddresses(ctx context.Context, userId string, asset string) ([]models.Address, error) {
	s.logger.Debug("Querying addresses",
		zap.String("user_id", userId),
		zap.String("asset", asset))

	rows, err := s.db.QueryContext(ctx, queryGetUserAddresses, userId, asset)
	if err != nil {
		s.logger.Error("Failed to query addresses",
			zap.String("user_id", userId),
			zap.String("asset", asset),
			zap.Error(err))
		return nil, fmt.Errorf("unable to query addresses: %v", err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			s.logger.Warn("Failed to close rows", zap.Error(err))
		}
	}(rows)

	var addresses []models.Address
	for rows.Next() {
		var addr models.Address
		err := rows.Scan(&addr.Id, &addr.UserId, &addr.Asset, &addr.Network, &addr.Address, &addr.WalletId, &addr.AccountIdentifier, &addr.CreatedAt)
		if err != nil {
			s.logger.Error("Failed to scan address row", zap.Error(err))
			return nil, fmt.Errorf("unable to scan address row: %v", err)
		}
		addresses = append(addresses, addr)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		s.logger.Error("Error during address row iteration", zap.Error(err))
		return nil, fmt.Errorf("error iterating address rows: %v", err)
	}

	s.logger.Debug("Retrieved addresses",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.Int("count", len(addresses)))
	return addresses, nil
}

func (s *Service) GetAllUserAddresses(ctx context.Context, userId string) ([]models.Address, error) {
	s.logger.Debug("Querying all addresses for user", zap.String("user_id", userId))

	rows, err := s.db.QueryContext(ctx, queryGetAllUserAddresses, userId)
	if err != nil {
		s.logger.Error("Failed to query all addresses",
			zap.String("user_id", userId),
			zap.Error(err))
		return nil, fmt.Errorf("unable to query all addresses: %v", err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			s.logger.Warn("Failed to close rows", zap.Error(err))
		}
	}(rows)

	var addresses []models.Address
	for rows.Next() {
		var addr models.Address
		err := rows.Scan(&addr.Id, &addr.UserId, &addr.Asset, &addr.Network, &addr.Address, &addr.WalletId, &addr.AccountIdentifier, &addr.CreatedAt)
		if err != nil {
			s.logger.Error("Failed to scan address row", zap.Error(err))
			return nil, fmt.Errorf("unable to scan address row: %v", err)
		}
		addresses = append(addresses, addr)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		s.logger.Error("Error during address row iteration", zap.Error(err))
		return nil, fmt.Errorf("error iterating address rows: %v", err)
	}

	s.logger.Debug("Retrieved all addresses",
		zap.String("user_id", userId),
		zap.Int("count", len(addresses)))
	return addresses, nil
}

func (s *Service) FindUserByAddress(ctx context.Context, address string) (*models.User, *models.Address, error) {
	s.logger.Debug("Finding user by address", zap.String("address", address))

	var user models.User
	var addr models.Address
	err := s.db.QueryRowContext(ctx, queryFindUserByAddress, address).Scan(
		&user.Id, &user.Name, &user.Email, &user.CreatedAt, &user.UpdatedAt,
		&addr.Id, &addr.UserId, &addr.Asset, &addr.Network, &addr.Address, &addr.WalletId, &addr.AccountIdentifier, &addr.CreatedAt,
	)

	if err == sql.ErrNoRows {
		s.logger.Debug("No user found for address", zap.String("address", address))
		return nil, nil, nil
	}

	if err != nil {
		s.logger.Error("Failed to query user by address", zap.String("address", address), zap.Error(err))
		return nil, nil, fmt.Errorf("unable to query user by address: %v", err)
	}

	s.logger.Debug("Found user by address",
		zap.String("address", address),
		zap.String("user_id", user.Id),
		zap.String("user_name", user.Name))
	return &user, &addr, nil
}
