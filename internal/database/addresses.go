package database

import (
	"context"
	"database/sql"
	"fmt"

	"prime-send-receive-go/internal/models"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type StoreAddressParams struct {
	UserId            string
	Asset             string
	Network           string
	Address           string
	WalletId          string
	AccountIdentifier string
}

func (s *Service) StoreAddress(ctx context.Context, params StoreAddressParams) (*models.Address, error) {
	zap.L().Info("Storing address",
		zap.String("user_id", params.UserId),
		zap.String("asset", params.Asset),
		zap.String("network", params.Network),
		zap.String("address", params.Address))

	// Generate UUID for the address
	addressId := uuid.New().String()

	addr := &models.Address{}
	err := s.db.QueryRowContext(ctx, queryInsertAddress, addressId, params.UserId, params.Asset, params.Network, params.Address, params.WalletId, params.AccountIdentifier).Scan(
		&addr.Id, &addr.UserId, &addr.Asset, &addr.Network, &addr.Address, &addr.WalletId, &addr.AccountIdentifier, &addr.CreatedAt,
	)
	if err != nil {
		zap.L().Error("Failed to insert address",
			zap.String("user_id", params.UserId),
			zap.String("asset", params.Asset),
			zap.Error(err))
		return nil, fmt.Errorf("unable to insert address: %w", err)
	}

	zap.L().Info("Address stored successfully", zap.String("id", addressId))
	return addr, nil
}

func (s *Service) GetAddresses(ctx context.Context, userId string, asset string, network string) ([]models.Address, error) {
	zap.L().Debug("Querying addresses",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.String("network", network))

	rows, err := s.db.QueryContext(ctx, queryGetUserAddresses, userId, asset, network)
	if err != nil {
		zap.L().Error("Failed to query addresses",
			zap.String("user_id", userId),
			zap.String("asset", asset),
			zap.String("network", network),
			zap.Error(err))
		return nil, fmt.Errorf("unable to query addresses: %w", err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			zap.L().Warn("Failed to close rows", zap.Error(err))
		}
	}(rows)

	var addresses []models.Address
	for rows.Next() {
		var addr models.Address
		err := rows.Scan(&addr.Id, &addr.UserId, &addr.Asset, &addr.Network, &addr.Address, &addr.WalletId, &addr.AccountIdentifier, &addr.CreatedAt)
		if err != nil {
			zap.L().Error("Failed to scan address row", zap.Error(err))
			return nil, fmt.Errorf("unable to scan address row: %w", err)
		}
		addresses = append(addresses, addr)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		zap.L().Error("Error during address row iteration", zap.Error(err))
		return nil, fmt.Errorf("error iterating address rows: %w", err)
	}

	zap.L().Debug("Retrieved addresses",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.String("network", network),
		zap.Int("count", len(addresses)))
	return addresses, nil
}

func (s *Service) GetAllUserAddresses(ctx context.Context, userId string) ([]models.Address, error) {
	zap.L().Debug("Querying all addresses for user", zap.String("user_id", userId))

	rows, err := s.db.QueryContext(ctx, queryGetAllUserAddresses, userId)
	if err != nil {
		zap.L().Error("Failed to query all addresses",
			zap.String("user_id", userId),
			zap.Error(err))
		return nil, fmt.Errorf("unable to query all addresses: %w", err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			zap.L().Warn("Failed to close rows", zap.Error(err))
		}
	}(rows)

	var addresses []models.Address
	for rows.Next() {
		var addr models.Address
		err := rows.Scan(&addr.Id, &addr.UserId, &addr.Asset, &addr.Network, &addr.Address, &addr.WalletId, &addr.AccountIdentifier, &addr.CreatedAt)
		if err != nil {
			zap.L().Error("Failed to scan address row", zap.Error(err))
			return nil, fmt.Errorf("unable to scan address row: %w", err)
		}
		addresses = append(addresses, addr)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		zap.L().Error("Error during address row iteration", zap.Error(err))
		return nil, fmt.Errorf("error iterating address rows: %w", err)
	}

	zap.L().Debug("Retrieved all addresses",
		zap.String("user_id", userId),
		zap.Int("count", len(addresses)))
	return addresses, nil
}

func (s *Service) FindUserByAddress(ctx context.Context, address string) (*models.User, *models.Address, error) {
	zap.L().Debug("Finding user by address", zap.String("address", address))

	var user models.User
	var addr models.Address
	err := s.db.QueryRowContext(ctx, queryFindUserByAddress, address).Scan(
		&user.Id, &user.Name, &user.Email, &user.CreatedAt, &user.UpdatedAt,
		&addr.Id, &addr.UserId, &addr.Asset, &addr.Network, &addr.Address, &addr.WalletId, &addr.AccountIdentifier, &addr.CreatedAt,
	)

	if err == sql.ErrNoRows {
		zap.L().Debug("No user found for address", zap.String("address", address))
		return nil, nil, nil
	}

	if err != nil {
		zap.L().Error("Failed to query user by address", zap.String("address", address), zap.Error(err))
		return nil, nil, fmt.Errorf("unable to query user by address: %w", err)
	}

	zap.L().Debug("Found user by address",
		zap.String("address", address),
		zap.String("user_id", user.Id),
		zap.String("user_name", user.Name))
	return &user, &addr, nil
}
