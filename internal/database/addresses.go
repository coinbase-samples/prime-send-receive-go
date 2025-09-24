package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

func (s *Service) StoreAddress(ctx context.Context, userId string, asset, network, address, walletId, accountIdentifier string) (*Address, error) {
	s.logger.Info("Storing address",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.String("network", network),
		zap.String("address", address))

	// Generate UUID for the address
	addressId := uuid.New().String()

	query := `
		INSERT INTO addresses (id, user_id, asset, network, address, wallet_id, account_identifier)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`

	_, err := s.db.ExecContext(ctx, query, addressId, userId, asset, network, address, walletId, accountIdentifier)
	if err != nil {
		s.logger.Error("Failed to insert address",
			zap.String("user_id", userId),
			zap.String("asset", asset),
			zap.Error(err))
		return nil, fmt.Errorf("unable to insert address: %v", err)
	}

	// Get the created address
	addr := &Address{
		Id:                addressId,
		UserId:            userId,
		Asset:             asset,
		Network:           network,
		Address:           address,
		WalletId:          walletId,
		AccountIdentifier: accountIdentifier,
		CreatedAt:         time.Now(),
	}

	s.logger.Info("Address stored successfully", zap.String("id", addressId))
	return addr, nil
}

func (s *Service) GetAddresses(ctx context.Context, userId string, asset string) ([]Address, error) {
	s.logger.Debug("Querying addresses",
		zap.String("user_id", userId),
		zap.String("asset", asset))

	query := `
		SELECT id, user_id, asset, network, address, wallet_id, account_identifier, created_at
		FROM addresses
		WHERE user_id = ? AND asset = ?
		ORDER BY created_at DESC
	`

	rows, err := s.db.QueryContext(ctx, query, userId, asset)
	if err != nil {
		s.logger.Error("Failed to query addresses",
			zap.String("user_id", userId),
			zap.String("asset", asset),
			zap.Error(err))
		return nil, fmt.Errorf("unable to query addresses: %v", err)
	}
	defer rows.Close()

	var addresses []Address
	for rows.Next() {
		var addr Address
		err := rows.Scan(&addr.Id, &addr.UserId, &addr.Asset, &addr.Network, &addr.Address, &addr.WalletId, &addr.AccountIdentifier, &addr.CreatedAt)
		if err != nil {
			s.logger.Error("Failed to scan address row", zap.Error(err))
			return nil, fmt.Errorf("unable to scan address row: %v", err)
		}
		addresses = append(addresses, addr)
	}

	s.logger.Debug("Retrieved addresses",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.Int("count", len(addresses)))
	return addresses, nil
}

func (s *Service) FindUserByAddress(ctx context.Context, address string) (*User, *Address, error) {
	s.logger.Debug("Finding user by address", zap.String("address", address))

	query := `
		SELECT u.id, u.name, u.email, u.created_at, u.updated_at,
		       a.id, a.user_id, a.asset, a.network, a.address, a.wallet_id, a.account_identifier, a.created_at
		FROM users u
		JOIN addresses a ON u.id = a.user_id
		WHERE a.address = ? AND u.active = 1
	`

	var user User
	var addr Address
	err := s.db.QueryRowContext(ctx, query, address).Scan(
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
