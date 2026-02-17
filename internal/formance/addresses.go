package formance

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/store"

	"github.com/formancehq/formance-sdk-go/v3/pkg/models/operations"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// StoreAddress persists a deposit address in Formance via account metadata on the
// user account directly (no per-network sub-accounts). Each address is stored as
// an individual metadata key for reverse-lookup via ListAccounts metadata query.
func (s *Service) StoreAddress(ctx context.Context, params store.StoreAddressParams) (*models.Address, error) {
	userAccount := "users:" + params.UserId

	isWithdrawalAddr := params.Asset == "WITHDRAWAL" || params.Network == "external"

	zap.L().Info("Storing address in Formance",
		zap.String("account", userAccount),
		zap.String("asset", params.Asset),
		zap.String("network", params.Network),
		zap.String("address", params.Address),
		zap.Bool("is_withdrawal", isWithdrawalAddr))

	var meta map[string]string

	if isWithdrawalAddr {
		// Withdrawal address: separate prefix, not added to deposit_addresses map.
		// Value is the asset symbol (e.g. "USDC" from address book, or "WITHDRAWAL" if unknown).
		meta = map[string]string{
			withdrawalAddrMetaKey(params.Address): params.Asset,
		}
	} else {
		// Deposit address: added to deposit_addresses map + deposit_addr_ key.
		existing := s.getMetadataMapList(ctx, userAccount, "deposit_addresses")
		existing[params.Asset] = appendUnique(existing[params.Asset], params.Address)

		wallets := s.getMetadataMap(ctx, userAccount, "wallet_ids")
		if params.WalletId != "" {
			wallets[params.Asset] = params.WalletId
		}

		depositJSON, _ := json.Marshal(existing)
		walletJSON, _ := json.Marshal(wallets)

		meta = map[string]string{
			"deposit_addresses":         string(depositJSON),
			"wallet_ids":                string(walletJSON),
			addrMetaKey(params.Address): params.Asset,
		}
		if params.AccountIdentifier != "" {
			meta["account_identifier"] = params.AccountIdentifier
		}
	}

	_, err := s.client.Ledger.V2.AddMetadataToAccount(ctx, operations.V2AddMetadataToAccountRequest{
		Ledger:      s.ledger,
		Address:     userAccount,
		RequestBody: meta,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update user account metadata: %w", err)
	}

	now := time.Now()
	return &models.Address{
		Id:                uuid.New().String(),
		UserId:            params.UserId,
		Asset:             params.Asset,
		Network:           params.Network,
		Address:           params.Address,
		WalletId:          params.WalletId,
		AccountIdentifier: params.AccountIdentifier,
		CreatedAt:         now,
	}, nil
}

// GetAddresses returns addresses for a user/asset/network from the user account metadata.
func (s *Service) GetAddresses(ctx context.Context, userId, asset, network string) ([]models.Address, error) {
	userAccount := "users:" + userId

	resp, err := s.client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
		Ledger:  s.ledger,
		Address: userAccount,
	})
	if err != nil {
		if isNotFoundError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get user account: %w", err)
	}

	return parseAddressesFromMeta(userId, network, asset, resp.V2AccountResponse.Data.Metadata), nil
}

// GetAllUserAddresses returns all addresses for a user from their account metadata.
func (s *Service) GetAllUserAddresses(ctx context.Context, userId string) ([]models.Address, error) {
	userAccount := "users:" + userId

	resp, err := s.client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
		Ledger:  s.ledger,
		Address: userAccount,
	})
	if err != nil {
		if isNotFoundError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get user account: %w", err)
	}

	return parseAddressesFromMeta(userId, "", "", resp.V2AccountResponse.Data.Metadata), nil
}

// FindUserByAddress queries Formance accounts by the per-address metadata key
// to find the user account that owns a given deposit or withdrawal address.
func (s *Service) FindUserByAddress(ctx context.Context, address string) (*models.User, *models.Address, error) {
	depositKey := addrMetaKey(address)
	withdrawalKey := withdrawalAddrMetaKey(address)

	zap.L().Debug("Looking up user by address via metadata query",
		zap.String("address", address))

	// Search both deposit_addr_ and withdrawal_addr_ keys.
	var orClauses []any
	depositFilter := "metadata[" + depositKey + "]"
	for symbol := range assetPrecision {
		orClauses = append(orClauses, map[string]any{
			"$match": map[string]any{depositFilter: symbol},
		})
	}
	// Withdrawal address keys -- check for "WITHDRAWAL" and all known asset symbols.
	withdrawalFilter := "metadata[" + withdrawalKey + "]"
	orClauses = append(orClauses, map[string]any{
		"$match": map[string]any{withdrawalFilter: "WITHDRAWAL"},
	})
	for symbol := range assetPrecision {
		orClauses = append(orClauses, map[string]any{
			"$match": map[string]any{withdrawalFilter: symbol},
		})
	}

	resp, err := s.client.Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
		Ledger:   s.ledger,
		PageSize: ptrInt64(1),
		RequestBody: map[string]any{
			"$or": orClauses,
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query accounts by address: %w", err)
	}

	if len(resp.V2AccountsCursorResponse.Cursor.Data) == 0 {
		return nil, nil, nil
	}

	acct := resp.V2AccountsCursorResponse.Cursor.Data[0]
	meta := acct.Metadata
	userId := strings.TrimPrefix(acct.Address, "users:")

	if userId == "" || !strings.HasPrefix(acct.Address, "users:") {
		return nil, nil, nil
	}

	// Determine asset from whichever key matched (deposit or withdrawal).
	asset := meta[depositKey]
	if asset == "" {
		asset = meta[withdrawalKey] // asset symbol from address book, or "WITHDRAWAL"
	}
	walletIDs := parseJSONMap(meta["wallet_ids"])

	user, err := s.GetUserById(ctx, userId)
	if err != nil {
		return nil, nil, err
	}

	now := time.Now()
	addr := &models.Address{
		Id:                uuid.New().String(),
		UserId:            userId,
		Asset:             asset,
		Network:           "", // network is no longer part of the account structure
		Address:           address,
		WalletId:          walletIDs[asset],
		AccountIdentifier: meta["account_identifier"],
		CreatedAt:         now,
	}
	return user, addr, nil
}

// ---------- helpers ----------

// addrMetaKey returns the metadata key for a deposit address.
func addrMetaKey(address string) string {
	return "deposit_addr_" + strings.ToLower(address)
}

// withdrawalAddrMetaKey returns the metadata key for a withdrawal address.
func withdrawalAddrMetaKey(address string) string {
	return "withdrawal_addr_" + strings.ToLower(address)
}

// getMetadataMap reads a JSON-encoded string map from an account's metadata key.
func (s *Service) getMetadataMap(ctx context.Context, account, key string) map[string]string {
	resp, err := s.client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
		Ledger:  s.ledger,
		Address: account,
	})
	if err != nil {
		return make(map[string]string)
	}
	return parseJSONMap(resp.V2AccountResponse.Data.Metadata[key])
}

// getMetadataMapList reads a JSON-encoded map of string->[]string from metadata.
func (s *Service) getMetadataMapList(ctx context.Context, account, key string) map[string][]string {
	resp, err := s.client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
		Ledger:  s.ledger,
		Address: account,
	})
	if err != nil {
		return make(map[string][]string)
	}
	return parseJSONMapList(resp.V2AccountResponse.Data.Metadata[key])
}

// parseJSONMap parses a JSON-encoded string map.
func parseJSONMap(raw string) map[string]string {
	if raw == "" {
		return make(map[string]string)
	}
	var m map[string]string
	if json.Unmarshal([]byte(raw), &m) != nil {
		return make(map[string]string)
	}
	return m
}

// parseJSONMapList parses deposit_addresses which can be either:
//   - new format: {"USDC": ["0xabc", "0xdef"]}
//   - legacy format: {"USDC": "0xabc"}
func parseJSONMapList(raw string) map[string][]string {
	if raw == "" {
		return make(map[string][]string)
	}
	var listMap map[string][]string
	if json.Unmarshal([]byte(raw), &listMap) == nil {
		return listMap
	}
	var strMap map[string]string
	if json.Unmarshal([]byte(raw), &strMap) == nil {
		result := make(map[string][]string, len(strMap))
		for k, v := range strMap {
			result[k] = []string{v}
		}
		return result
	}
	return make(map[string][]string)
}

// appendUnique appends a value to a slice only if it's not already present.
func appendUnique(slice []string, val string) []string {
	for _, v := range slice {
		if strings.EqualFold(v, val) {
			return slice
		}
	}
	return append(slice, val)
}

// parseAddressesFromMeta extracts models.Address entries from user account metadata.
func parseAddressesFromMeta(userId, networkFilter, assetFilter string, meta map[string]string) []models.Address {
	depAddrs := parseJSONMapList(meta["deposit_addresses"])
	walletIDs := parseJSONMap(meta["wallet_ids"])
	if len(depAddrs) == 0 {
		return nil
	}

	now := time.Now()
	var result []models.Address
	for asset, addrs := range depAddrs {
		if assetFilter != "" && asset != assetFilter {
			continue
		}
		for _, addr := range addrs {
			result = append(result, models.Address{
				Id:                uuid.New().String(),
				UserId:            userId,
				Asset:             asset,
				Network:           "", // network not tracked at account level
				Address:           addr,
				WalletId:          walletIDs[asset],
				AccountIdentifier: meta["account_identifier"],
				CreatedAt:         now,
			})
		}
	}
	return result
}
