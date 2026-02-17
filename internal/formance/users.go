package formance

import (
	"context"
	"fmt"
	"strings"
	"time"

	"prime-send-receive-go/internal/models"

	"github.com/formancehq/formance-sdk-go/v3/pkg/models/operations"
	"github.com/formancehq/formance-sdk-go/v3/pkg/models/shared"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// decimalFromString is a thin helper around shopspring/decimal.
func decimalFromString(s string) (decimal.Decimal, error) {
	return decimal.NewFromString(s)
}

// ---------- User CRUD ----------

func (s *Service) CreateUser(ctx context.Context, userId, name, email string) (*models.User, error) {
	// Check if a user with this email already exists -- reject to prevent duplicates.
	existing, err := s.GetUserByEmail(ctx, email)
	if err == nil && existing != nil {
		zap.L().Info("User with this email already exists in Formance",
			zap.String("existing_id", existing.Id),
			zap.String("email", email))
		return nil, fmt.Errorf("user with email %s already exists", email)
	}

	addr := "users:" + userId
	zap.L().Info("Creating user in Formance", zap.String("address", addr), zap.String("email", email))

	_, err = s.client.Ledger.V2.AddMetadataToAccount(ctx, operations.V2AddMetadataToAccountRequest{
		Ledger:  s.ledger,
		Address: addr,
		RequestBody: map[string]string{
			"entity_type": "end_user",
			"active":      "true",
			"name":        name,
			"email":       email,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create user account: %w", err)
	}

	return s.GetUserById(ctx, userId)
}

func (s *Service) GetUserById(ctx context.Context, userId string) (*models.User, error) {
	addr := "users:" + userId

	resp, err := s.client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
		Ledger:  s.ledger,
		Address: addr,
	})
	if err != nil {
		if isNotFoundError(err) {
			return nil, fmt.Errorf("user not found: %s", userId)
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	acct := resp.V2AccountResponse.Data
	if acct.Metadata["email"] == "" {
		return nil, fmt.Errorf("user not found: %s", userId)
	}

	return accountToUser(&acct), nil
}

func (s *Service) GetUserByEmail(ctx context.Context, email string) (*models.User, error) {
	resp, err := s.client.Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
		Ledger:   s.ledger,
		PageSize: ptrInt64(100),
		RequestBody: map[string]any{
			"$match": map[string]any{
				"metadata[email]": email,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to search user by email: %w", err)
	}

	for i := range resp.V2AccountsCursorResponse.Cursor.Data {
		acct := &resp.V2AccountsCursorResponse.Cursor.Data[i]
		if strings.HasPrefix(acct.Address, "users:") && !strings.Contains(acct.Address[6:], ":") {
			return accountToUser(acct), nil
		}
	}
	return nil, fmt.Errorf("user not found: %s", email)
}

func (s *Service) GetUsers(ctx context.Context) ([]models.User, error) {
	resp, err := s.client.Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
		Ledger:   s.ledger,
		PageSize: ptrInt64(100),
		RequestBody: map[string]any{
			"$match": map[string]any{
				"metadata[entity_type]": "end_user",
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}

	var users []models.User
	for i := range resp.V2AccountsCursorResponse.Cursor.Data {
		acct := &resp.V2AccountsCursorResponse.Cursor.Data[i]
		// Only top-level user accounts (users:{id}, not users:{id}:{network}).
		parts := strings.Split(acct.Address, ":")
		if len(parts) == 2 && parts[0] == "users" {
			users = append(users, *accountToUser(acct))
		}
	}
	return users, nil
}

// ---------- helpers ----------

func accountToUser(acct *shared.V2Account) *models.User {
	meta := acct.Metadata
	addr := acct.Address
	userId := strings.TrimPrefix(addr, "users:")

	now := time.Now()
	if t := acct.FirstUsage; t != nil {
		now = *t
	}

	return &models.User{
		Id:        userId,
		Name:      meta["name"],
		Email:     meta["email"],
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func ptrInt64(v int64) *int64 { return &v }
