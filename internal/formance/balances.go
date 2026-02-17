package formance

import (
	"context"
	"math/big"
	"time"

	"prime-send-receive-go/internal/models"

	v3 "github.com/formancehq/formance-sdk-go/v3"
	"github.com/formancehq/formance-sdk-go/v3/pkg/models/operations"
	"github.com/formancehq/formance-sdk-go/v3/pkg/models/shared"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// GetUserBalance returns the current balance for a user and asset.
// Queries the single users:{userId} account directly.
func (s *Service) GetUserBalance(ctx context.Context, userId, asset string) (decimal.Decimal, error) {
	zap.L().Debug("Getting user balance from Formance",
		zap.String("user_id", userId), zap.String("asset", asset))

	fAsset := formanceAsset(asset)
	vols := s.getAccountVolumes(ctx, "users:"+userId)
	if bal := volumeBalance(vols, fAsset); bal != nil {
		return bigIntToDecimal(bal, asset), nil
	}
	return decimal.Zero, nil
}

// GetAllUserBalances returns all non-zero balances for a user.
func (s *Service) GetAllUserBalances(ctx context.Context, userId string) ([]models.AccountBalance, error) {
	zap.L().Debug("Getting all user balances from Formance", zap.String("user_id", userId))

	addr := "users:" + userId
	vols := s.getAccountVolumes(ctx, addr)
	acctTime := s.getAccountUpdatedAt(ctx, addr)
	lastTx := s.getLastTransactionForAccount(ctx, addr)

	now := acctTime
	var balances []models.AccountBalance
	for fAsset, vol := range vols {
		bal := volumeBalance(map[string]shared.V2Volume{fAsset: vol}, fAsset)
		if bal == nil || bal.Sign() == 0 {
			continue
		}
		symbol := assetSymbol(fAsset)
		balances = append(balances, models.AccountBalance{
			Id:                addr,
			UserId:            userId,
			Asset:             symbol,
			Balance:           bigIntToDecimal(bal, symbol),
			LastTransactionId: lastTx.id,
			UpdatedAt:         now,
		})
	}
	return balances, nil
}

// ---------- helpers ----------

type txSummary struct {
	id     string
	amount string
	ts     time.Time
}

// getAccountVolumes fetches volumes for a single account via GetAccount (clean GET).
func (s *Service) getAccountVolumes(ctx context.Context, address string) map[string]shared.V2Volume {
	resp, err := s.client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
		Ledger:  s.ledger,
		Address: address,
		Expand:  v3.Pointer("volumes"),
	})
	if err != nil {
		zap.L().Warn("Failed to get account volumes", zap.String("address", address), zap.Error(err))
		return nil
	}
	return resp.V2AccountResponse.Data.Volumes
}

// getAccountUpdatedAt returns the last updated timestamp for an account.
func (s *Service) getAccountUpdatedAt(ctx context.Context, address string) time.Time {
	resp, err := s.client.Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
		Ledger:  s.ledger,
		Address: address,
	})
	if err != nil {
		return time.Now()
	}
	if t := resp.V2AccountResponse.Data.UpdatedAt; t != nil {
		return *t
	}
	if t := resp.V2AccountResponse.Data.FirstUsage; t != nil {
		return *t
	}
	return time.Now()
}

// getLastTransactionForAccount finds the most recent transaction touching this account.
func (s *Service) getLastTransactionForAccount(ctx context.Context, address string) txSummary {
	pageSize := int64(1)
	resp, err := s.client.Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
		Ledger:   s.ledger,
		PageSize: &pageSize,
		RequestBody: map[string]any{
			"$or": []any{
				map[string]any{"$match": map[string]any{"source": address}},
				map[string]any{"$match": map[string]any{"destination": address}},
			},
		},
	})
	if err != nil || len(resp.V2TransactionsCursorResponse.Cursor.Data) == 0 {
		return txSummary{}
	}
	tx := resp.V2TransactionsCursorResponse.Cursor.Data[0]
	ref := ""
	if tx.Reference != nil {
		ref = *tx.Reference
	}
	amt := tx.Metadata["amount_human"]
	return txSummary{id: ref, amount: amt, ts: tx.Timestamp}
}

// volumeBalance extracts the balance for a specific asset from volumes.
func volumeBalance(vols map[string]shared.V2Volume, fAsset string) *big.Int {
	vol, ok := vols[fAsset]
	if !ok {
		return nil
	}
	if vol.Balance != nil {
		return vol.Balance
	}
	if vol.Input == nil {
		return nil
	}
	result := new(big.Int).Set(vol.Input)
	if vol.Output != nil {
		result.Sub(result, vol.Output)
	}
	return result
}

// bigIntToDecimal converts a *big.Int in smallest-unit to a human-readable decimal.
func bigIntToDecimal(raw *big.Int, symbol string) decimal.Decimal {
	if raw == nil {
		return decimal.Zero
	}
	p := 6
	if v, ok := assetPrecision[symbol]; ok {
		p = v
	}
	return decimal.NewFromBigInt(raw, -int32(p))
}

// assetSymbol extracts the symbol from a Formance asset like "USDC/6".
func assetSymbol(fAsset string) string {
	for i, c := range fAsset {
		if c == '/' {
			return fAsset[:i]
		}
	}
	return fAsset
}
