package models

import (
	"context"
	"time"
)

type primeContextKey struct{}

// PrimeDepositContext carries supplementary Prime API data through context
// so the Formance backend can store it as transaction metadata without
// changing the LedgerStore interface.
type PrimeDepositContext struct {
	TransactionId    string    // on-chain tx hash (e.g. "7AC02436")
	SourceAddress    string    // external sender wallet address
	SourceType       string    // transfer_from type (e.g. "WALLET")
	NetworkFees      string    // network fees paid
	Fees             string    // platform fees
	BlockchainIds    []string
	Network          string    // raw network from Prime (e.g. "base-mainnet")
	PrimeApiSymbol   string    // raw symbol before normalization (e.g. "BASEUSDC")
	WalletId         string    // source Prime wallet ID
	CreatedAt        string    // Prime created_at as string (for metadata)
	CompletedAt      string    // Prime completed_at as string (for metadata)
	TransactionTime  time.Time // effective transaction time for the ledger entry
}

// WithPrimeDepositContext attaches Prime deposit data to a context.
func WithPrimeDepositContext(ctx context.Context, pdc *PrimeDepositContext) context.Context {
	return context.WithValue(ctx, primeContextKey{}, pdc)
}

// GetPrimeDepositContext retrieves Prime deposit data from context, or nil if absent.
func GetPrimeDepositContext(ctx context.Context) *PrimeDepositContext {
	pdc, _ := ctx.Value(primeContextKey{}).(*PrimeDepositContext)
	return pdc
}
