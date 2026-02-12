package store

import (
	"context"
	"errors"
	"time"

	"prime-send-receive-go/internal/models"

	"github.com/shopspring/decimal"
)

// Sentinel errors shared across all backend implementations.
var (
	ErrDuplicateTransaction   = errors.New("duplicate transaction")
	ErrConcurrentModification = errors.New("concurrent modification detected")
	ErrUserNotFound           = errors.New("no user found for address")
)

// StoreAddressParams contains the parameters for storing a deposit address.
type StoreAddressParams struct {
	UserId            string
	Asset             string
	Network           string
	Address           string
	WalletId          string
	AccountIdentifier string
}

// PlatformTransactionParams captures any Prime transaction type for recording
// in the backend (conversions, transfers, rewards, internal movements, etc.).
type PlatformTransactionParams struct {
	TransactionId   string
	Type            string // CONVERSION, TRANSFER, REWARD, INTERNAL_DEPOSIT, etc.
	Status          string
	Symbol          string
	Amount          string
	Network         string
	WalletId        string
	TransactionTime time.Time         // effective time of the Prime transaction
	Metadata        map[string]string // additional context from Prime
}

// ConversionParams captures a Prime conversion (e.g. USD -> USDC).
type ConversionParams struct {
	TransactionId     string
	Status            string
	SourceSymbol      string
	SourceAmount      string
	DestinationSymbol string
	DestinationAmount string // same as SourceAmount if not separately provided
	SourceWalletId    string
	DestWalletId      string
	Network           string
	Fees              string
	FeeSymbol         string
	TransactionTime   time.Time
}

// WithdrawalFromWalletParams captures a pending withdrawal debited directly from the Prime wallet
// (for withdrawals initiated outside this system, e.g. OTHER_TRANSACTION_STATUS).
type WithdrawalFromWalletParams struct {
	TransactionId      string
	Status             string
	Symbol             string
	PrimeApiSymbol     string
	Amount             decimal.Decimal
	WalletId           string
	DestinationAddress string
	IdempotencyKey     string
	TransactionTime    time.Time
}

// FailedWithdrawalPlatformParams captures a failed withdrawal that could not be matched
// to a user. Both the synthetic initiation (wallet→pending) and reversal (pending→wallet)
// are recorded so the ledger contains a full audit trail with all Prime metadata.
type FailedWithdrawalPlatformParams struct {
	TransactionId      string
	Status             string
	Symbol             string
	PrimeApiSymbol     string
	Amount             decimal.Decimal
	WalletId           string
	DestinationAddress string
	IdempotencyKey     string
	TransactionTime    time.Time
}

// WithdrawalConfirmDirectParams for a confirmed withdrawal debited directly from the user
// (with overdraft) when no prior pending phase exists.
type WithdrawalConfirmDirectParams struct {
	UserId             string
	Asset              string
	Amount             decimal.Decimal
	WalletId           string
	ExternalTxId       string
	WithdrawalRef      string
	DestinationAddress string
	Network            string
	PrimeTxId          string
	IdempotencyKey     string
	TransactionTime    time.Time
}

// LedgerStore defines the contract that every backend (SQLite, Formance, ...) must satisfy.
type LedgerStore interface {
	// --- Users ---
	GetUsers(ctx context.Context) ([]models.User, error)
	GetUserById(ctx context.Context, userId string) (*models.User, error)
	GetUserByEmail(ctx context.Context, email string) (*models.User, error)
	CreateUser(ctx context.Context, userId, name, email string) (*models.User, error)

	// --- Addresses ---
	StoreAddress(ctx context.Context, params StoreAddressParams) (*models.Address, error)
	GetAddresses(ctx context.Context, userId, asset, network string) ([]models.Address, error)
	GetAllUserAddresses(ctx context.Context, userId string) ([]models.Address, error)
	FindUserByAddress(ctx context.Context, address string) (*models.User, *models.Address, error)

	// --- Balances ---
	GetUserBalance(ctx context.Context, userId, asset string) (decimal.Decimal, error)
	GetAllUserBalances(ctx context.Context, userId string) ([]models.AccountBalance, error)

	// --- Transactions ---
	ProcessDepositPending(ctx context.Context, asset, walletId string, amount decimal.Decimal, transactionId, depositAddress string) error
	ConfirmDeposit(ctx context.Context, address, asset string, amount decimal.Decimal, transactionId string) error
	ProcessDeposit(ctx context.Context, address, asset string, amount decimal.Decimal, transactionId string) error
	ProcessWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, transactionId string) error
	ProcessWithdrawalFromWallet(ctx context.Context, params WithdrawalFromWalletParams) error
	ConfirmWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, withdrawalRef, externalTxId string) error
	ConfirmWithdrawalDirect(ctx context.Context, params WithdrawalConfirmDirectParams) error
	ReverseWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, originalTxId string) error
	HasPendingWithdrawal(ctx context.Context, withdrawalRef string) (bool, error)
	RevertTransaction(ctx context.Context, reference string) error
	RecordFailedWithdrawalPlatform(ctx context.Context, params FailedWithdrawalPlatformParams) error
	RecordPlatformTransaction(ctx context.Context, params PlatformTransactionParams) error
	RecordConversion(ctx context.Context, params ConversionParams) error
	GetTransactionHistory(ctx context.Context, userId, asset string, limit, offset int) ([]models.Transaction, error)
	GetMostRecentTransactionTime(ctx context.Context) (time.Time, error)
	ReconcileUserBalance(ctx context.Context, userId, asset string) error

	// --- Lifecycle ---
	Close()
}
