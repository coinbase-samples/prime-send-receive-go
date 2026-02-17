package formance

import (
	"context"
	"fmt"
	"strings"
	"time"

	"prime-send-receive-go/internal/models"
	"prime-send-receive-go/internal/store"

	"github.com/formancehq/formance-sdk-go/v3/pkg/models/operations"
	"github.com/formancehq/formance-sdk-go/v3/pkg/models/shared"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// Numscript templates -- match the chart YAML event definitions exactly.
// All metadata is set inside the script via set_tx_meta() so the Formance
// transaction is fully self-describing.
// ---------------------------------------------------------------------------

const numscriptDepositPending = `vars {
  asset $asset
  number $amount
  account $portfolio_id
  account $wallet_id
  string $external_tx_id
  string $deposit_address
  string $asset_symbol
  string $prime_status
}

send [$asset $amount] (
  source = @prime:portfolio:$portfolio_id:wallets:$wallet_id allowing unbounded overdraft
  destination = @prime:portfolio:$portfolio_id:deposits:pending
)

set_tx_meta("event_type", "deposit_pending")
set_tx_meta("external_tx_id", $external_tx_id)
set_tx_meta("deposit_address", $deposit_address)
set_tx_meta("asset_symbol", $asset_symbol)
set_tx_meta("prime_status", $prime_status)
`

const numscriptDepositConfirmed = `vars {
  asset $asset
  number $amount
  account $user_id
  account $portfolio_id
  string $external_tx_id
  string $deposit_address
  string $asset_symbol
  string $prime_status
}

send [$asset $amount] (
  source = @prime:portfolio:$portfolio_id:deposits:pending
  destination = @users:$user_id
)

set_tx_meta("event_type", "deposit_confirmed")
set_tx_meta("external_tx_id", $external_tx_id)
set_tx_meta("deposit_address", $deposit_address)
set_tx_meta("asset_symbol", $asset_symbol)
set_tx_meta("prime_status", $prime_status)
`

const numscriptDepositReceived = `vars {
  asset $asset
  number $amount
  account $user_id
  account $portfolio_id
  account $wallet_id
  string $external_tx_id
  string $deposit_address
  string $prime_status
  string $asset_symbol
  string $network
  string $user_name
  string $user_email
  string $prime_wallet_id
  string $account_identifier
  string $prime_api_symbol
  string $canonical_symbol
  string $amount_human
  string $prime_tx_id
  string $source_address
  string $source_type
  string $network_fees
  string $fees
  string $blockchain_ids
  string $prime_created_at
  string $prime_completed_at
}

send [$asset $amount] (
  source = @prime:portfolio:$portfolio_id:wallets:$wallet_id allowing unbounded overdraft
  destination = @users:$user_id
)

set_tx_meta("event_type", "deposit_received")
set_tx_meta("external_tx_id", $external_tx_id)
set_tx_meta("deposit_address", $deposit_address)
set_tx_meta("prime_status", $prime_status)
set_tx_meta("asset_symbol", $asset_symbol)
set_tx_meta("network", $network)
set_tx_meta("user_name", $user_name)
set_tx_meta("user_email", $user_email)
set_tx_meta("prime_wallet_id", $prime_wallet_id)
set_tx_meta("account_identifier", $account_identifier)
set_tx_meta("prime_api_symbol", $prime_api_symbol)
set_tx_meta("canonical_symbol", $canonical_symbol)
set_tx_meta("amount_human", $amount_human)
set_tx_meta("prime_tx_id", $prime_tx_id)
set_tx_meta("source_address", $source_address)
set_tx_meta("source_type", $source_type)
set_tx_meta("network_fees", $network_fees)
set_tx_meta("fees", $fees)
set_tx_meta("blockchain_ids", $blockchain_ids)
set_tx_meta("prime_created_at", $prime_created_at)
set_tx_meta("prime_completed_at", $prime_completed_at)
`

const numscriptWithdrawalPendingFromWallet = `vars {
  asset $asset
  number $amount
  account $portfolio_id
  account $wallet_id
  string $external_tx_id
  string $prime_status
  string $asset_symbol
  string $destination_address
  string $idempotency_key
  string $prime_api_symbol
  string $amount_human
}

send [$asset $amount] (
  source = @prime:portfolio:$portfolio_id:wallets:$wallet_id allowing unbounded overdraft
  destination = @prime:portfolio:$portfolio_id:withdrawals:pending
)

set_tx_meta("event_type", "withdrawal_pending_from_wallet")
set_tx_meta("external_tx_id", $external_tx_id)
set_tx_meta("prime_status", $prime_status)
set_tx_meta("asset_symbol", $asset_symbol)
set_tx_meta("destination_address", $destination_address)
set_tx_meta("idempotency_key", $idempotency_key)
set_tx_meta("prime_api_symbol", $prime_api_symbol)
set_tx_meta("amount_human", $amount_human)
`

const numscriptWithdrawalInitiated = `vars {
  asset $asset
  number $amount
  account $user_id
  account $portfolio_id
  string $destination_address
  string $withdrawal_ref
  string $asset_symbol
}

send [$asset $amount] (
  source = @users:$user_id
  destination = @prime:portfolio:$portfolio_id:withdrawals:pending
)

set_tx_meta("event_type", "withdrawal_initiated")
set_tx_meta("destination_address", $destination_address)
set_tx_meta("withdrawal_ref", $withdrawal_ref)
set_tx_meta("asset_symbol", $asset_symbol)
`

const numscriptWithdrawalFailedReversal = `vars {
  asset $asset
  number $amount
  account $user_id
  account $portfolio_id
  string $external_tx_id
  string $prime_status
  string $withdrawal_ref
  string $reversal_ref
}

send [$asset $amount] (
  source = @prime:portfolio:$portfolio_id:withdrawals:pending allowing unbounded overdraft
  destination = @users:$user_id
)

set_tx_meta("event_type", "withdrawal_failed_reversal")
set_tx_meta("external_tx_id", $external_tx_id)
set_tx_meta("prime_status", $prime_status)
set_tx_meta("withdrawal_ref", $withdrawal_ref)
set_tx_meta("reversal_ref", $reversal_ref)
`

// numscriptWithdrawalFailedPlatformRoundTrip records a failed withdrawal that
// could not be matched to any user as a single atomic transaction with two
// postings: wallet→pending (initiation) then pending→wallet (reversal).
// Net balance impact is zero; the ledger keeps a full audit trail.
const numscriptWithdrawalFailedPlatformRoundTrip = `vars {
  asset $asset
  number $amount
  account $portfolio_id
  account $wallet_id
  string $external_tx_id
  string $prime_status
  string $asset_symbol
  string $amount_human
  string $destination_address
  string $idempotency_key
  string $prime_api_symbol
}

send [$asset $amount] (
  source = @prime:portfolio:$portfolio_id:wallets:$wallet_id allowing unbounded overdraft
  destination = @prime:portfolio:$portfolio_id:withdrawals:pending
)

send [$asset $amount] (
  source = @prime:portfolio:$portfolio_id:withdrawals:pending
  destination = @prime:portfolio:$portfolio_id:wallets:$wallet_id
)

set_tx_meta("event_type", "withdrawal_failed_platform_round_trip")
set_tx_meta("external_tx_id", $external_tx_id)
set_tx_meta("prime_status", $prime_status)
set_tx_meta("asset_symbol", $asset_symbol)
set_tx_meta("amount_human", $amount_human)
set_tx_meta("destination_address", $destination_address)
set_tx_meta("idempotency_key", $idempotency_key)
set_tx_meta("prime_api_symbol", $prime_api_symbol)
`

const numscriptWithdrawalConfirmed = `vars {
  asset $asset
  number $amount
  account $portfolio_id
  account $wallet_id
  string $external_tx_id
  string $prime_status
  string $withdrawal_ref
  string $asset_symbol
  string $amount_human
  string $destination_address
  string $user_id
  string $network
  string $prime_tx_id
}

send [$asset $amount] (
  source = @prime:portfolio:$portfolio_id:withdrawals:pending
  destination = @prime:portfolio:$portfolio_id:wallets:$wallet_id
)

set_tx_meta("event_type", "withdrawal_confirmed")
set_tx_meta("external_tx_id", $external_tx_id)
set_tx_meta("prime_status", $prime_status)
set_tx_meta("withdrawal_ref", $withdrawal_ref)
set_tx_meta("asset_symbol", $asset_symbol)
set_tx_meta("amount_human", $amount_human)
set_tx_meta("destination_address", $destination_address)
set_tx_meta("user_id", $user_id)
set_tx_meta("network", $network)
set_tx_meta("prime_tx_id", $prime_tx_id)
`

const numscriptWithdrawalConfirmedDirect = `vars {
  asset $asset
  number $amount
  account $user_id
  account $portfolio_id
  account $wallet_id
  string $external_tx_id
  string $prime_status
  string $withdrawal_ref
  string $asset_symbol
  string $amount_human
  string $destination_address
  string $network
  string $prime_tx_id
  string $idempotency_key
}

send [$asset $amount] (
  source = @users:$user_id allowing unbounded overdraft
  destination = @prime:portfolio:$portfolio_id:wallets:$wallet_id
)

set_tx_meta("event_type", "withdrawal_confirmed_direct")
set_tx_meta("external_tx_id", $external_tx_id)
set_tx_meta("prime_status", $prime_status)
set_tx_meta("withdrawal_ref", $withdrawal_ref)
set_tx_meta("asset_symbol", $asset_symbol)
set_tx_meta("amount_human", $amount_human)
set_tx_meta("destination_address", $destination_address)
set_tx_meta("user_id", $user_id)
set_tx_meta("network", $network)
set_tx_meta("prime_tx_id", $prime_tx_id)
set_tx_meta("idempotency_key", $idempotency_key)
`

const numscriptConversion = `vars {
  asset $source_asset
  number $source_amount
  asset $destination_asset
  number $destination_amount
  account $portfolio_id
  account $source_wallet_id
  account $destination_wallet_id
  string $external_tx_id
  string $prime_status
  string $source_symbol
  string $destination_symbol
  string $amount_human
  string $fees
  string $fee_symbol
}

send [$source_asset $source_amount] (
  source = @prime:portfolio:$portfolio_id:conversions allowing unbounded overdraft
  destination = @prime:portfolio:$portfolio_id:wallets:$source_wallet_id
)

send [$destination_asset $destination_amount] (
  source = @prime:portfolio:$portfolio_id:wallets:$destination_wallet_id allowing unbounded overdraft
  destination = @prime:portfolio:$portfolio_id:conversions
)

set_tx_meta("event_type", "conversion")
set_tx_meta("external_tx_id", $external_tx_id)
set_tx_meta("prime_status", $prime_status)
set_tx_meta("source_symbol", $source_symbol)
set_tx_meta("destination_symbol", $destination_symbol)
set_tx_meta("amount_human", $amount_human)
set_tx_meta("fees", $fees)
set_tx_meta("fee_symbol", $fee_symbol)
`

const numscriptPlatformTransaction = `vars {
  asset $asset
  number $amount
  account $portfolio_id
  account $wallet_id
  account $platform_user_id
  string $external_tx_id
  string $event_type
  string $prime_status
  string $transaction_type
  string $asset_symbol
  string $amount_human
  string $network_raw
  string $prime_wallet_id
}

send [$asset $amount] (
  source = @prime:portfolio:$portfolio_id:wallets:$wallet_id allowing unbounded overdraft
  destination = @users:$platform_user_id
)

set_tx_meta("event_type", $event_type)
set_tx_meta("external_tx_id", $external_tx_id)
set_tx_meta("prime_status", $prime_status)
set_tx_meta("transaction_type", $transaction_type)
set_tx_meta("asset_symbol", $asset_symbol)
set_tx_meta("amount_human", $amount_human)
set_tx_meta("network", $network_raw)
set_tx_meta("prime_wallet_id", $prime_wallet_id)
`

// ---------------------------------------------------------------------------
// Transaction operations
// ---------------------------------------------------------------------------

// ProcessDepositPending parks incoming funds in a pending deposits account.
// This is phase 1 of the two-phase deposit flow (TRANSACTION_IMPORT_PENDING).
func (s *Service) ProcessDepositPending(ctx context.Context, asset, walletId string, amount decimal.Decimal, transactionId, depositAddress string) error {
	fAsset := formanceAsset(asset)
	smallAmt := amount.Shift(int32(precisionFor(asset))).BigInt().String()

	postTx := shared.V2PostTransaction{
		Reference: strPtr(transactionId + "-pending"),
		Script: &shared.V2PostTransactionScript{
			Plain: numscriptDepositPending,
			Vars: map[string]string{
				"asset":           fAsset,
				"amount":          smallAmt,
				"portfolio_id":    s.portfolioID,
				"wallet_id":       walletId,
				"external_tx_id":  transactionId,
				"deposit_address": depositAddress,
				"asset_symbol":    asset,
				"prime_status":    "TRANSACTION_IMPORT_PENDING",
			},
		},
	}
	if pdc := models.GetPrimeDepositContext(ctx); pdc != nil && !pdc.TransactionTime.IsZero() {
		postTx.Timestamp = &pdc.TransactionTime
	}

	_, err := s.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		Ledger:            s.ledger,
		V2PostTransaction: postTx,
	})
	if err != nil {
		if isConflictError(err) {
			return nil // idempotent
		}
		return fmt.Errorf("error recording pending deposit: %w", err)
	}

	zap.L().Info("Deposit pending recorded in Formance",
		zap.String("asset", asset),
		zap.String("amount", amount.String()),
		zap.String("tx_id", transactionId))
	return nil
}

// ConfirmDeposit moves funds from pending deposits to the user's account.
// This is phase 2 of the two-phase deposit flow (TRANSACTION_IMPORTED).
func (s *Service) ConfirmDeposit(ctx context.Context, address, asset string, amount decimal.Decimal, transactionId string) error {
	user, addr, err := s.FindUserByAddress(ctx, address)
	if err != nil {
		return fmt.Errorf("error finding user by address: %w", err)
	}
	var userId, canonicalSymbol string
	if user != nil {
		userId = user.Id
		canonicalSymbol = addr.Asset
	} else {
		userId = "prime-platform-" + s.portfolioID
		canonicalSymbol = normalizeSymbolFallback(asset)
		zap.L().Info("Confirming deposit to platform account (unmapped address)",
			zap.String("address", address))
	}

	fAsset := formanceAsset(canonicalSymbol)
	smallAmt := amount.Shift(int32(precisionFor(canonicalSymbol))).BigInt().String()

	postTx := shared.V2PostTransaction{
		Reference: strPtr(transactionId + "-confirmed"),
		Script: &shared.V2PostTransactionScript{
			Plain: numscriptDepositConfirmed,
			Vars: map[string]string{
				"asset":           fAsset,
				"amount":          smallAmt,
				"user_id":         userId,
				"portfolio_id":    s.portfolioID,
				"external_tx_id":  transactionId,
				"deposit_address": address,
				"asset_symbol":    canonicalSymbol,
				"prime_status":    "TRANSACTION_IMPORTED",
			},
		},
	}
	if pdc := models.GetPrimeDepositContext(ctx); pdc != nil && !pdc.TransactionTime.IsZero() {
		postTx.Timestamp = &pdc.TransactionTime
	}

	_, err = s.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		Ledger:            s.ledger,
		V2PostTransaction: postTx,
	})
	if err != nil {
		if isConflictError(err) {
			return fmt.Errorf("%w: deposit confirmation %s already exists", store.ErrDuplicateTransaction, transactionId)
		}
		return fmt.Errorf("error confirming deposit: %w", err)
	}

	zap.L().Info("Deposit confirmed in Formance (pending to user)",
		zap.String("user_id", userId),
		zap.String("asset", canonicalSymbol),
		zap.String("amount", amount.String()))
	return nil
}

// ProcessDeposit credits a user's network account from the Prime wallet (single-phase, for backward compat).
func (s *Service) ProcessDeposit(ctx context.Context, address, asset string, amount decimal.Decimal, transactionId string) error {
	user, addr, err := s.FindUserByAddress(ctx, address)
	if err != nil {
		return fmt.Errorf("error finding user by address: %w", err)
	}

	// If no user is mapped to this address, credit the platform account.
	canonicalSymbol := asset
	var network, walletId, accountIdentifier, userName, userEmail string
	var userId string

	if user != nil {
		canonicalSymbol = addr.Asset
		userId = user.Id
		network = addr.Network
		walletId = addr.WalletId
		accountIdentifier = addr.AccountIdentifier
		userName = user.Name
		userEmail = user.Email
		if canonicalSymbol != asset {
			zap.L().Info("Using canonical symbol from address index",
				zap.String("prime_symbol", asset),
				zap.String("canonical", canonicalSymbol))
		}
	} else {
		userId = "prime-platform-" + s.portfolioID
		canonicalSymbol = normalizeSymbolFallback(asset)
		network = "platform"
		zap.L().Info("Deposit to unmapped address, crediting platform account",
			zap.String("address", address),
			zap.String("platform_user", userId))
	}

	// Resolve wallet_id from context if not available from address metadata.
	if walletId == "" {
		if pdc := models.GetPrimeDepositContext(ctx); pdc != nil && pdc.WalletId != "" {
			walletId = pdc.WalletId
		}
	}
	// Final fallback: use a placeholder to avoid empty Numscript account segments.
	if walletId == "" {
		walletId = "unknown"
	}
	if network == "" {
		network = "unknown"
	}

	fAsset := formanceAsset(canonicalSymbol)
	smallAmt := amount.Shift(int32(precisionFor(canonicalSymbol))).BigInt().String()

	vars := map[string]string{
		"asset":              fAsset,
		"amount":             smallAmt,
		"user_id":            userId,
		"network":            network,
		"portfolio_id":       s.portfolioID,
		"wallet_id":          walletId,
		"external_tx_id":     transactionId,
		"deposit_address":    address,
		"prime_status":       "TRANSACTION_IMPORTED",
		"asset_symbol":       canonicalSymbol,
		"user_name":          userName,
		"user_email":         userEmail,
		"prime_wallet_id":    walletId,
		"prime_network":      network,
		"account_identifier": accountIdentifier,
		"prime_api_symbol":   asset,
		"canonical_symbol":   canonicalSymbol,
		"amount_human":       amount.String(),
		"prime_tx_id":        "",
		"source_address":     "",
		"source_type":        "",
		"network_fees":       "",
		"fees":               "",
		"blockchain_ids":     "",
		"prime_created_at":   "",
		"prime_completed_at": "",
	}

	// Enrich with full Prime transaction data if available via context.
	if pdc := models.GetPrimeDepositContext(ctx); pdc != nil {
		vars["prime_tx_id"] = pdc.TransactionId
		vars["source_address"] = pdc.SourceAddress
		vars["source_type"] = pdc.SourceType
		vars["network_fees"] = pdc.NetworkFees
		vars["fees"] = pdc.Fees
		vars["prime_created_at"] = pdc.CreatedAt
		vars["prime_completed_at"] = pdc.CompletedAt
		if len(pdc.BlockchainIds) > 0 {
			vars["blockchain_ids"] = strings.Join(pdc.BlockchainIds, ",")
		}
	}

	// Use the Prime transaction timestamp as the effective date if available.
	postTx := shared.V2PostTransaction{
		Reference: strPtr(transactionId),
		Script: &shared.V2PostTransactionScript{
			Plain: numscriptDepositReceived,
			Vars:  vars,
		},
	}
	if pdc := models.GetPrimeDepositContext(ctx); pdc != nil && !pdc.TransactionTime.IsZero() {
		postTx.Timestamp = &pdc.TransactionTime
	}

	_, err = s.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		Ledger:            s.ledger,
		V2PostTransaction: postTx,
	})
	if err != nil {
		if isConflictError(err) {
			return fmt.Errorf("%w: external_transaction_id %s already exists", store.ErrDuplicateTransaction, transactionId)
		}
		return fmt.Errorf("error processing deposit transaction: %w", err)
	}

	zap.L().Info("Deposit processed in Formance",
		zap.String("user_id", userId),
		zap.String("asset", canonicalSymbol),
		zap.String("amount", amount.String()),
		zap.String("network", network))
	return nil
}

// ProcessWithdrawalFromWallet debits the Prime wallet (with overdraft) and moves
// funds to the withdrawal pending account. Used for withdrawals initiated outside
// this system (e.g. OTHER_TRANSACTION_STATUS) where no user account was credited.
func (s *Service) ProcessWithdrawalFromWallet(ctx context.Context, params store.WithdrawalFromWalletParams) error {
	fAsset := formanceAsset(params.Symbol)
	smallAmt := params.Amount.Shift(int32(precisionFor(params.Symbol))).BigInt().String()

	postTx := shared.V2PostTransaction{
		Reference: strPtr(params.TransactionId),
		Script: &shared.V2PostTransactionScript{
			Plain: numscriptWithdrawalPendingFromWallet,
			Vars: map[string]string{
				"asset":               fAsset,
				"amount":              smallAmt,
				"portfolio_id":        s.portfolioID,
				"wallet_id":           params.WalletId,
				"external_tx_id":      params.TransactionId,
				"prime_status":        params.Status,
				"asset_symbol":        params.Symbol,
				"destination_address": params.DestinationAddress,
				"idempotency_key":     params.IdempotencyKey,
				"prime_api_symbol":    params.PrimeApiSymbol,
				"amount_human":        params.Amount.String(),
			},
		},
	}
	if !params.TransactionTime.IsZero() {
		postTx.Timestamp = &params.TransactionTime
	}

	_, err := s.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		Ledger:            s.ledger,
		V2PostTransaction: postTx,
	})
	if err != nil {
		if isConflictError(err) {
			return nil
		}
		return fmt.Errorf("error processing wallet withdrawal: %w", err)
	}

	zap.L().Info("Pending withdrawal from wallet recorded in Formance",
		zap.String("asset", params.Symbol),
		zap.String("amount", params.Amount.String()),
		zap.String("wallet_id", params.WalletId))
	return nil
}

// ProcessWithdrawal debits a user's account into the pending withdrawal account.
func (s *Service) ProcessWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, transactionId string) error {
	fAsset := formanceAsset(asset)
	smallAmt := amount.Shift(int32(precisionFor(asset))).BigInt().String()

	_, err := s.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		Ledger: s.ledger,
		V2PostTransaction: shared.V2PostTransaction{
			Reference: strPtr(transactionId),
			Script: &shared.V2PostTransactionScript{
				Plain: numscriptWithdrawalInitiated,
				Vars: map[string]string{
					"asset":               fAsset,
					"amount":              smallAmt,
					"user_id":             userId,
					"portfolio_id":        s.portfolioID,
					"destination_address": "",
					"withdrawal_ref":      transactionId,
					"asset_symbol":        asset,
				},
			},
		},
	})
	if err != nil {
		if isConflictError(err) {
			return fmt.Errorf("%w: external_transaction_id %s already exists", store.ErrDuplicateTransaction, transactionId)
		}
		return fmt.Errorf("error processing withdrawal transaction: %w", err)
	}

	zap.L().Info("Withdrawal processed in Formance",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.String("amount", amount.String()))
	return nil
}

// ConfirmWithdrawal settles a completed withdrawal by moving funds from the
// pending account to the Prime portfolio wallet. This is the third phase of
// the three-phase withdrawal flow (WITHDRAWAL_CONFIRMED in the chart).
func (s *Service) ConfirmWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, withdrawalRef, externalTxId string) error {
	// Resolve wallet_id for this asset from the user's addresses.
	addrs, err := s.GetAllUserAddresses(ctx, userId)
	if err != nil {
		return fmt.Errorf("error resolving wallet for confirmation: %w", err)
	}
	walletId := ""
	for _, a := range addrs {
		if a.Asset == asset && a.WalletId != "" {
			walletId = a.WalletId
			break
		}
	}
	if walletId == "" {
		zap.L().Warn("No wallet_id found for withdrawal confirmation, using fallback",
			zap.String("user_id", userId), zap.String("asset", asset))
		walletId = "unknown"
	}

	fAsset := formanceAsset(asset)
	smallAmt := amount.Shift(int32(precisionFor(asset))).BigInt().String()
	confirmRef := externalTxId + "-confirmed"

	postTx := shared.V2PostTransaction{
		Reference: strPtr(confirmRef),
		Script: &shared.V2PostTransactionScript{
			Plain: numscriptWithdrawalConfirmed,
			Vars: map[string]string{
				"asset":               fAsset,
				"amount":              smallAmt,
				"portfolio_id":        s.portfolioID,
				"wallet_id":           walletId,
				"external_tx_id":      externalTxId,
				"prime_status":        "TRANSACTION_DONE",
				"withdrawal_ref":      withdrawalRef,
				"asset_symbol":        asset,
				"amount_human":        amount.String(),
				"destination_address": "",
				"user_id":             userId,
				"network":             "",
				"prime_tx_id":         "",
			},
		},
	}

	_, err = s.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		Ledger:            s.ledger,
		V2PostTransaction: postTx,
	})
	if err != nil {
		if isConflictError(err) {
			return fmt.Errorf("%w: confirmation %s already exists", store.ErrDuplicateTransaction, confirmRef)
		}
		return fmt.Errorf("error confirming withdrawal: %w", err)
	}

	zap.L().Info("Withdrawal confirmed in Formance (pending settled to portfolio)",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.String("amount", amount.String()),
		zap.String("external_tx_id", externalTxId))
	return nil
}

// ConfirmWithdrawalDirect debits the user directly (with overdraft) to the portfolio
// wallet. Used for confirmed withdrawals where no pending phase existed.
// Users CAN go negative -- this is detectable via balance queries.
func (s *Service) ConfirmWithdrawalDirect(ctx context.Context, params store.WithdrawalConfirmDirectParams) error {
	fAsset := formanceAsset(params.Asset)
	smallAmt := params.Amount.Shift(int32(precisionFor(params.Asset))).BigInt().String()

	walletId := params.WalletId
	if walletId == "" {
		walletId = "unknown"
	}

	postTx := shared.V2PostTransaction{
		Reference: strPtr(params.ExternalTxId + "-direct"),
		Script: &shared.V2PostTransactionScript{
			Plain: numscriptWithdrawalConfirmedDirect,
			Vars: map[string]string{
				"asset":               fAsset,
				"amount":              smallAmt,
				"user_id":             params.UserId,
				"portfolio_id":        s.portfolioID,
				"wallet_id":           walletId,
				"external_tx_id":      params.ExternalTxId,
				"prime_status":        "TRANSACTION_DONE",
				"withdrawal_ref":      params.WithdrawalRef,
				"asset_symbol":        params.Asset,
				"amount_human":        params.Amount.String(),
				"destination_address": params.DestinationAddress,
				"network":             params.Network,
				"prime_tx_id":         params.PrimeTxId,
				"idempotency_key":     params.IdempotencyKey,
			},
		},
	}
	if !params.TransactionTime.IsZero() {
		postTx.Timestamp = &params.TransactionTime
	}

	_, err := s.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		Ledger:            s.ledger,
		V2PostTransaction: postTx,
	})
	if err != nil {
		if isConflictError(err) {
			return nil
		}
		return fmt.Errorf("error recording direct confirmed withdrawal: %w", err)
	}

	zap.L().Info("Withdrawal confirmed directly in Formance (user -> wallet with overdraft)",
		zap.String("user_id", params.UserId),
		zap.String("asset", params.Asset),
		zap.String("amount", params.Amount.String()),
		zap.String("destination", params.DestinationAddress))
	return nil
}

// HasPendingWithdrawal checks if a WITHDRAWAL_INITIATED or WITHDRAWAL_PENDING_FROM_WALLET
// transaction exists for the given withdrawal reference by querying Formance metadata.
func (s *Service) HasPendingWithdrawal(ctx context.Context, withdrawalRef string) (bool, error) {
	pageSize := int64(1)
	resp, err := s.client.Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
		Ledger:   s.ledger,
		PageSize: &pageSize,
		RequestBody: map[string]any{
			"$match": map[string]any{
				"metadata[withdrawal_ref]": withdrawalRef,
			},
		},
	})
	if err != nil {
		return false, fmt.Errorf("failed to check for pending withdrawal: %w", err)
	}

	if len(resp.V2TransactionsCursorResponse.Cursor.Data) > 0 {
		tx := resp.V2TransactionsCursorResponse.Cursor.Data[0]
		eventType := tx.Metadata["event_type"]
		zap.L().Debug("Found existing withdrawal transaction",
			zap.String("withdrawal_ref", withdrawalRef),
			zap.String("event_type", eventType),
			zap.Bool("reverted", tx.Reverted))
		// Only count as pending if not already reverted.
		return !tx.Reverted, nil
	}

	return false, nil
}

// RevertTransaction uses Formance's native RevertTransaction API to atomically
// undo a transaction by its withdrawal_ref metadata. This creates a mirror posting
// that exactly reverses the original. If already reverted, returns nil.
func (s *Service) RevertTransaction(ctx context.Context, reference string) error {
	zap.L().Info("Reverting transaction in Formance",
		zap.String("withdrawal_ref", reference))

	// Look up the WITHDRAWAL_INITIATED transaction by its withdrawal_ref metadata.
	pageSize := int64(1)
	resp, err := s.client.Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
		Ledger:   s.ledger,
		PageSize: &pageSize,
		RequestBody: map[string]any{
			"$match": map[string]any{
				"metadata[withdrawal_ref]": reference,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to find transaction by withdrawal_ref %s: %w", reference, err)
	}
	if len(resp.V2TransactionsCursorResponse.Cursor.Data) == 0 {
		return fmt.Errorf("no transaction found with withdrawal_ref %s", reference)
	}

	tx := resp.V2TransactionsCursorResponse.Cursor.Data[0]

	// Already reverted? Nothing to do.
	if tx.Reverted {
		zap.L().Info("Transaction already reverted",
			zap.String("withdrawal_ref", reference),
			zap.String("tx_id", tx.ID.String()))
		return nil
	}

	_, err = s.client.Ledger.V2.RevertTransaction(ctx, operations.V2RevertTransactionRequest{
		Ledger:          s.ledger,
		ID:              tx.ID,
		AtEffectiveDate: ptrBool(true),
	})
	if err != nil {
		// ALREADY_REVERT is also fine -- race condition between CLI and listener.
		if isConflictError(err) || isAlreadyRevertedError(err) {
			zap.L().Info("Transaction already reverted (race)",
				zap.String("withdrawal_ref", reference))
			return nil
		}
		return fmt.Errorf("failed to revert transaction %s: %w", reference, err)
	}

	zap.L().Info("Transaction reverted in Formance",
		zap.String("withdrawal_ref", reference),
		zap.String("tx_id", tx.ID.String()))
	return nil
}

// RecordFailedWithdrawalPlatform records a failed withdrawal that could not be
// matched to any user as a single atomic Formance transaction with two postings:
//
//	Posting 1 (initiation): wallet → pending
//	Posting 2 (reversal):   pending → wallet
//
// Net balance impact is zero. The ledger keeps a full audit trail with all
// Prime metadata attached.
func (s *Service) RecordFailedWithdrawalPlatform(ctx context.Context, params store.FailedWithdrawalPlatformParams) error {
	fAsset := formanceAsset(params.Symbol)
	smallAmt := params.Amount.Shift(int32(precisionFor(params.Symbol))).BigInt().String()

	postTx := shared.V2PostTransaction{
		Reference: strPtr(params.TransactionId),
		Script: &shared.V2PostTransactionScript{
			Plain: numscriptWithdrawalFailedPlatformRoundTrip,
			Vars: map[string]string{
				"asset":               fAsset,
				"amount":              smallAmt,
				"portfolio_id":        s.portfolioID,
				"wallet_id":           params.WalletId,
				"external_tx_id":      params.TransactionId,
				"prime_status":        params.Status,
				"asset_symbol":        params.Symbol,
				"amount_human":        params.Amount.String(),
				"destination_address": params.DestinationAddress,
				"idempotency_key":     params.IdempotencyKey,
				"prime_api_symbol":    params.PrimeApiSymbol,
			},
		},
	}
	if !params.TransactionTime.IsZero() {
		postTx.Timestamp = &params.TransactionTime
	}

	_, err := s.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		Ledger:            s.ledger,
		V2PostTransaction: postTx,
	})
	if err != nil {
		if isConflictError(err) {
			return nil
		}
		return fmt.Errorf("failed to record platform-level failed withdrawal: %w", err)
	}

	zap.L().Info("Failed withdrawal round-trip recorded (2 postings, 1 tx)",
		zap.String("transaction_id", params.TransactionId),
		zap.String("status", params.Status),
		zap.String("asset", params.Symbol),
		zap.String("amount", params.Amount.String()))

	return nil
}

// ReverseWithdrawal credits back a failed withdrawal from pending to user.
func (s *Service) ReverseWithdrawal(ctx context.Context, userId, asset string, amount decimal.Decimal, originalTxId string) error {
	reversalRef := originalTxId + "-reversal"

	fAsset := formanceAsset(asset)
	smallAmt := amount.Shift(int32(precisionFor(asset))).BigInt().String()

	_, err := s.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		Ledger: s.ledger,
		V2PostTransaction: shared.V2PostTransaction{
			Reference: strPtr(reversalRef),
			Script: &shared.V2PostTransactionScript{
				Plain: numscriptWithdrawalFailedReversal,
				Vars: map[string]string{
					"asset":          fAsset,
					"amount":         smallAmt,
					"user_id":        userId,
					"portfolio_id":   s.portfolioID,
					"external_tx_id": originalTxId,
					"prime_status":   "TRANSACTION_FAILED",
					"withdrawal_ref": originalTxId,
					"reversal_ref":   reversalRef,
				},
			},
		},
	})
	if err != nil {
		if isConflictError(err) {
			return fmt.Errorf("%w: reversal %s already exists", store.ErrDuplicateTransaction, reversalRef)
		}
		return fmt.Errorf("error reversing withdrawal: %w", err)
	}

	zap.L().Info("Withdrawal reversed in Formance",
		zap.String("user_id", userId),
		zap.String("asset", asset),
		zap.String("amount", amount.String()),
		zap.String("reversal_ref", reversalRef))
	return nil
}

// ---------------------------------------------------------------------------
// Query operations
// ---------------------------------------------------------------------------

// RecordPlatformTransaction records a platform-level Prime transaction (conversion,
// transfer, reward, etc.) as a proper double-entry in the Formance ledger:
// debit from the portfolio wallet, credit to the platform user account.
func (s *Service) RecordPlatformTransaction(ctx context.Context, params store.PlatformTransactionParams) error {
	fAsset := formanceAsset(params.Symbol)
	amt, err := decimal.NewFromString(params.Amount)
	if err != nil {
		return fmt.Errorf("invalid amount %q: %w", params.Amount, err)
	}
	if amt.IsNegative() {
		amt = amt.Neg()
	}
	smallAmt := amt.Shift(int32(precisionFor(params.Symbol))).BigInt().String()
	eventType := strings.ToLower(params.Type)

	platformUserId := "prime-platform-" + s.portfolioID

	postTx := shared.V2PostTransaction{
		Reference: strPtr(params.TransactionId),
		Script: &shared.V2PostTransactionScript{
			Plain: numscriptPlatformTransaction,
			Vars: map[string]string{
				"asset":            fAsset,
				"amount":           smallAmt,
				"portfolio_id":     s.portfolioID,
				"wallet_id":        params.WalletId,
				"platform_user_id": platformUserId,
				"external_tx_id":   params.TransactionId,
				"event_type":       eventType,
				"prime_status":     params.Status,
				"transaction_type": params.Type,
				"asset_symbol":     params.Symbol,
				"amount_human":     params.Amount,
				"network_raw":      params.Network,
				"prime_wallet_id":  params.WalletId,
			},
		},
	}
	if !params.TransactionTime.IsZero() {
		postTx.Timestamp = &params.TransactionTime
	}

	_, err = s.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		Ledger:            s.ledger,
		V2PostTransaction: postTx,
	})
	if err != nil {
		if isConflictError(err) {
			return nil // idempotent
		}
		return fmt.Errorf("failed to record platform transaction: %w", err)
	}

	zap.L().Info("Platform transaction recorded in Formance",
		zap.String("type", params.Type),
		zap.String("symbol", params.Symbol),
		zap.String("amount", params.Amount),
		zap.String("tx_id", params.TransactionId))
	return nil
}

// RecordConversion records a Prime conversion (e.g. USD -> USDC) as a two-leg
// Numscript transaction: source asset debited from source wallet into a conversion
// account, destination asset credited from conversion account to destination wallet.
func (s *Service) RecordConversion(ctx context.Context, params store.ConversionParams) error {
	srcAsset := formanceAsset(params.SourceSymbol)
	dstAsset := formanceAsset(params.DestinationSymbol)

	srcAmt, err := decimal.NewFromString(params.SourceAmount)
	if err != nil {
		return fmt.Errorf("invalid source amount: %w", err)
	}
	if srcAmt.IsNegative() {
		srcAmt = srcAmt.Neg()
	}
	srcSmall := srcAmt.Shift(int32(precisionFor(params.SourceSymbol))).BigInt().String()

	dstAmount := params.DestinationAmount
	if dstAmount == "" {
		dstAmount = params.SourceAmount
	}
	dstAmt, err := decimal.NewFromString(dstAmount)
	if err != nil {
		return fmt.Errorf("invalid destination amount: %w", err)
	}
	if dstAmt.IsNegative() {
		dstAmt = dstAmt.Neg()
	}
	dstSmall := dstAmt.Shift(int32(precisionFor(params.DestinationSymbol))).BigInt().String()

	postTx := shared.V2PostTransaction{
		Reference: strPtr(params.TransactionId),
		Script: &shared.V2PostTransactionScript{
			Plain: numscriptConversion,
			Vars: map[string]string{
				"source_asset":          srcAsset,
				"source_amount":         srcSmall,
				"destination_asset":     dstAsset,
				"destination_amount":    dstSmall,
				"portfolio_id":          s.portfolioID,
				"source_wallet_id":      params.SourceWalletId,
				"destination_wallet_id": params.DestWalletId,
				"external_tx_id":        params.TransactionId,
				"prime_status":          params.Status,
				"source_symbol":         params.SourceSymbol,
				"destination_symbol":    params.DestinationSymbol,
				"amount_human":          params.SourceAmount,
				"fees":                  params.Fees,
				"fee_symbol":            params.FeeSymbol,
			},
		},
	}
	if !params.TransactionTime.IsZero() {
		postTx.Timestamp = &params.TransactionTime
	}

	_, err = s.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
		Ledger:            s.ledger,
		V2PostTransaction: postTx,
	})
	if err != nil {
		if isConflictError(err) {
			return nil
		}
		return fmt.Errorf("failed to record conversion: %w", err)
	}

	zap.L().Info("Conversion recorded in Formance",
		zap.String("source", params.SourceSymbol),
		zap.String("destination", params.DestinationSymbol),
		zap.String("amount", params.SourceAmount))
	return nil
}

// GetTransactionHistory returns paginated transaction history for a user/asset.
func (s *Service) GetTransactionHistory(ctx context.Context, userId, asset string, limit, offset int) ([]models.Transaction, error) {
	userPrefix := "users:" + userId
	pageSize := int64(limit + offset) // fetch enough to skip offset

	resp, err := s.client.Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
		Ledger:   s.ledger,
		PageSize: &pageSize,
		RequestBody: map[string]any{
			"$or": []any{
				map[string]any{"$match": map[string]any{"source": userPrefix}},
				map[string]any{"$match": map[string]any{"destination": userPrefix}},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list transactions: %w", err)
	}

	var result []models.Transaction
	skipped := 0
	for _, tx := range resp.V2TransactionsCursorResponse.Cursor.Data {
		txAsset := tx.Metadata["asset_symbol"]
		if txAsset != "" && txAsset != asset {
			continue
		}
		if skipped < offset {
			skipped++
			continue
		}

		eventType := tx.Metadata["event_type"]
		txType := "deposit"
		if strings.Contains(eventType, "withdrawal") {
			txType = "withdrawal"
		}

		// Derive signed amount from postings.
		amt := decimal.Zero
		for _, p := range tx.Postings {
			symbol := assetSymbol(p.Asset)
			if symbol != asset {
				continue
			}
			pAmt := bigIntToDecimal(p.Amount, symbol)
			if strings.HasPrefix(p.Source, userPrefix) {
				amt = pAmt.Neg()
			} else if strings.HasPrefix(p.Destination, userPrefix) {
				amt = pAmt
			}
		}

		ref := ""
		if tx.Reference != nil {
			ref = *tx.Reference
		}

		result = append(result, models.Transaction{
			Id:                    fmt.Sprintf("%d", tx.ID),
			UserId:                userId,
			Asset:                 asset,
			TransactionType:       txType,
			Amount:                amt,
			ExternalTransactionId: tx.Metadata["external_tx_id"],
			Reference:             ref,
			Status:                "confirmed",
			CreatedAt:             tx.Timestamp,
			ProcessedAt:           tx.Timestamp,
		})

		if len(result) >= limit {
			break
		}
	}
	return result, nil
}

// GetMostRecentTransactionTime returns the timestamp of the most recent transaction.
func (s *Service) GetMostRecentTransactionTime(ctx context.Context) (time.Time, error) {
	pageSize := int64(1)
	resp, err := s.client.Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
		Ledger:   s.ledger,
		PageSize: &pageSize,
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get recent transaction: %w", err)
	}
	if len(resp.V2TransactionsCursorResponse.Cursor.Data) == 0 {
		return time.Now().Add(-2 * time.Hour), nil
	}
	return resp.V2TransactionsCursorResponse.Cursor.Data[0].Timestamp, nil
}

// ReconcileUserBalance is a no-op in Formance; balances are consistent by construction.
func (s *Service) ReconcileUserBalance(ctx context.Context, userId, asset string) error {
	zap.L().Info("Reconciliation is a no-op in Formance (consistent by construction)",
		zap.String("user_id", userId), zap.String("asset", asset))
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// normalizeSymbolFallback normalizes Prime API symbols (e.g. BASEUSDC -> USDC) for
// deposits that aren't mapped to a user (no address index to resolve from).
func normalizeSymbolFallback(symbol string) string {
	mapping := map[string]string{
		"BASEUSDC": "USDC", "SPLUSDC": "USDC", "AVAUSDC": "USDC", "ARBUSDC": "USDC",
		"BASEETH": "ETH",
	}
	if canonical, ok := mapping[symbol]; ok {
		return canonical
	}
	return symbol
}

func precisionFor(symbol string) int {
	if p, ok := assetPrecision[symbol]; ok {
		return p
	}
	return 6
}

func strPtr(s string) *string  { return &s }
func ptrBool(v bool) *bool     { return &v }

