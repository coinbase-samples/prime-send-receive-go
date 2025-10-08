package models

import "time"

// WalletInfo represents a trading wallet we monitor for deposits
type WalletInfo struct {
	Id          string `json:"id"`
	AssetSymbol string `json:"asset_symbol"`
}

// PrimeTransferInfo represents the actual transfer_to structure from Prime API
type PrimeTransferInfo struct {
	Type              string `json:"type"`
	Value             string `json:"value"`
	Address           string `json:"address"`
	AccountIdentifier string `json:"account_identifier"`
}

// PrimeTransaction represents a transaction from Prime API with complete fields
type PrimeTransaction struct {
	Id             string            `json:"id"`
	WalletId       string            `json:"wallet_id"`
	Type           string            `json:"type"`
	Status         string            `json:"status"`
	Symbol         string            `json:"symbol"`
	Amount         string            `json:"amount"`
	CreatedAt      time.Time         `json:"created_at"`
	CompletedAt    time.Time         `json:"completed_at"`
	TransferTo     PrimeTransferInfo `json:"transfer_to"`
	TransactionId  string            `json:"transaction_id"`
	Network        string            `json:"network"`
	IdempotencyKey string            `json:"idempotency_key"`
}
