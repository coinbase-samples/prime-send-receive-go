package models

// Portfolio represents a Prime portfolio
type Portfolio struct {
	Id   string
	Name string
}

// Wallet represents a Prime wallet
type Wallet struct {
	Id     string
	Name   string
	Symbol string
	Type   string
}

// DepositAddress represents a Prime deposit address
type DepositAddress struct {
	Id      string
	Address string
	Network string
	Asset   string
}

// Withdrawal represents a Prime withdrawal transaction
type Withdrawal struct {
	ActivityId     string
	Asset          string
	Amount         string
	Destination    string
	IdempotencyKey string
}
