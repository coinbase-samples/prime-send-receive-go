/**
 * Copyright 2025-present Coinbase Global, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package models

import (
	"time"

	"github.com/shopspring/decimal"
)

// User represents a user in the system
type User struct {
	Id        string    `db:"id"`
	Name      string    `db:"name"`
	Email     string    `db:"email"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

// Address represents a user's deposit address
type Address struct {
	Id                string    `db:"id"`
	UserId            string    `db:"user_id"`
	Asset             string    `db:"asset"`
	Network           string    `db:"network"`
	Address           string    `db:"address"`
	WalletId          string    `db:"wallet_id"`
	AccountIdentifier string    `db:"account_identifier"`
	CreatedAt         time.Time `db:"created_at"`
}

// AccountBalance represents current balance state (hot data)
type AccountBalance struct {
	Id                string          `db:"id"`
	UserId            string          `db:"user_id"`
	Asset             string          `db:"asset"`
	Balance           decimal.Decimal `db:"balance"`
	LastTransactionId string          `db:"last_transaction_id"`
	Version           int64           `db:"version"`
	UpdatedAt         time.Time       `db:"updated_at"`
}

// Transaction represents immutable transaction history (cold data)
type Transaction struct {
	Id                    string          `db:"id"`
	UserId                string          `db:"user_id"`
	Asset                 string          `db:"asset"`
	TransactionType       string          `db:"transaction_type"`
	Amount                decimal.Decimal `db:"amount"`
	BalanceBefore         decimal.Decimal `db:"balance_before"`
	BalanceAfter          decimal.Decimal `db:"balance_after"`
	ExternalTransactionId string          `db:"external_transaction_id"`
	Address               string          `db:"address"`
	Reference             string          `db:"reference"`
	Status                string          `db:"status"`
	CreatedAt             time.Time       `db:"created_at"`
	ProcessedAt           time.Time       `db:"processed_at"`
}
