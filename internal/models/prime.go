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
