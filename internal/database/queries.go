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

package database

const (
	// User queries
	queryGetActiveUsers = `
		SELECT id, name, email, created_at, updated_at
		FROM users
		WHERE active = 1
		ORDER BY created_at`

	queryInsertUser = `
		INSERT OR IGNORE INTO users (id, name, email) VALUES (?, ?, ?)`

	queryGetUserById = `
		SELECT id, name, email, created_at, updated_at
		FROM users
		WHERE id = ? AND active = 1`

	queryGetUserByEmail = `
		SELECT id, name, email, created_at, updated_at
		FROM users
		WHERE email = ? AND active = 1`

	// Address queries
	queryInsertAddress = `
		INSERT INTO addresses (id, user_id, asset, network, address, wallet_id, account_identifier)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		RETURNING *`

	queryGetUserAddresses = `
		SELECT id, user_id, asset, network, address, wallet_id, account_identifier, created_at
		FROM addresses
		WHERE user_id = ? AND asset = ? AND network = ?
		ORDER BY created_at DESC`

	queryGetAllUserAddresses = `
		SELECT id, user_id, asset, network, address, wallet_id, account_identifier, created_at
		FROM addresses
		WHERE user_id = ?
		ORDER BY asset, created_at DESC`

	queryFindUserByAddress = `
		SELECT u.id, u.name, u.email, u.created_at, u.updated_at,
		       a.id, a.user_id, a.asset, a.network, a.address, a.wallet_id, a.account_identifier, a.created_at
		FROM users u
		JOIN addresses a ON u.id = a.user_id
		WHERE LOWER(a.address) = LOWER(?) AND u.active = 1`

	// Balance queries
	queryGetBalance = `
		SELECT balance 
		FROM account_balances 
		WHERE user_id = ? AND asset = ?`

	queryGetAllUserBalances = `
		SELECT id, user_id, asset, balance, last_transaction_id, version, updated_at
		FROM account_balances 
		WHERE user_id = ? AND balance != 0
		ORDER BY asset`

	queryReconcileBalance = `
		SELECT COALESCE(SUM(amount), 0) as calculated_balance
		FROM transactions 
		WHERE user_id = ? AND asset = ? AND status = 'confirmed'`

	// Transaction queries
	queryCheckDuplicateTransaction = `
		SELECT id FROM transactions WHERE external_transaction_id = ? LIMIT 1`

	queryGetAccountBalance = `
		SELECT id, balance, version 
		FROM account_balances 
		WHERE user_id = ? AND asset = ?`

	queryInsertAccountBalance = `
		INSERT INTO account_balances (id, user_id, asset, balance, version)
		VALUES (?, ?, ?, ?, ?)`

	queryInsertTransaction = `
		INSERT INTO transactions (
			id, user_id, asset, transaction_type, amount, balance_before, balance_after,
			external_transaction_id, address, reference, status, created_at, processed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id, user_id, asset, transaction_type, amount, balance_before, balance_after,
		          external_transaction_id, address, reference, status, created_at, processed_at`

	queryUpdateAccountBalance = `
		UPDATE account_balances 
		SET balance = ?, last_transaction_id = ?, version = version + 1, updated_at = CURRENT_TIMESTAMP
		WHERE user_id = ? AND asset = ? AND version = ?`

	queryInsertJournalEntry = `
		INSERT INTO journal_entries (id, transaction_id, account_type, account_id, debit_amount, credit_amount)
		VALUES (?, ?, ?, ?, ?, ?)`

	queryGetTransactionHistory = `
		SELECT id, user_id, asset, transaction_type, amount, balance_before, balance_after,
		       external_transaction_id, address, reference, status, created_at, processed_at
		FROM transactions 
		WHERE user_id = ? AND asset = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`

	queryGetMostRecentTransactionTime = `
		SELECT MAX(created_at) 
		FROM transactions 
		WHERE external_transaction_id IS NOT NULL AND external_transaction_id != ''`
)
