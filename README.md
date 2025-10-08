# Prime Send/Receive Go

Prime Send/Receive Go is a deposit and withdrawal management system with Coinbase Prime API integration, designed to work out of the box with Coinbase Prime's scalable deposit address solution. 

This is a sample application; test thoroughly and verify it meets your requirements before using.

## Overview

This system processes crypto deposits and withdrawals by monitoring Prime API transactions and maintaining user balances in a high-performance subledger.

**Core Features:**
- Deposit and withdrawal detection from Prime API
- Withdrawal confirmation tracking via idempotency keys
- Subledger with O(1) balance lookups
- Complete audit trail and transaction history
- Configurable via environment variables

## Setup

### 1. Environment Configuration

Copy and configure environment variables:
```bash
cp .env.example .env
```

Edit `.env` with your Prime API credentials and desired configuration. All settings have sensible defaults except the Prime API credentials which are required.

**Required Environment Variables:**
```bash
# Prime API credentials (required)
PRIME_ACCESS_KEY=your-prime-access-key-here
PRIME_PASSPHRASE=your-prime-passphrase-here
PRIME_SIGNING_KEY=your-prime-signing-key-here
```

**Optional Configuration:**
```bash
# Database configuration
DATABASE_PATH=addresses.db
DB_MAX_OPEN_CONNS=25
DB_MAX_IDLE_CONNS=5
DB_CONN_MAX_LIFETIME=5m
DB_CONN_MAX_IDLE_TIME=30s
DB_PING_TIMEOUT=5s

# Listener configuration
LISTENER_LOOKBACK_WINDOW=6h        # How far back to check for missed transactions
LISTENER_POLLING_INTERVAL=30s      # How often to poll Prime API
LISTENER_CLEANUP_INTERVAL=15m      # How often to clean up processed transaction cache
ASSETS_FILE=assets.yaml            # Asset configuration file
```

**API Usage Notes:**
- The system fetches up to 500 transactions per wallet per polling cycle
- With the default 30-second polling interval, this provides adequate processing time per transaction
- The 6-hour lookback window ensures no transactions are missed between polling cycles
- If you exceed 500 transactions in 30 seconds, consider adjusting the polling interval

### 2. Asset Configuration

Create `assets.yaml` to specify which cryptocurrencies to monitor. You must specify the appropriate network, e.g. `ethereum-mainnet`. A full list of such networks is returned by the [List Assets](https://docs.cdp.coinbase.com/api-reference/prime-api/rest-api/assets/list-assets) REST API:
```yaml
assets:
  - symbol: "USDC"
    network: "ethereum-mainnet"
  - symbol: "BTC"
    network: "bitcoin-mainnet"
  - symbol: "ETH"
    network: "ethereum-mainnet"
  - symbol: "SOL"
    network: "solana-mainnet"
```

### 3. User Configuration

The system creates three dummy users on first run: Alice Johnson, Bob Smith, and Carol Williams. To add your own users, edit `internal/database/service.go` in the `initSchema()` function or insert users directly into the SQLite database after initialization.

### 4. Initial Setup

Generate deposit addresses for provided users:
```bash
go run cmd/setup/main.go
```

This will:
- Initialize the database and run migrations (including user creation)
- Generate unique trading balance deposit addresses per user/asset
- Store addresses in the database

## Running the System

### Deposit & Withdrawal Listener

Start the transaction listener:
```bash
go run cmd/listener/main.go
```

This service:
- Monitors all configured trading balances for new transactions
- Processes deposits automatically when they reach "TRANSACTION_IMPORTED" status
- Processes withdrawals when they reach "TRANSACTION_DONE" status
- Updates user balances
- Handles out-of-order transactions with lookback window

## How the Ledger Works

### Balance Management
- **Current Balances**: Stored in `account_balances` table
- **Transaction History**: Complete audit trail in `transactions` table
- **Atomic Updates**: Balance and transaction record updated together
- **Optimistic Locking**: Prevents race conditions with version control

### Database Schema
```sql
-- Fast balance lookups
account_balances: user_id, asset, balance, version

-- Complete transaction history  
transactions: user_id, asset, type, amount, balance_before, balance_after, external_transaction_id

-- User and address management
users: id, name, email
addresses: user_id, asset, address, wallet_id
```

## Withdrawal Tracking

### Idempotency Key Format
The Coinbase Prime Create Withdrawal API requires a valid UUID when creating a withdrawal. In order to accurately ledger withdrawals within this app, use the following concatenated UUID idempotency key format:
```
{user_id_first_segment}-{uuid_fragment_without_first_segment}
```

**Generation Steps:**
1. Extract first segment from user ID (before first hyphen)
2. Generate a random UUID
3. Replace the UUID's first segment with the user ID's first segment

**Example:**
```bash
# If user ID is: abcd1234-def4-567g-890h-ijklmnop1234
# Generate random UUID: 550e8400-e29b-41d4-a716-446655440000
# Use idempotency key: abcd1234-e29b-41d4-a716-446655440000
```

**Implementation:**
```bash
# Extract user ID prefix
USER_PREFIX=$(echo "$USER_ID" | cut -d'-' -f1)

# Generate random UUID and replace first segment
RANDOM_UUID=$(uuidgen | tr '[:upper:]' '[:lower:]')
WITHDRAWAL_UUID="${USER_PREFIX}-$(echo "$RANDOM_UUID" | cut -d'-' -f2-)"
```

### Withdrawal Processing Flow
1. **Create Withdrawal**: Submit to Prime API with proper idempotency key
2. **Transaction Appears**: Listener detects new withdrawal transaction
3. **Status Check**: Waits for "TRANSACTION_DONE" status
4. **User Matching**: Matches via idempotency key prefix
5. **Balance Update**: Debits user balance atomically

## Monitoring & Debugging

### Check User Balances
```sql
SELECT u.name, ab.asset, ab.balance 
FROM users u 
JOIN account_balances ab ON u.id = ab.user_id
WHERE ab.balance > 0;
```

### View Recent Transactions
```sql
SELECT u.name, t.transaction_type, t.asset, t.amount, t.created_at
FROM transactions t
JOIN users u ON t.user_id = u.id  
ORDER BY t.created_at DESC
LIMIT 10;
```

### Balance Reconciliation
```sql
SELECT 
  ab.user_id,
  ab.asset,
  ab.balance as current_balance,
  COALESCE(SUM(t.amount), 0) as calculated_balance
FROM account_balances ab
LEFT JOIN transactions t ON ab.user_id = t.user_id AND ab.asset = t.asset
GROUP BY ab.user_id, ab.asset;
```