# Prime Send/Receive Go

Prime Send/Receive Go is a deposit and withdrawal management system with Coinbase Prime API integration, designed to work out of the box with Coinbase Prime's scalable deposit address solution. 

This is a sample application; test thoroughly and verify it meets your requirements before using.

## Overview

This system processes crypto deposits and withdrawals by monitoring Prime API transactions and maintaining user balances in a subledger.

**Core Features:**
- Deposit and withdrawal detection from Prime API
- Withdrawal confirmation tracking via idempotency keys
- Subledger with O(1) balance lookups
- Complete audit trail and transaction history
- Configurable via environment variables
- **Pluggable storage backend**: SQLite (default) or [Formance Ledger](https://formance.com)

**[Architecture Diagrams](ARCHITECTURE.md)** - System architecture, deposit/withdrawal flows, and database schema

**[E2E Testing Guide](E2E_TESTING.md)** - Step-by-step commands to test all functionality

### Asset-Agnostic Balance Model

The subledger mirrors Coinbase Prime's asset-agnostic trading balance model. Balances are tracked per **asset symbol only**, not per network.

**Example:**
- User deposits 1 USDC on Base → Balance: 1 USDC
- User deposits 2 USDC on Ethereum → Balance: 3 USDC (aggregated)
- User withdraws 1 USDC on Base → Balance: 2 USDC

This design reflects how Prime manages trading balances internally, where the same asset on different networks contributes to a unified balance per symbol.

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
CREATE_DUMMY_USERS=false           # Set to true to create 3 dummy test users on first run

# Listener configuration
LISTENER_LOOKBACK_WINDOW=6h        # How far back to check for missed transactions
LISTENER_POLLING_INTERVAL=30s      # How often to poll Prime API
LISTENER_CLEANUP_INTERVAL=15m      # How often to clean up processed transaction cache
ASSETS_FILE=assets.yaml            # Asset configuration file
```

### Storage Backend

The system supports two storage backends, selected via `BACKEND_TYPE`:

**SQLite (default)** -- embedded database, zero dependencies:
```bash
BACKEND_TYPE=sqlite
```

**Formance Ledger** -- remote double-entry ledger with native Numscript, idempotency, and audit trail:
```bash
BACKEND_TYPE=formance
FORMANCE_STACK_URL=https://orgID-stackID.sandbox.formance.cloud
FORMANCE_CLIENT_ID=your-client-id
FORMANCE_CLIENT_SECRET=your-client-secret
FORMANCE_LEDGER_NAME=coinbase-prime-send-receive
```

The Formance backend uses the [formance-sdk-go/v3](https://github.com/formancehq/formance-sdk-go) SDK. The ledger is auto-created on first startup. See [ARCHITECTURE.md](ARCHITECTURE.md) for a detailed comparison of both backends.

**API Usage Notes:**
- The system fetches up to 500 transactions per wallet per polling cycle
- With the default 30-second polling interval, this provides adequate processing time per transaction
- The 6-hour lookback window ensures no transactions are missed between polling cycles
- If you exceed 500 transactions in 30 seconds, consider adjusting the polling interval

### 2. Asset Configuration

The application includes an `assets.yaml` file with default cryptocurrencies to monitor. You can modify this file to add or remove assets as needed.

**Default configuration:**
```yaml
assets:
  - symbol: "USDC"
    network: "ethereum-mainnet"
  - symbol: "USDC"
    network: "base-mainnet"
  - symbol: "SOL"
    network: "solana-mainnet"
```

You must specify the appropriate network for each asset (e.g., `ethereum-mainnet`, `base-mainnet`). A full list of supported networks is returned by the [List Assets](https://docs.cdp.coinbase.com/api-reference/prime-api/rest-api/assets/list-assets) REST API.

**To customize:** Edit `assets.yaml` to add or remove assets based on your needs.

### 3. User Configuration

By default, the system does not create any users. You have several options for adding users:

**Option 1: Use the adduser CLI command (Recommended)**

Add users with automatic address generation:
```bash
go run cmd/adduser/main.go --name "John Doe" --email "john.doe@example.com"
```

This command will:
- Create the user in the database with a generated UUID
- Validate the email format
- Automatically generate deposit addresses for all assets configured in `assets.yaml`
- Display a summary of created addresses

**Option 2: Enable dummy users for testing**

Set `CREATE_DUMMY_USERS=true` in your `.env` file to create three test users on first run: Alice Johnson, Bob Smith, and Carol Williams.

**Option 3: Insert directly into SQLite database**

For advanced use cases, you can insert users directly:
```sql
INSERT INTO users (id, name, email) VALUES
  ('your-uuid-here', 'Your Name', 'your.email@example.com');
```

Then run `go run cmd/setup/main.go` to generate deposit addresses.

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

### Quick Command Reference

```bash
# Setup
go run cmd/adduser/main.go [flags]          # Add new user with deposit addresses
go run cmd/setup/main.go                    # Generate deposit addresses for existing users

# Operations
go run cmd/listener/main.go                 # Start transaction listener
go run cmd/addresses/main.go                # View deposit addresses
go run cmd/balances/main.go                 # View user balances
go run cmd/withdrawal/main.go [flags]       # Create withdrawal
```

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

### CLI Commands

The system provides several CLI commands for managing and querying user balances and addresses.

#### Add New User

Create a new user with automatic deposit address generation:
```bash
# Add a user
go run cmd/adduser/main.go \
  --name "Jane Smith" \
  --email "jane.smith@example.com"
```

The command will:
- Validate the email format and name
- Generate a unique UUID for the user
- Create the user in the database
- Automatically generate deposit addresses for all configured assets
- Display the created user details and address summary

**Required Flags:**
- `--name`: User's full name (minimum 2 characters)
- `--email`: User's email address (must be valid format and unique)

**Example Output:**
```
USER CREATED
ID:    a1b2c3d4-e5f6-7890-g1h2-i3j4k5l6m7n8
Name:  Jane Smith
Email: jane.smith@example.com

🔄 Generating deposit addresses for 4 assets...
  ✓ USDC-ethereum-mainnet: 0x123...
  ✓ BTC-bitcoin-mainnet: bc1q...
  ✓ ETH-ethereum-mainnet: 0x456...
  ✓ SOL-solana-mainnet: 7Np...

✅ User and all deposit addresses created successfully!
```

#### View User Addresses

Display all deposit addresses for users:
```bash
# Show addresses for all users
go run cmd/addresses/main.go

# Show addresses for a specific user
go run cmd/addresses/main.go --email alice.johnson@example.com
```

Output includes:
- User name and email
- Asset-network format (e.g., `ETH-ethereum-mainnet`)
- Deposit address
- Account identifier (if different from address)

#### Check User Balances

Query current balances for all users:
```bash
# Show balances for all users
go run cmd/balances/main.go

# Show balances for a specific user
go run cmd/balances/main.go --email alice.johnson@example.com
```

Output includes:
- Current balance per asset
- Version number (for optimistic locking)
- Last transaction ID
- Last updated timestamp

#### Create Withdrawal

Initiate a withdrawal for a user:
```bash
go run cmd/withdrawal/main.go \
  --email alice.johnson@example.com \
  --asset ETH-ethereum-mainnet \
  --amount 0.1 \
  --destination 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb
```

The withdrawal process:
1. **Validates user** by email
2. **Checks balance** to ensure sufficient funds
3. **Looks up wallet ID** from addresses table
4. **Creates withdrawal** via Prime API with proper idempotency key
5. **Records transaction** (handled automatically by listener)

**Required Flags:**
- `--email`: User's email address
- `--asset`: Asset in format `SYMBOL-network-type` (e.g., `ETH-ethereum-mainnet`)
- `--amount`: Withdrawal amount (as decimal string)
- `--destination`: Blockchain address to send funds to

**Note:** The withdrawal command generates the idempotency key automatically using the format specified below, combining the user's ID prefix with a random UUID suffix.

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

Use the balances CLI command for a formatted view:
```bash
go run cmd/balances/main.go
```

Or query directly via SQL:
```sql
SELECT u.name, ab.asset, ab.balance 
FROM users u 
JOIN account_balances ab ON u.id = ab.user_id
WHERE ab.balance > 0;
```

### View User Addresses

Use the addresses CLI command:
```bash
go run cmd/addresses/main.go --email user@example.com
```

Or query via SQL:
```sql
SELECT u.name, a.asset, a.address, a.wallet_id
FROM users u
JOIN addresses a ON u.id = a.user_id
WHERE u.email = 'user@example.com';
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