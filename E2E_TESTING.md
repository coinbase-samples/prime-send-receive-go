# End-to-End Testing Guide

This guide provides step-by-step commands to test the core functionality of the Prime Send/Receive system.

## Prerequisites

1. `.env` file configured with valid Prime API credentials (see below)
2. `assets.yaml` configured with desired assets (default includes USDC on Ethereum and Base)
3. `BACKEND_TYPE` set in `.env` (defaults to `sqlite`; set to `formance` for the Formance backend)
4. Clean slate (optional): `rm addresses.db` to start fresh (SQLite) or use a new ledger name (Formance)

### Coinbase Prime API Credentials

The system requires API credentials from a Coinbase Prime account. You need three values:

```bash
PRIME_ACCESS_KEY=your-prime-access-key-here
PRIME_PASSPHRASE=your-prime-passphrase-here
PRIME_SIGNING_KEY=your-prime-signing-key-here
```

These are created in the Coinbase Prime web console under **Settings > API**.

#### Required Permissions

The API key must have the following permissions enabled on the Prime platform:

| Permission | Used by | Prime API operations |
|---|---|---|
| **View** (Portfolios) | All commands (startup) | `ListPortfolios` -- finds the default portfolio on every startup |
| **View** (Wallets) | `setup`, `adduser`, `listener` | `ListWallets` -- lists trading wallets for a given asset |
| **Create** (Wallets) | `setup`, `adduser` | `CreateWallet` -- creates a new trading wallet if one doesn't exist for an asset |
| **Create** (Addresses) | `setup`, `adduser` | `CreateWalletAddress` -- generates a new blockchain deposit address on a wallet |
| **View** (Transactions) | `listener` | `ListWalletTransactions` -- polls for deposits and withdrawal status changes |
| **Create** (Withdrawals) | `withdrawal` | `CreateWalletWithdrawal` -- submits a withdrawal to the blockchain |

#### Which permissions for which test

| Test | Minimum permissions needed |
|---|---|
| Test 1: Create users & assign addresses | View Portfolios, View Wallets, Create Wallets, Create Addresses |
| Test 2: Setup & discover wallets | View Portfolios, View Wallets |
| Test 3: Start listener | View Portfolios, View Wallets, View Transactions |
| Test 4: Deposit (external) | No additional permissions (deposit happens on-chain; listener detects it) |
| Test 5: Withdrawal (insufficient funds) | View Portfolios only (fails before calling Prime) |
| Test 6: Withdrawal (rejection) | View Portfolios, View Wallets, Create Withdrawals, View Transactions |
| Test 7: Withdrawal (success) | View Portfolios, View Wallets, Create Withdrawals, View Transactions |

For full E2E testing, enable **all** the permissions listed above. The withdrawal destination address must also be on the portfolio's **allowlist** in the Prime console, or the withdrawal will be rejected.

---

## Test 1: Create Users and Assign Addresses

Create known users FIRST, with their deposit and withdrawal addresses:

```bash
# Create user with deposit addresses (where they receive funds on Prime)
# and withdrawal addresses (external addresses they send funds TO)
go run cmd/adduser/main.go \
  --name "Alice Johnson" \
  --email alice.johnson@example.com \
  --deposit-addresses 0x3A5027e5DE8d541aaD285d8BcDa1aCD52E2FeFa9 \
  --withdrawal-addresses 0x52f5E49be3cB110Fbc4Be565C69E7ADfb1f7c54d

# Create user with only deposit addresses
go run cmd/adduser/main.go \
  --name "Bob Smith" \
  --email bob.smith@example.com \
  --deposit-addresses 0x44297dd60397ae58cf280ffdb89c4634d5ce7c4f

# Create user without specifying addresses (synced/created from Prime later)
go run cmd/adduser/main.go --name "Carol Williams" --email carol.williams@example.com
```

Each `adduser` call:
- Creates the user in the local store (or finds them if they already exist)
- `--deposit-addresses`: verifies each address exists on a Prime trading wallet, then assigns it to the user
- `--withdrawal-addresses`: registers external addresses so outgoing withdrawals to those addresses can be matched to this user
- Syncs any remaining addresses from Prime for each asset/network in `assets.yaml`

---

## Test 2: Setup and Discover Wallets

After creating known users, run setup to discover everything else from Prime:

```bash
go run cmd/setup/main.go
```

**What this does:**
- Discovers ALL trading wallets on the Prime portfolio (not limited to `assets.yaml`)
- Fetches all existing deposit addresses from each wallet
- Addresses already assigned to users (from Test 1) stay with those users
- Unassigned addresses go to a `Prime Platform (Unattributed)` account
- For each user + each asset/network in `assets.yaml`, ensures addresses exist

**Verify (SQLite):**
```bash
sqlite3 addresses.db "SELECT id, name, email FROM users;"
sqlite3 addresses.db "SELECT user_id, asset, network, address FROM addresses LIMIT 10;"
```

**Verify (Formance):**
```bash
curl -s "$STACK/api/ledger/v2/$LEDGER/accounts?metadata[entity_type]=end_user" \
  -H "Authorization: Bearer $TOKEN" | jq '.cursor.data[] | {address, metadata}'

curl -s "$STACK/api/ledger/v2/$LEDGER/accounts?metadata[purpose]=user_deposit_address" \
  -H "Authorization: Bearer $TOKEN" | jq '.cursor.data[] | {address, metadata}'
```

---

## Test 3: Start Listener

Open a **new terminal window** and run:

```bash
go run cmd/listener/main.go
```

**Expected output:**
- "Starting Prime Send/Receive Listener"
- "Send/Receive listener running - waiting for transactions..."
- Polling messages every 30 seconds

**Keep this terminal open** - it will detect and process all transactions.

---

## Test 4: Make Deposit (External)

Get Alice's Base USDC address:
```bash
go run cmd/addresses/main.go --email alice.johnson@example.com
```

**Expected output:**
- Shows all deposit addresses for Alice
- Copy the Base USDC address

**Send crypto:**
Using a wallet or exchange, send 1-5 USDC on Base mainnet to Alice's Base USDC address.

**Wait ~30-60 seconds** for the listener to poll and process the deposit.

**Verify balance updated:**
```bash
go run cmd/balances/main.go --email alice.johnson@example.com
```

**Expected:** Alice should show the deposited amount

---

## Test 5: Withdrawal - Insufficient Funds

```bash
go run cmd/withdrawal/main.go \
  --email bob.smith@example.com \
  --asset USDC-base-mainnet \
  --amount 10.0 \
  --destination allowlistedaddresshere
```

**Expected output:**
- Fatal error: "Insufficient balance"
- Shows current balance: 0.0
- Shows requested amount: 5.0
- Shows shortfall: 5.0

**Verify balance unchanged:**
```bash
go run cmd/balances/main.go --email bob.smith@example.com
```

---

## Test 6: Withdrawal - Rejection After Submission (Terminal Failure)

Create a withdrawal that will be subsequently manually rejected via Prime UI:

```bash
go run cmd/withdrawal/main.go \
  --email alice.johnson@example.com \
  --asset USDC-base-mainnet \
  --amount 1.0 \
  --destination allowlistedaddresshere
```

**Expected output:**
```
🔄 Reserving funds (debiting local balance)...
Funds reserved - balance debited locally
   New balance: <amount-1.0>

🔄 Creating withdrawal via Prime API...
✅ Withdrawal created successfully!
   Activity ID: <some-activity-id>
   Amount:      1.0 USDC
   Destination: allowlistedaddresshere
```

**Verify balance debited:**
```bash
go run cmd/balances/main.go --email alice.johnson@example.com
```

**Expected:** Balance should be debited (reduced by 1.0 USDC)

**Manually reject in Prime UI:**
- Navigate to the Prime Console
- Find the withdrawal activity
- Reject via consensus rejection

**Watch listener terminal:**
Within ~30-60 seconds, should see:
```
"Processing failed withdrawal - crediting back to user"
"transaction_id": "<activity-id>"
"status": "TRANSACTION_REJECTED"
"Failed withdrawal credited back successfully"
```

**Verify balance refunded:**
```bash
go run cmd/balances/main.go --email alice.johnson@example.com
```

**Expected:** Balance should be refunded (amount credited back automatically)

---

## Test 7: Withdrawal - Success

```bash
go run cmd/withdrawal/main.go \
  --email alice.johnson@example.com \
  --asset USDC-base-mainnet \
  --amount 2.0 \
  --destination 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb
```

**Expected output:**
```
✅ Balance verification PASSED - user has sufficient funds
🔄 Reserving funds (debiting local balance)...
Funds reserved - balance debited locally
   New balance: <amount-2.0>

🔄 Creating withdrawal via Prime API...
✅ Withdrawal created successfully!
   Activity ID: <some-activity-id>
   Amount:      2.0 USDC
   Destination: 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb
```

**Verify balance immediately:**
```bash
go run cmd/balances/main.go --email alice.johnson@example.com
```

**Expected:** Balance debited immediately (shows new balance)

**Watch listener terminal:**
- Within ~30 seconds, should see: "Processing completed withdrawal"
- **No double-debit!**

**Verify balance after listener processes:**
```bash
go run cmd/balances/main.go --email alice.johnson@example.com
```

**Expected:** Same balance as before (no double debit)

---

## Summary Checklist

- [ ] Setup discovers all Prime wallets and creates platform account
- [ ] Users created and addresses synced from Prime
- [ ] Addresses generated for all assets in `assets.yaml`
- [ ] Listener starts and polls successfully
- [ ] Deposit detected and balance credited
- [ ] Insufficient funds rejection works
- [ ] Prime API failure triggers rollback
- [ ] Successful withdrawal debits immediately (funds move to pending in Formance)
- [ ] Listener confirms settlement (pending cleared in Formance)
- [ ] Listener detects and skips duplicate (no double-debit)

---

## Testing with Formance Backend

The system supports two backends, selectable via `BACKEND_TYPE` in `.env`:
- `sqlite` (default) -- local SQLite database
- `formance` -- remote Formance Stack ledger

### Setting up a Formance Sandbox

1. Sign up at [console.formance.cloud](https://console.formance.cloud) and create a sandbox stack.
2. Create an OAuth2 client (Settings > API Clients) with `ledger:read` and `ledger:write` scopes.
3. Note your **Stack URL**, **Client ID**, and **Client Secret**.

### Configuring `.env` for Formance

```bash
BACKEND_TYPE=formance
FORMANCE_STACK_URL=https://orgID-stackID.sandbox.formance.cloud
FORMANCE_CLIENT_ID=your-client-id
FORMANCE_CLIENT_SECRET=your-client-secret
FORMANCE_LEDGER_NAME=coinbase-prime-send-receive
```

The ledger is auto-created on first startup if it doesn't exist.

### Obtaining a Bearer Token with fctl

The `curl` verification commands below require a Bearer token. Use the Formance CLI (`fctl`) to obtain one:

```bash
# Install fctl (if not already installed)
brew install formancehq/tap/fctl

# Login to your Formance organization
fctl login

# Get a Bearer token for API calls
export TOKEN=$(fctl cloud api-clients tokens create CLIENT_ID | jq -r '.data.token')
```

Replace `CLIENT_ID` with the OAuth2 client ID from your `.env` file.

Set the convenience variables used in the commands below:

```bash
export STACK=$FORMANCE_STACK_URL
export LEDGER=$FORMANCE_LEDGER_NAME
```

### Running the Same E2E Tests

All 6 test scenarios above work identically with the Formance backend. The key differences in what you can observe:

- **Verification**: Instead of `sqlite3` queries, inspect data in the Formance Console or API. Every state transition is a separate, queryable ledger transaction.
- **Balances**: Computed from postings (no separate balance table). They cannot drift.
- **Idempotency**: Handled natively by Formance's `reference` field -- no application-side duplicate checks.
- **In-flight visibility**: After a withdrawal is initiated but before on-chain confirmation, you can query `prime:withdrawals:pending` to see the exact amount of funds in-flight. This is not possible with the SQLite backend.

### Three-Phase Withdrawal Verification (Formance)

The Formance backend tracks each withdrawal through three distinct ledger states. You can verify each phase independently:

**After CLI creates a withdrawal (Phase 1: WITHDRAWAL_INITIATED):**

```bash
# User balance is debited, funds are now in pending
# Check user balance (reduced by withdrawal amount)
curl -s "$STACK/api/ledger/v2/$LEDGER/accounts/users:$USER_ID:$NETWORK" \
  -H "Authorization: Bearer $TOKEN" | jq '.data.volumes'

# Check in-flight funds (pending account has a balance)
curl -s "$STACK/api/ledger/v2/$LEDGER/accounts/prime:withdrawals:pending" \
  -H "Authorization: Bearer $TOKEN" | jq '.data.volumes'

# The WITHDRAWAL_INITIATED transaction is visible
curl -s "$STACK/api/ledger/v2/$LEDGER/transactions?metadata[event_type]=withdrawal_initiated&pageSize=5" \
  -H "Authorization: Bearer $TOKEN" | jq '.cursor.data[0]'
```

**After listener detects TRANSACTION_DONE (Phase 2: WITHDRAWAL_CONFIRMED):**

```bash
# Pending account balance is reduced (funds settled to portfolio wallet)
curl -s "$STACK/api/ledger/v2/$LEDGER/accounts/prime:withdrawals:pending" \
  -H "Authorization: Bearer $TOKEN" | jq '.data.volumes'

# Portfolio wallet reflects the settled amount
curl -s "$STACK/api/ledger/v2/$LEDGER/accounts/prime:portfolio:$PORTFOLIO_ID:wallets:$WALLET_ID" \
  -H "Authorization: Bearer $TOKEN" | jq '.data.volumes'

# The WITHDRAWAL_CONFIRMED transaction links back to the original
curl -s "$STACK/api/ledger/v2/$LEDGER/transactions?metadata[event_type]=withdrawal_confirmed&pageSize=5" \
  -H "Authorization: Bearer $TOKEN" | jq '.cursor.data[0]'
```

**After listener detects terminal failure (Phase 3: WITHDRAWAL_FAILED_REVERSAL):**

```bash
# User balance is restored (pending returned to user)
curl -s "$STACK/api/ledger/v2/$LEDGER/accounts/users:$USER_ID:$NETWORK" \
  -H "Authorization: Bearer $TOKEN" | jq '.data.volumes'

# Pending account balance is reduced (funds no longer in-flight)
curl -s "$STACK/api/ledger/v2/$LEDGER/accounts/prime:withdrawals:pending" \
  -H "Authorization: Bearer $TOKEN" | jq '.data.volumes'
```

### Monitoring In-Flight Withdrawals

At any time, query the pending account to see total outstanding settlement risk:

```bash
# Total in-flight withdrawals across all assets
curl -s "$STACK/api/ledger/v2/$LEDGER/accounts/prime:withdrawals:pending?expand=volumes" \
  -H "Authorization: Bearer $TOKEN" | jq '.data.volumes'

# List all pending withdrawal transactions (not yet confirmed or reversed)
curl -s "$STACK/api/ledger/v2/$LEDGER/transactions?metadata[event_type]=withdrawal_initiated&pageSize=50" \
  -H "Authorization: Bearer $TOKEN" | jq '.cursor.data[] | {id, reference, metadata, timestamp}'
```

A non-zero balance on `prime:withdrawals:pending` means there are withdrawals waiting for on-chain confirmation. This is a direct operational signal that is not available with the SQLite backend.

### General Formance Verification

```bash
# List user accounts
curl -s "$STACK/api/ledger/v2/$LEDGER/accounts?metadata[entity_type]=end_user" \
  -H "Authorization: Bearer $TOKEN" | jq '.cursor.data[] | {address, metadata}'

# List recent transactions with event metadata
curl -s "$STACK/api/ledger/v2/$LEDGER/transactions?pageSize=10" \
  -H "Authorization: Bearer $TOKEN" | jq '.cursor.data[] | {id, reference, metadata, postings}'
```

### Running Unit Tests

```bash
go test ./...
```

The Formance helper functions (asset conversion, network normalization, metadata parsing) are tested locally without requiring a live stack.

---

## Troubleshooting

**Listener not processing transactions:**
- Check `.env` has valid Prime API credentials
- Check listener logs for errors
- Verify polling interval (default 30s)

**Balances not updating (SQLite):**
- Check `account_balances` table directly
- Verify transactions table has entries
- Check for errors in logs

**Balances not updating (Formance):**
- Verify the ledger exists in the Formance Console
- Check that OAuth2 credentials have `ledger:read` and `ledger:write` scopes
- Look for `CONFLICT` errors in logs (duplicate transaction references)
- Inspect account balances via the Formance API or Console

**Withdrawals failing:**
- Verify wallet_id exists in addresses table (SQLite) or account metadata (Formance)
- Check Prime API is accessible
- Verify destination address format is correct
