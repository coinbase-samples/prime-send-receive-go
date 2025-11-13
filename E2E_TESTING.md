# End-to-End Testing Guide

This guide provides step-by-step commands to test the core functionality of the Prime Send/Receive system.

## Prerequisites

1. `.env` file configured with valid Prime API credentials
2. `assets.yaml` configured with desired assets (default includes USDC on Ethereum and Base)
3. Clean slate (optional): `rm addresses.db` to start fresh

---

## Test 1: Setup

Enable dummy users in `.env`:
```bash
CREATE_DUMMY_USERS=true
```

Run setup to create users and generate addresses:
```bash
go run cmd/setup/main.go
```

**Expected output:**
- Creates 3 users: Alice Johnson, Bob Smith, Carol Williams
- Generates deposit addresses for all configured assets
- Summary showing addresses created per user

**Verify:**
```bash
# Check users were created
sqlite3 addresses.db "SELECT id, name, email FROM users;"

# Check addresses were generated
sqlite3 addresses.db "SELECT user_id, asset, network, address FROM addresses LIMIT 10;"
```

---

## Test 2: Start Listener

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

## Test 3: Make Deposit (External)

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

## Test 4: Withdrawal - Insufficient Funds

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

## Test 5: Withdrawal - Rejection After Submission (Terminal Failure)

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

## Test 6: Withdrawal - Success

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

- [ ] Dummy users created successfully
- [ ] Addresses generated for all assets
- [ ] Listener starts and polls successfully
- [ ] Deposit detected and balance credited
- [ ] Insufficient funds rejection works
- [ ] Prime API failure triggers rollback
- [ ] Successful withdrawal debits immediately
- [ ] Listener detects and skips duplicate (no double-debit)

---

## Troubleshooting

**Listener not processing transactions:**
- Check `.env` has valid Prime API credentials
- Check listener logs for errors
- Verify polling interval (default 30s)

**Balances not updating:**
- Check `account_balances` table directly
- Verify transactions table has entries
- Check for errors in logs

**Withdrawals failing:**
- Verify wallet_id exists in addresses table
- Check Prime API is accessible
- Verify destination address format is correct
