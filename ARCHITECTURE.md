# Architecture

## System Overview

The system is a custodial crypto deposit/withdrawal platform backed by Coinbase Prime. A listener service polls the Prime API for transactions and credits or debits user accounts through a pluggable storage backend.

Two storage backends are supported, selectable via the `BACKEND_TYPE` environment variable:

| | SQLite | Formance Ledger |
|---|---|---|
| **Type** | Embedded relational database | Remote double-entry ledger service |
| **Accounting model** | Single-entry with bolt-on journal | Native double-entry by construction |
| **Balance storage** | Separate `account_balances` table with optimistic locking | Computed from postings (no balance table) |
| **Idempotency** | SQL duplicate check on `external_transaction_id` | Native `reference` field on transactions |
| **Concurrency** | `version` column + `BEGIN`/`COMMIT` | Atomic Numscript execution |
| **Audit trail** | `balance_before`/`balance_after` per row | Immutable transaction log; balances consistent by construction |
| **Reconciliation** | `SUM(transactions)` vs stored balance | Not needed -- single source of truth |
| **Deployment** | Zero dependencies (single file) | Requires a running Formance Stack |

---

## High-Level Component Diagram

```mermaid
graph TB
    subgraph cli [CLI Commands]
        AddUser[adduser]
        Setup[setup]
        Withdrawal[withdrawal]
        Addresses[addresses]
        Balances[balances]
    end

    subgraph core [Core Services]
        Listener[Listener Service]
        API[api.LedgerService]
    end

    subgraph iface [Interface Layer]
        Store["store.LedgerStore"]
    end

    subgraph backends [Storage Backends]
        SQLite["database.Service\n--- SQLite ---"]
        Formance["formance.Service\n--- Formance Ledger ---"]
    end

    subgraph external [External]
        Prime[Coinbase Prime API]
    end

    AddUser --> Store
    AddUser --> Prime
    Setup --> Store
    Setup --> Prime
    Withdrawal --> Store
    Withdrawal --> Prime
    Addresses --> Store
    Balances --> Store

    Listener --> Prime
    Listener --> API
    API --> Store

    Store -.->|BACKEND_TYPE=sqlite| SQLite
    Store -.->|BACKEND_TYPE=formance| Formance
```

The `store.LedgerStore` interface defines 16 methods covering users, addresses, balances, and transactions. Both backends implement the full interface, so every CLI command and the listener work identically regardless of which backend is active.

---

## Deposit Flow

```mermaid
sequenceDiagram
    participant User
    participant Setup as setup CLI
    participant Store as LedgerStore
    participant Prime as Prime API
    participant Listener
    participant Blockchain

    User->>Setup: Create user & generate addresses
    Setup->>Store: CreateUser / StoreAddress
    Setup->>Prime: CreateDepositAddress
    Prime-->>Setup: Return address
    Setup->>Store: StoreAddress (with wallet_id)
    Setup-->>User: Display deposit address

    Note over User,Blockchain: User sends crypto
    User->>Blockchain: Send crypto to address
    Blockchain->>Prime: Transaction detected

    Note over Listener: Polling cycle (every 30s)
    Listener->>Prime: ListWalletTransactions
    Prime-->>Listener: Return transactions
    Listener->>Listener: Filter TRANSACTION_IMPORTED
    Listener->>Store: ProcessDeposit(address, asset, amount, txId)
    Note over Store: SQLite: find user by address, insert tx, update balance<br/>Formance: find user via index account, execute DEPOSIT_RECEIVED Numscript
    Store-->>Listener: Success (duplicate rejected via idempotency)
```

---

## Withdrawal Flow

The withdrawal lifecycle follows a three-phase pattern. With Formance, each phase is a distinct ledger transaction with funds explicitly tracked through a `prime:withdrawals:pending` account. With SQLite, the balance is debited immediately and confirmation is a no-op.

```mermaid
sequenceDiagram
    participant User
    participant CLI as withdrawal CLI
    participant Store as LedgerStore
    participant Prime as Prime API
    participant Listener

    User->>CLI: Request withdrawal
    CLI->>Store: GetUserByEmail / GetUserBalance
    Store-->>CLI: Sufficient balance

    CLI->>CLI: Generate idempotency key

    rect rgb(230, 245, 255)
    Note over CLI,Store: Phase 1 -- WITHDRAWAL_INITIATED (reserve funds)
    CLI->>Store: ProcessWithdrawal(userId, asset, amount, idempotencyKey)
    Note over Store: SQLite: debit balance with version check<br/>Formance: user to pending
    Store-->>CLI: Funds reserved
    end

    CLI->>Prime: CreateWithdrawal
    alt Prime API fails immediately
        Prime-->>CLI: Error
        rect rgb(255, 235, 235)
        Note over CLI,Store: Phase 3 -- WITHDRAWAL_FAILED_REVERSAL
        CLI->>Store: ReverseWithdrawal
        Note over Store: SQLite: credit-back deposit<br/>Formance: RevertTransaction or pending to user
        Store-->>CLI: Balance restored
        end
    else Prime API succeeds
        Prime-->>CLI: Withdrawal created
        CLI-->>User: Success

        Note over Listener: Polling cycle (every 30s)
        Listener->>Prime: ListWalletTransactions

        alt TRANSACTION_DONE
            rect rgb(230, 255, 230)
            Note over Listener,Store: Phase 2 -- WITHDRAWAL_CONFIRMED (settle)
            Listener->>Store: ConfirmWithdrawal(withdrawalRef, externalTxId)
            Note over Store: SQLite: no-op (already debited)<br/>Formance: pending to portfolio:wallet
            Store-->>Listener: Settled
            end
        else Terminal failure (CANCELLED / REJECTED / FAILED / EXPIRED)
            rect rgb(255, 235, 235)
            Note over Listener,Store: Phase 3 -- WITHDRAWAL_FAILED_REVERSAL
            Listener->>Store: ReverseWithdrawal
            Note over Store: SQLite: credit-back deposit<br/>Formance: RevertTransaction or pending to user
            Store-->>Listener: Balance credited back
            end
        end
    end
```

### Withdrawal phases at a glance

| Phase | Event | SQLite | Formance |
|---|---|---|---|
| 1. Reserve | `WITHDRAWAL_INITIATED` | Debit `account_balances` directly | `users:{id}` to `prime:portfolio:{id}:withdrawals:pending` |
| 2. Settle | `WITHDRAWAL_CONFIRMED` | No-op (balance already debited) | `prime:portfolio:{id}:withdrawals:pending` to `prime:portfolio:{id}:wallets:{wid}` |
| 2b. Direct | `WITHDRAWAL_CONFIRMED_DIRECT` | N/A | `users:{id}` (with overdraft) to `prime:portfolio:{id}:wallets:{wid}` |
| 3. Reverse | `WITHDRAWAL_FAILED_REVERSAL` | Credit-back deposit | `prime:portfolio:{id}:withdrawals:pending` to `users:{id}` |

With Formance, the `prime:withdrawals:pending` account balance at any point in time represents the total value of in-flight withdrawals that have been submitted to Coinbase Prime but not yet confirmed or failed on-chain. This gives operations teams immediate visibility into outstanding settlement risk.

---

## Storage Backend: SQLite

SQLite uses a traditional relational model with four tables. Bookkeeping is single-entry at its core: each transaction records an amount, and a separate `account_balances` table caches the current state. An optional `journal_entries` table bolts on double-entry semantics after the fact.

### Schema

```mermaid
erDiagram
    users ||--o{ addresses : has
    users ||--o{ account_balances : has
    users ||--o{ transactions : has

    users {
        string id PK
        string name
        string email UK
        bool active
        timestamp created_at
    }

    addresses {
        string id PK
        string user_id FK
        string asset
        string network
        string address UK
        string wallet_id
        string account_identifier
    }

    account_balances {
        string id PK
        string user_id FK
        string asset
        decimal balance
        string last_transaction_id
        int64 version
        timestamp updated_at
    }

    transactions {
        string id PK
        string user_id FK
        string asset
        string transaction_type
        decimal amount
        decimal balance_before
        decimal balance_after
        string external_transaction_id
        timestamp created_at
    }
```

### How it works

- **User creation**: The `CREATE_DUMMY_USERS=true` flag inserts three test users during schema initialization. This is SQLite-specific -- it has no effect when using the Formance backend (users must be created via `cmd/adduser` instead).
- **Address creation**: The `cmd/setup` CLI reads users from the store, calls the Coinbase Prime API to generate a deposit address per user/asset/network, and stores the mapping via `StoreAddress()`. This works identically for both backends.
- **Balance updates** are explicit: every `ProcessDeposit` / `ProcessWithdrawal` reads the current row from `account_balances`, computes the new value, and writes it back within a SQL transaction using optimistic locking (`WHERE version = ?`).
- **Idempotency** is enforced by checking `external_transaction_id` before inserting.
- **Reconciliation** is a separate function that compares `SUM(transactions.amount)` against the cached `account_balances.balance` -- they can drift if there's a bug.
- **Journal entries** (double-entry) are appended in `addJournalEntries()` as a secondary step; they don't drive any balance logic.

### Benefits

- Zero deployment overhead -- single embedded file, no external services.
- Fast local reads -- all data on disk, no network round-trips.
- Simple to inspect and debug (`sqlite3 addresses.db "SELECT ..."`).
- Works offline -- no connectivity required for balance queries or history lookups.

### Trade-offs

- Balance and transaction history can diverge if the application crashes between writes.
- Concurrency limited to SQLite's single-writer model; optimistic locking is bolted on at the application layer.
- Double-entry bookkeeping is an afterthought (`addJournalEntries`) rather than a structural guarantee.

---

## Storage Backend: Formance Ledger

Formance is a purpose-built double-entry ledger. Every operation is expressed as a Numscript transaction that atomically moves funds between accounts. Balances are never stored separately -- they are computed from the sum of all postings to an account.

### Account Model and Fund Flows

All user accounts are flat (`users:{user_id}`) -- no per-network sub-accounts. Network information is captured in transaction metadata only, matching how Coinbase Prime treats wallets (unified balance across networks).

```mermaid
graph TB
    subgraph platform [Platform Accounts -- Asset side]
        PW["prime:portfolio:{id}:wallets:{wid}"]
        DP["prime:portfolio:{id}:deposits:pending"]
        WP["prime:portfolio:{id}:withdrawals:pending"]
        CV["prime:portfolio:{id}:conversions"]
    end

    subgraph users [User Accounts -- Liability side]
        UA["users:{user_id}"]
        PA["users:prime-platform-{id}"]
    end

    PW -->|"DEPOSIT_PENDING"| DP
    DP -->|"DEPOSIT_CONFIRMED"| UA
    PW -->|"DEPOSIT_RECEIVED"| UA
    UA -->|"WITHDRAWAL_INITIATED"| WP
    PW -->|"WITHDRAWAL_PENDING_FROM_WALLET"| WP
    WP -->|"WITHDRAWAL_CONFIRMED"| PW
    UA -->|"WITHDRAWAL_CONFIRMED_DIRECT"| PW
    WP -->|"WITHDRAWAL_FAILED_REVERSAL"| UA
    CV -->|"CONVERSION leg 1"| PW
    PW -->|"CONVERSION leg 2"| CV
    PW -->|"PLATFORM_TRANSACTION"| PA
```

| Account | Purpose |
|---|---|
| `prime:portfolio:{id}:wallets:{wid}` | Omnibus Prime wallet (one per asset) |
| `prime:portfolio:{id}:deposits:pending` | In-flight deposits not yet confirmed |
| `prime:portfolio:{id}:withdrawals:pending` | In-flight withdrawals not yet settled |
| `prime:portfolio:{id}:conversions` | Conversion clearing account |
| `users:{user_id}` | End-user account (all assets, flat) |
| `users:prime-platform-{id}` | Platform catch-all for unattributed transactions |

### 10 Event Types

| # | Event | Source | Destination | Overdraft |
|---|---|---|---|---|
| 1 | DEPOSIT_PENDING | wallet | deposits:pending | source |
| 2 | DEPOSIT_CONFIRMED | deposits:pending | user | -- |
| 3 | DEPOSIT_RECEIVED | wallet | user | source |
| 4 | WITHDRAWAL_INITIATED | user | withdrawals:pending | -- |
| 5 | WITHDRAWAL_PENDING_FROM_WALLET | wallet | withdrawals:pending | source |
| 6 | WITHDRAWAL_CONFIRMED | withdrawals:pending | wallet | -- |
| 7 | WITHDRAWAL_CONFIRMED_DIRECT | user | wallet | source |
| 8 | WITHDRAWAL_FAILED_REVERSAL | withdrawals:pending | user | source |
| 9 | CONVERSION (2 legs) | conversions / wallet | wallet / conversions | source |
| 10 | PLATFORM_TRANSACTION | wallet | platform user | source |

### How it works

- **Balances** are fetched via `GetAccount` with `expand=volumes` on `users:{userId}`. Single API call per user, no aggregation needed.
- **Idempotency** uses Formance's native `reference` field. Duplicates return `CONFLICT`, mapped to `store.ErrDuplicateTransaction`.
- **Reconciliation** is a no-op -- balances are consistent by construction.
- **Users and addresses** stored as metadata on `users:{userId}`. Deposit addresses: `deposit_addr_{address}={asset}`. Withdrawal addresses: `withdrawal_addr_{address}={asset}`.
- **User lookup** via `FindUserByAddress`: `ListAccounts` with `$or` query across both deposit and withdrawal address keys.
- **Pending check** via `HasPendingWithdrawal`: `ListTransactions` with `metadata[withdrawal_ref]` filter.
- **Immediate rollback** via Formance native `RevertTransaction` API (looks up by `metadata[withdrawal_ref]`).

### Benefits

- True double-entry accounting by construction -- every Numscript `send` is an atomic debit/credit pair. There is no way to credit one account without debiting another.
- Balances are computed from postings, not cached. They cannot drift, go stale, or disagree with the transaction log. Reconciliation is structurally unnecessary.
- Immutable, append-only transaction log provides a complete audit trail out of the box.
- Native idempotency via the `reference` field -- duplicate submissions are rejected at the ledger level without application-side duplicate checks.
- Atomic execution -- a Numscript transaction either fully succeeds or has no effect. No partial writes, no manual rollback code, no version-column locking.
- Rich metadata on accounts and transactions enables flexible querying, reporting, and operational tooling through the Formance Console.
- Three-phase withdrawal pattern (reserve, settle, reverse) prevents double-spending at the ledger level rather than relying on application logic.
- Eliminates ~400 lines of manual bookkeeping code (balance caching, optimistic locking, journal entries, reconciliation).

### Trade-offs

- Requires a running Formance Stack (cloud-hosted sandbox or self-hosted).
- Balance queries require an API call per user (vs. local SQLite query).

---

## Interface Contract

Both backends implement the `store.LedgerStore` interface. The rest of the application is backend-agnostic.

```go
type LedgerStore interface {
    // Users
    GetUsers / GetUserById / GetUserByEmail / CreateUser

    // Addresses
    StoreAddress / GetAddresses / GetAllUserAddresses / FindUserByAddress

    // Balances
    GetUserBalance / GetAllUserBalances

    // Deposits
    ProcessDepositPending                   // DEPOSIT_PENDING
    ConfirmDeposit                          // DEPOSIT_CONFIRMED
    ProcessDeposit                          // DEPOSIT_RECEIVED (direct)

    // Withdrawals
    ProcessWithdrawal                       // WITHDRAWAL_INITIATED
    ProcessWithdrawalFromWallet             // WITHDRAWAL_PENDING_FROM_WALLET
    ConfirmWithdrawal                       // WITHDRAWAL_CONFIRMED
    ConfirmWithdrawalDirect                 // WITHDRAWAL_CONFIRMED_DIRECT
    ReverseWithdrawal                       // WITHDRAWAL_FAILED_REVERSAL
    HasPendingWithdrawal                    // Pre-flight check
    RevertTransaction                       // Native revert (Formance)

    // Platform
    RecordPlatformTransaction               // PLATFORM_TRANSACTION
    RecordConversion                        // CONVERSION

    // Queries
    GetTransactionHistory / GetMostRecentTransactionTime
    ReconcileUserBalance

    // Lifecycle
    Close()
}
```

Backend selection happens once at startup in `common.InitializeServices()` based on the `BACKEND_TYPE` environment variable (`sqlite` or `formance`). All downstream code -- CLI commands, the API service layer, and the listener -- operate exclusively through this interface.
