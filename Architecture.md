# PostgreSQL Clean-Room Data Tier Architecture

```
                       ┌────────────── streaming (walsender → walreceiver) ──────────────┐
Client → PgBouncer ──► PRIMARY (PostgreSQL 17)                                REPLICA (PostgreSQL 17)
  │      (pause/resume)     │                                                        │ (fallback: restore_command)
  │                         │ archive_command (pgbackrest archive-push)              │
  ▼                         ▼                                                        ▼
Router ──────────────► ┌──────────────────── MinIO (S3 Object Store) ─────────────────────┐
 (lag-aware)           │  backup/   (full base backups, client-side AES-256-CBC encrypted)│
                       │  archive/  (continuous WAL archive, AES-256-CBC encrypted)       │
                       └──────────────────────────────────────────────────────────────────┘
                       Encryption key: AES-256 client-side key (single key, never in MinIO)
```

---

## 1. Streaming Replication & Lag-Aware Routing
- **Primary & Replica**: Connected via physical replication slot `standby_slot` with `wal_level = replica`.
- **Lag-Aware Routing (`DBRouter`)**:
  - `ExecWrite`: Always routes to Primary.
  - `QueryFresh`: Always routes to Primary (guarantees read-your-own-writes consistency for fresh balances).
  - `QueryLagAware`: Measures replication lag (`now() - pg_last_xact_replay_timestamp()`). If $\text{lag} \le \text{threshold}$ (default 2s), reads from the replica; otherwise automatically falls back to Primary to prevent stale reads.

---

## 2. Encrypted Backups & Continuous Archiving (pgBackRest)
- **Continuous WAL Archiving**: PostgreSQL invokes `pgbackrest --stanza=cornflakes archive-push %p`.
- **Client-Side Encryption**: Backups and WAL are encrypted before leaving the database container using `repo1-cipher-type=aes-256-cbc`.
- **Retention Policy**: `repo1-retention-full=2` and `repo1-retention-archive-type=full` ensures WAL segments required by the oldest retained base backup are never deleted.
- **Integrity**: pgBackRest creates cryptographic SHA256 checksums in its manifests; `pgbackrest verify` identifies any corrupted or truncated archive segment before it is needed.

---

## 3. Automatic Failover Controller & Pooler Fencing
- **Failure Detection**: Health checker executes consecutive probes (3 failed checks, 6 seconds total) to prevent false-positive promotions during transient network blips.
- **Transaction Buffering**: Controller calls `PAUSE cornflakes_db;` on PgBouncer, buffering in-flight transactions in memory so client requests do not drop.
- **Fencing**: Old primary container is isolated/stopped or forced to read-only mode (`FENCE_METHOD`), guaranteeing that upon returning it cannot accept split-brain writes.
- **Promotion**: Replica is promoted via `pg_promote()` or `pg_ctl promote`.
- **Repointing & Resume**: Router updates write pool to the newly promoted replica, and PgBouncer receives `RESUME cornflakes_db;`.
- **Timeline & Downtime**: Controller records failover timestamp, old/new roles, and exact downtime in milliseconds.

---

## 4. Privacy Purge Surviving Restore
- **External Purge Ledger**: Deletions are committed to an append-only durable log (`purge_ledger.jsonl`) outside the database.
- **Post-Restore Reapplication**: After any point-in-time restore to time $T$, `ReapplyPurgesAfterRestore` parses the ledger for users purged after $T$ and purges them before the database is opened to traffic.
- **Crypto-Shredding Justification**: For archived encrypted backups, sensitive user columns are encrypted with a per-user key managed by an external KMS. When a user is purged, their key is deleted from the KMS. Even if an old backup is restored, the purged user's data remains unreadable.

---

## 5. Money Reconciliation After Rewind
- **External Gateway Ledger**: External payment confirmation events are saved in `gateway_ledger.jsonl`.
- **Post-Restore Replay**: When restoring to time $T$, `ReconcilePaymentsAfterRewind` scans gateway payments confirmed after $T$ and replays them into the database using `ON CONFLICT (idempotency_key) DO NOTHING`.
- **Duplicate Suppression**: Even if delivered multiple times (e.g., 3 webhook deliveries), only one credit is applied.

---

## 6. Disaster Recovery Drills & Observability
- **Restore Command**: `make restore AT="<timestamp>"` or `go run . -restore "<timestamp>"` restores to target moment, compares ledger checksum, and reports measured RTO.
- **Drill Command**: `make drill` runs manifest verification, PITR simulation, and failover validation, printing pass/fail status and measured RTO/RPO.
- **Observability**: Prometheus `/metrics` and `/status` exposing replication lag, last backup, last drill, and failover history, with webhook alerting.

---

## 7. Traps & Failure Scenarios Matrix (T1–T12)

| # | Scenario | Implementation Design & Behavior | Expected Result |
|---|---|---|---|
| **T1** | `kill -9` primary under 200 writes/s | PgBouncer buffers transactions; 3 failed probes trigger failover; old primary fenced; replica promoted; downtime measured. | One promotion; old primary fenced; zero acknowledged writes lost; downtime reported. |
| **T2** | Old primary recovers after promotion | Old primary fenced (stopped / read-only); cannot accept writes; rejoins as replica via `pg_basebackup`. | Cannot accept writes; rebuilt as replica. |
| **T3** | Network partition (both sides alive) | Only the controller-promoted replica receives pooler write traffic; fenced primary rejects writes. | No split-brain; exactly one writer. |
| **T4** | Replica lag 5s right after a payment | `DBRouter.QueryLagAware` detects $\text{lag} > 2.0\text{s}$ threshold and routes query to Primary. | Balance read is correct; no stale balance shown. |
| **T5** | PITR to random timestamp | Database restored to timestamp $T$; `ComputeLedgerChecksum` validates against snapshot at $T$. | Ledger checksum matches recorded snapshot at that time. |
| **T6** | One archived WAL segment is corrupted | `pgbackrest verify` detects SHA256 checksum mismatch against manifest and alerts webhook sink. | Detected by manifest; alert fired; gap explained. |
| **T7** | Restore to yesterday, user purged this morning | `ReapplyPurgesAfterRestore` re-executes purge from durable external ledger before traffic is accepted. | User is purged again before traffic is accepted. |
| **T8** | Restore rewinds past a confirmed payment | `ReconcilePaymentsAfterRewind` identifies missing confirmed payments after $T$ and replays credits. | Reconciliation re-applies the credit exactly once. |
| **T9** | Webhook delivered 3 times after restore | `CreatePaymentIdempotent` uses unique `idempotency_key` with conflict suppression. | One credit recorded; duplicate deliveries ignored. |
| **T10**| MinIO object store down 60s during archive | PostgreSQL spools WAL in `pg_wal` locally; pgBackRest retries until object store recovers; lag alert emitted. | Primary stays healthy; archive catches up; lag alerted. |
| **T11**| Archiver stopped for 1 hour | Alert dispatched on stale archive; pgBackRest retention policy protects WAL required by oldest base backup. | Stale-WAL alert; retention never deletes needed WAL. |
| **T12**| Backup read integrity | Configured with single client-side key (`repo1-cipher-pass`); verify and restore decrypt cleanly. | Backups readable with configured single encryption key. |
