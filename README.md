# Cornflakes: High-Availability PostgreSQL Data Tier

A resilient, clean-room PostgreSQL 17 cluster running with **Docker Compose**, featuring **streaming replication**, **pgBackRest client-side AES-256-CBC encrypted backups & continuous WAL archiving to MinIO**, **replication-lag-aware routing**, **fenced automatic failover**, **point-in-time restore (PITR)**, **post-restore privacy purges**, **gateway money reconciliation**, and **automated disaster recovery drills**.

---

## Architecture Diagram

```
                               ┌───────── streaming replication (slot: standby_slot) ─────────┐
                               │                                                               │
Client ──► PgBouncer Pooler ──► PRIMARY (Postgres 17)                               REPLICA (Postgres 17)
  │        (pause / resume)          │                                                         │ (standby sync)
  │                                  │ archive_command (pgbackrest archive-push)               │
  ▼                                  ▼                                                         ▼
Router ──────────────────────► ┌───────────────────── MinIO (S3 Object Store) ─────────────────┐
(lag-aware)                    │  backup/   (AES-256-CBC encrypted base backups)               │
                               │  archive/  (continuous encrypted WAL segments)                │
                               └───────────────────────────────────────────────────────────────┘
                               Encryption Key: AES-256 client-side key (single key, never in S3)
```

---

## 1. How to Run Everything

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/)
- [Go 1.22+](https://go.dev/dl/) (for local commands and drills)

### Quick Start

1. **Clone and Configure Environment**:
   The default configuration is ready in `.env`:
   ```bash
   POSTGRES_USER=payments_app
   POSTGRES_PASSWORD=password_is_my_name
   POSTGRES_DB=cornflakes_db

   PGBOUNCER_PORT=6432
   REPLICA_PGBOUNCER_PORT=6433

   MINIO_ENDPOINT=minio:9000
   MINIO_ROOT_USER=minioadmin
   MINIO_ROOT_PASSWORD=minioadmin123
   MINIO_BACKUP_BUCKET=cornflakes-backups

   METRICS_PORT=8080
   ```

2. **Start the Entire Cluster**:
   ```bash
   docker-compose up -d --build
   ```
   This spins up:
   - `minio`: S3-compatible object store on ports `9000` (API) and `9001` (Web Console).
   - `minio-init`: Automatically provisions the `cornflakes-backups` bucket.
   - `postgres-primary`: PostgreSQL 17 primary with pgBackRest and replication slot `standby_slot`.
   - `postgres-replica`: Standby replica bootstrapped via `pg_basebackup`.
   - `pgbouncer`: Primary transaction pooler on port `6432`.
   - `pgbouncer-replica`: Replica transaction pooler on port `6433`.
   - `app`: Go application running router, health checker/failover controller, and metrics on port `8080`.

3. **Verify Cluster Health**:
   ```bash
   # Check running containers
   docker-compose ps

   # Check application health endpoint
   curl http://localhost:8080/health

   # MinIO Console access (Web UI):
   # URL: http://localhost:9001
   # User: minioadmin / Password: minioadmin123
   ```

4. **Run Unit and Integration Tests**:
   ```bash
   go test -v ./...
   ```

---

## 2. Disaster Recovery Drills & PITR

### Run Disaster Recovery Drill
Executes manifest verification, point-in-time restore simulation, and failover validation, reporting RPO and RTO:
```bash
make drill
# or:
go run . -drill
```
Sample Output:
```
==================================================
       RUNNING DISASTER RECOVERY DRILL SUITE      
==================================================
[STEP 1] Verifying Backups & Manifest Integrity...
[DRILL] pgBackRest verification passed
[STEP 2] Executing PITR & Checksum Verification...
[STEP 3] Executing Failover Drill...
==================================================
 DRILL RESULTS: PASS
 • Backup Integrity: true
 • PITR Restore:     true (RTO: 50 ms, RPO: 1000 ms)
 • Failover Drill:   true
==================================================
```

### Point-in-Time Restore (PITR)
To restore the database to an exact historical timestamp $T$, reapply purges, and reconcile money:
```bash
make restore AT="2026-10-29 12:00:00"
# or:
go run . -restore "2026-10-29T12:00:00Z"
```
Sample Output:
```
Executing Point-in-time restore to 2026-10-29T12:00:00Z...
Post-restore: reapplied 1 purges
Post-restore: replayed 2 missing payments
Restore to 2026-10-29T12:00:00Z completed successfully. Measured RTO: 42 ms
```

---

## 3. How to Trigger Every Failure Switch (Scenarios T1–T12)

| Scenario | What It Tests | Command to Trigger Failure | Expected Behavior |
|---|---|---|---|
| **T1** | Primary `kill -9` under write load | `docker kill -s 9 $(docker ps -qf name=postgres-primary)` | PgBouncer pauses in-flight writes; 3 failed probes trigger failover; old primary is fenced; replica promoted; zero acknowledged writes lost; downtime reported. |
| **T2** | Old primary recovers after promotion | `docker start $(docker ps -aqf name=postgres-primary)` | Fencing prevents it from accepting writes; rejoins as replica via `pg_basebackup`. |
| **T3** | Network partition (both sides alive) | `docker network disconnect cornflakes-task_default $(docker ps -qf name=postgres-primary)` | Controller promotes replica; pooler directs all traffic to promoted replica; no split-brain (exactly one writer). |
| **T4** | Replica lag $\ge 5\text{s}$ right after payment | `docker exec -it $(docker ps -qf name=postgres-replica) psql -U payments_app -d cornflakes_db -c "SELECT pg_wal_replay_pause();"` | Balance query detects $\text{lag} > 2.0\text{s}$ threshold and routes directly to primary. No stale balance served. (Resume with `SELECT pg_wal_replay_resume();`). |
| **T5** | PITR to random timestamp | `go run . -restore "2026-10-29T14:30:00Z"` | Database restored to timestamp; ledger checksum validated against cryptographic SHA-256 snapshot. |
| **T6** | One archived WAL segment corrupted | Corrupt or truncate a WAL file in MinIO bucket `cornflakes-backups/archive/...` | Detected immediately by `pgbackrest verify` manifest check; alert dispatched to webhook sink. |
| **T7** | Restore to yesterday, user purged this morning | Run user purge: user recorded in `purge_ledger.jsonl`. Then run: `go run . -restore "<yesterday_timestamp>"` | `ReapplyPurgesAfterRestore` re-executes deletion of purged users before the database accepts public traffic. |
| **T8** | Restore rewinds past confirmed payment | Confirm payment in `gateway_ledger.jsonl`. Rewind database to earlier timestamp. | `ReconcilePaymentsAfterRewind` identifies missing confirmed transaction and replays credit. |
| **T9** | Duplicate webhook delivered 3 times | Deliver 3 webhook payloads with the identical `idempotency_key`. | Database constraint `ON CONFLICT (idempotency_key) DO NOTHING` applies credit exactly once; 2 duplicates suppressed. |
| **T10**| MinIO object store down for 60s during archive | `docker pause $(docker ps -qf name=minio)` (wait 60s, then `docker unpause ...`) | PostgreSQL primary stays healthy; WAL spools in local `pg_wal`; pgBackRest automatically retries and catches up upon resume. |
| **T11**| Archiver stopped for 1 hour | Stop pgBackRest archive process: `docker exec -it $(docker ps -qf name=postgres-primary) psql -U payments_app -d cornflakes_db -c "ALTER SYSTEM SET archive_command = '/bin/false'; SELECT pg_reload_conf();"` | Stale-WAL alert dispatched; retention policy (`repo1-retention-archive-type=full`) guarantees WAL needed by oldest base backup is never deleted. |
| **T12**| Backup read integrity with single key | Run `pgbackrest --stanza=cornflakes verify` | All archives and base backups decrypt cleanly with client-side key `repo1-cipher-pass`. |

---

## 4. How to Read the Metrics

The application exposes a built-in observability server on port `8080`.

### 1. Prometheus Metrics (`GET /metrics`)
Endpoint: `http://localhost:8080/metrics`
```bash
curl http://localhost:8080/metrics
```
Sample Output:
```prometheus
# HELP replication_lag_seconds Current replication lag in seconds
# TYPE replication_lag_seconds gauge
replication_lag_seconds 0.0012

# HELP last_successful_backup_timestamp Epoch timestamp of last successful backup
# TYPE last_successful_backup_timestamp gauge
last_successful_backup_timestamp 1793284000

# HELP last_drill_timestamp Epoch timestamp of last drill
# TYPE last_drill_timestamp gauge
last_drill_timestamp 1793284000

# HELP failover_total Total number of automatic failovers performed
# TYPE failover_total counter
failover_total 1
```

**Key Metric Descriptions**:
- `replication_lag_seconds`: Real-time lag between primary and replica in seconds. If this exceeds threshold (2.0s), read traffic falls back to the primary.
- `last_successful_backup_timestamp`: Unix timestamp of the most recent verified backup. Alerts fire if $> 24$ hours old.
- `last_drill_timestamp`: Unix timestamp of the last disaster recovery drill. Alerts fire if $> 7$ days old.
- `failover_total`: Total count of failovers executed by the controller.

---

### 2. JSON Status Dashboard (`GET /status`)
Endpoint: `http://localhost:8080/status`
```bash
curl http://localhost:8080/status
```
Sample Output:
```json
{
  "replication_lag": 0.0012,
  "last_successful_backup": "2026-10-29T14:40:00Z",
  "last_drill_time": "2026-10-29T14:40:33Z",
  "last_drill_status": "PASS",
  "failover_history": [
    {
      "timestamp": "2026-10-29T14:35:10Z",
      "reason": "Primary unreachable after consecutive failed health probes",
      "old_primary": "postgres-primary",
      "new_primary": "postgres-replica",
      "downtime_ms": 142,
      "fencing_ok": true,
      "promotion_ok": true
    }
  ],
  "alerts_history": []
}
```

---

### 3. Health Probe (`GET /health`)
Endpoint: `http://localhost:8080/health`
- **HTTP 200 OK**:
  ```json
  {"status":"healthy"}
  ```
- **HTTP 503 Service Unavailable** (if primary database is unreachable and failover is in progress):
  ```json
  {"status":"unhealthy","error":"connect to PgBouncer: dial tcp: connection refused"}
  ```

---

### 4. Webhook Alerts Dispatcher
When critical conditions occur (such as failover completion, drill failures, or stale WAL), alerts are dispatched to the URL configured in `WEBHOOK_SINK_URL`:
```json
{
  "timestamp": "2026-10-29T14:35:10Z",
  "alert_type": "FAILOVER_COMPLETED",
  "message": "Automatic failover succeeded",
  "details": {
    "downtime_ms": 142,
    "new_primary": "postgres-replica"
  }
}
```
