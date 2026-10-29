                      ┌────────────── streaming (walsender → walreceiver) ──────────────┐
client → PgBouncer → PRIMARY (own disk, VM/container A)                      REPLICA (own disk, VM/container B)
                         │                                                       │ (fallback: restore_command)
                         │ archive_command (wal-g wal-push)                      │
                         ▼                                                       ▼
                  ┌──────────────────── MinIO (separate host/disk) ────────────────────┐
                  │  basebackups/  (periodic full backups, encrypted)                  │
                  │  wal_005/      (every WAL segment, encrypted, continuous)          │
                  └─────────────────────────────────────────────────────────────────────┘
                  Encryption keys: Vault / KMS / separate secret store (never in MinIO)