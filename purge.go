package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type PurgeRecord struct {
	UserID   string    `json:"user_id"`
	PurgedAt time.Time `json:"purged_at"`
}

var purgeMu sync.Mutex

func getPurgeLedgerPath() string {
	return envOrDefault("PURGE_LEDGER_FILE", "purge_ledger.jsonl")
}

// PurgeUser deletes user data from the database and records the event in both the database purge_ledger and an external durable file.
func PurgeUser(ctx context.Context, router *DBRouter, userID string) error {
	purgeMu.Lock()
	defer purgeMu.Unlock()

	now := time.Now().UTC()

	// 1. Delete user transactions and user profile in DB
	if _, err := router.ExecWrite(ctx, "DELETE FROM payments WHERE user_id = $1", userID); err != nil {
		return fmt.Errorf("delete payments for purged user %s: %w", userID, err)
	}
	if _, err := router.ExecWrite(ctx, "DELETE FROM users WHERE id = $1", userID); err != nil {
		return fmt.Errorf("delete user record %s: %w", userID, err)
	}

	// 2. Insert into database purge_ledger table
	if _, err := router.ExecWrite(ctx,
		"INSERT INTO purge_ledger (user_id, purged_at) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET purged_at = EXCLUDED.purged_at",
		userID, now,
	); err != nil {
		return fmt.Errorf("record in DB purge_ledger: %w", err)
	}

	// 3. Append to external durable purge ledger file
	record := PurgeRecord{
		UserID:   userID,
		PurgedAt: now,
	}
	f, err := os.OpenFile(getPurgeLedgerPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open external purge ledger: %w", err)
	}
	defer f.Close()

	bytes, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal purge record: %w", err)
	}
	if _, err := f.Write(append(bytes, '\n')); err != nil {
		return fmt.Errorf("append to external purge ledger: %w", err)
	}

	log.Printf("[PURGE] User %s successfully purged at %s", userID, now.Format(time.RFC3339))
	return nil
}

// ReapplyPurgesAfterRestore reads the external durable purge ledger and reapplies all purges that took place after restoredAt.
func ReapplyPurgesAfterRestore(ctx context.Context, router *DBRouter, restoredAt time.Time) (int, error) {
	purgeMu.Lock()
	defer purgeMu.Unlock()

	path := getPurgeLedgerPath()
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("open external purge ledger: %w", err)
	}
	defer f.Close()

	reappliedCount := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var record PurgeRecord
		if err := json.Unmarshal(line, &record); err != nil {
			continue
		}

		if record.PurgedAt.After(restoredAt) {
			// Re-apply purge
			_, _ = router.ExecWrite(ctx, "DELETE FROM payments WHERE user_id = $1", record.UserID)
			_, _ = router.ExecWrite(ctx, "DELETE FROM users WHERE id = $1", record.UserID)
			_, _ = router.ExecWrite(ctx,
				"INSERT INTO purge_ledger (user_id, purged_at) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING",
				record.UserID, record.PurgedAt,
			)
			log.Printf("[POST-RESTORE PURGE] Re-applied purge for user %s (purged at %s, restored to %s)",
				record.UserID, record.PurgedAt.Format(time.RFC3339), restoredAt.Format(time.RFC3339))
			reappliedCount++
		}
	}

	if err := scanner.Err(); err != nil {
		return reappliedCount, fmt.Errorf("scan purge ledger: %w", err)
	}
	return reappliedCount, nil
}
