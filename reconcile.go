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

type GatewayPaymentRecord struct {
	ID             string    `json:"id"`
	UserID         string    `json:"user_id"`
	PaymentID      string    `json:"payment_id"`
	OrderID        string    `json:"order_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	AmountCents    int64     `json:"amount_cents"`
	Currency       string    `json:"currency"`
	Status         string    `json:"status"`
	ConfirmedAt    time.Time `json:"confirmed_at"`
}

var reconcileMu sync.Mutex

func getGatewayLedgerPath() string {
	return envOrDefault("GATEWAY_LEDGER_FILE", "gateway_ledger.jsonl")
}

func RecordGatewayPayment(p GatewayPaymentRecord) error {
	reconcileMu.Lock()
	defer reconcileMu.Unlock()

	if p.ConfirmedAt.IsZero() {
		p.ConfirmedAt = time.Now().UTC()
	}

	f, err := os.OpenFile(getGatewayLedgerPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open gateway ledger file: %w", err)
	}
	defer f.Close()

	bytes, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal gateway payment: %w", err)
	}
	if _, err := f.Write(append(bytes, '\n')); err != nil {
		return fmt.Errorf("append to gateway ledger file: %w", err)
	}
	return nil
}

func ReconcilePaymentsAfterRewind(ctx context.Context, router *DBRouter, restoredAt time.Time) (int, error) {
	reconcileMu.Lock()
	defer reconcileMu.Unlock()

	path := getGatewayLedgerPath()
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("open gateway ledger: %w", err)
	}
	defer f.Close()

	replayedCount := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var record GatewayPaymentRecord
		if err := json.Unmarshal(line, &record); err != nil {
			continue
		}

		if record.ConfirmedAt.After(restoredAt) {
			payment := Payment{
				UserID:         record.UserID,
				PaymentID:      record.PaymentID,
				OrderID:        record.OrderID,
				IdempotencyKey: record.IdempotencyKey,
				AmountCents:    record.AmountCents,
				Currency:       record.Currency,
				Status:         record.Status,
			}

			_, inserted, err := router.CreatePaymentIdempotent(ctx, payment)
			if err != nil {
				log.Printf("[RECONCILE] Warning: failed to replay payment %s: %v", record.PaymentID, err)
				continue
			}
			if inserted {
				replayedCount++
				log.Printf("[RECONCILE] Replayed missing payment: id=%s amount=%d %s (confirmed at %s)",
					record.PaymentID, record.AmountCents, record.Currency, record.ConfirmedAt.Format(time.RFC3339))
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return replayedCount, fmt.Errorf("scan gateway ledger: %w", err)
	}
	log.Printf("[RECONCILE] Reconciliation complete. Replayed %d missing payments.", replayedCount)
	return replayedCount, nil
}
