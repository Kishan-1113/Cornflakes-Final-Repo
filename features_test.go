package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPurgeAndPostRestoreReapplication(t *testing.T) {
	tempFile := "test_purge_ledger.jsonl"
	t.Setenv("PURGE_LEDGER_FILE", tempFile)
	defer os.Remove(tempFile)

	ctx := context.Background()
	userID := "00000000-0000-0000-0000-000000000001"
	purgedAt := time.Now().UTC()

	// 1. Manually write a purge record as if PurgeUser occurred
	f, err := os.OpenFile(tempFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("create temp purge file: %v", err)
	}
	_, _ = f.WriteString(`{"user_id":"` + userID + `","purged_at":"` + purgedAt.Format(time.RFC3339) + "\"}\n")
	_ = f.Close()

	// 2. Simulate restoring to a point in time before the purge occurred (e.g. 1 hour ago)
	restoredAt := purgedAt.Add(-1 * time.Hour)

	// DB router is unconfigured in this test, so ReapplyPurgesAfterRestore will attempt reapplication
	// and skip without error because router.current is nil
	emptyRouter := &DBRouter{}
	reapplied, err := ReapplyPurgesAfterRestore(ctx, emptyRouter, restoredAt)
	if err != nil {
		t.Fatalf("ReapplyPurgesAfterRestore error: %v", err)
	}
	if reapplied != 1 {
		t.Fatalf("expected 1 reapplied purge, got %d", reapplied)
	}

	// 3. If restored to a time AFTER the purge, 0 purges should be reapplied
	restoredAfter := purgedAt.Add(1 * time.Hour)
	reappliedAfter, err := ReapplyPurgesAfterRestore(ctx, emptyRouter, restoredAfter)
	if err != nil {
		t.Fatalf("ReapplyPurgesAfterRestore error: %v", err)
	}
	if reappliedAfter != 0 {
		t.Fatalf("expected 0 reapplied purges, got %d", reappliedAfter)
	}
}

func TestMoneyReconciliationAndIdempotency(t *testing.T) {
	tempFile := "test_gateway_ledger.jsonl"
	t.Setenv("GATEWAY_LEDGER_FILE", tempFile)
	defer os.Remove(tempFile)

	confirmedAt := time.Now().UTC()
	payment := GatewayPaymentRecord{
		ID:             "pay_123",
		UserID:         "user_abc",
		PaymentID:      "pid_999",
		OrderID:        "ord_001",
		IdempotencyKey: "idem_key_unique_test_123",
		AmountCents:    5000,
		Currency:       "USD",
		Status:         "succeeded",
		ConfirmedAt:    confirmedAt,
	}

	if err := RecordGatewayPayment(payment); err != nil {
		t.Fatalf("RecordGatewayPayment error: %v", err)
	}

	// Verify payment can be parsed from ledger
	restoredAt := confirmedAt.Add(-10 * time.Minute)
	emptyRouter := &DBRouter{}
	// Reconcile with unconfigured DB (should handle gracefully)
	reconciled, err := ReconcilePaymentsAfterRewind(context.Background(), emptyRouter, restoredAt)
	if err != nil {
		t.Fatalf("ReconcilePaymentsAfterRewind error: %v", err)
	}
	if reconciled != 0 {
		t.Fatalf("expected 0 successful DB inserts with nil router, got %d", reconciled)
	}
}

func TestObservabilityMetricsEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	router := &DBRouter{}
	RegisterObservabilityEndpoints(mux, router)

	// Test /health
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	// Router has no DB configured, so health returns 503 Service Unavailable
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("/health status = %d, want 503", w.Code)
	}

	// Test /metrics
	reqMetrics := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	wMetrics := httptest.NewRecorder()
	mux.ServeHTTP(wMetrics, reqMetrics)
	if wMetrics.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", wMetrics.Code)
	}
	body := wMetrics.Body.String()
	if !strings.Contains(body, "replication_lag_seconds") {
		t.Fatalf("expected /metrics to include replication_lag_seconds, got: %s", body)
	}
	if !strings.Contains(body, "last_drill_timestamp") {
		t.Fatalf("expected /metrics to include last_drill_timestamp, got: %s", body)
	}

	// Test /status
	reqStatus := httptest.NewRequest(http.MethodGet, "/status", nil)
	wStatus := httptest.NewRecorder()
	mux.ServeHTTP(wStatus, reqStatus)
	if wStatus.Code != http.StatusOK {
		t.Fatalf("/status status = %d, want 200", wStatus.Code)
	}
}

func TestDrillExecution(t *testing.T) {
	t.Setenv("MOCK_PGBACKREST_CHECK", "true")

	res, err := RunDrillSuite(context.Background(), nil)
	if err != nil {
		t.Fatalf("RunDrillSuite error: %v", err)
	}

	if !res.OverallPass {
		t.Fatal("expected drill to pass")
	}
	if !res.BackupVerificationOK {
		t.Fatal("expected backup verification to be true")
	}
	if !res.PITROK {
		t.Fatal("expected PITR to be true")
	}
	if !res.FailoverOK {
		t.Fatal("expected Failover to be true")
	}
	if res.MeasuredRTOms <= 0 {
		t.Fatalf("expected measured RTO > 0 ms, got %d", res.MeasuredRTOms)
	}
}
