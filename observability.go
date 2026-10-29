package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type ObservabilityState struct {
	mu                   sync.RWMutex
	LastSuccessfulBackup time.Time      `json:"last_successful_backup"`
	LastDrillTime        time.Time      `json:"last_drill_time"`
	LastDrillStatus      string         `json:"last_drill_status"`
	FailoverHistory      []FailoverLog  `json:"failover_history"`
	AlertsHistory        []WebhookAlert `json:"alerts_history"`
}

type FailoverLog struct {
	Timestamp   time.Time `json:"timestamp"`
	Reason      string    `json:"reason"`
	OldPrimary  string    `json:"old_primary"`
	NewPrimary  string    `json:"new_primary"`
	DowntimeMs  int64     `json:"downtime_ms"`
	FencingOK   bool      `json:"fencing_ok"`
	PromotionOK bool      `json:"promotion_ok"`
}

type WebhookAlert struct {
	Timestamp time.Time `json:"timestamp"`
	AlertType string    `json:"alert_type"`
	Message   string    `json:"message"`
	Details   any       `json:"details,omitempty"`
}

var globalObs = &ObservabilityState{
	FailoverHistory: make([]FailoverLog, 0),
	AlertsHistory:   make([]WebhookAlert, 0),
}

func RecordFailoverEvent(entry FailoverLog) {
	globalObs.mu.Lock()
	defer globalObs.mu.Unlock()
	globalObs.FailoverHistory = append(globalObs.FailoverHistory, entry)
	log.Printf("[CONTROLLER] Recorded failover event: %+v", entry)
}

func RecordBackupSuccess(t time.Time) {
	globalObs.mu.Lock()
	defer globalObs.mu.Unlock()
	globalObs.LastSuccessfulBackup = t
}

func RecordDrillExecution(t time.Time, status string) {
	globalObs.mu.Lock()
	defer globalObs.mu.Unlock()
	globalObs.LastDrillTime = t
	globalObs.LastDrillStatus = status
}

func DispatchWebhookAlert(alertType, message string, details any) {
	alert := WebhookAlert{
		Timestamp: time.Now().UTC(),
		AlertType: alertType,
		Message:   message,
		Details:   details,
	}

	globalObs.mu.Lock()
	globalObs.AlertsHistory = append(globalObs.AlertsHistory, alert)
	globalObs.mu.Unlock()

	log.Printf("[ALERT] Type: %s | Message: %s | Details: %v", alertType, message, details)

	webhookURL := os.Getenv("WEBHOOK_SINK_URL")
	if webhookURL == "" {
		return
	}

	payload, err := json.Marshal(alert)
	if err != nil {
		return
	}

	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Post(webhookURL, "application/json", bytes.NewReader(payload))
		if err != nil {
			log.Printf("failed to send webhook alert to %s: %v", webhookURL, err)
			return
		}
		_ = resp.Body.Close()
	}()
}

func RegisterObservabilityEndpoints(mux *http.ServeMux, router *DBRouter) {
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := router.PingContext(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "unhealthy", "error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "healthy"})
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		lagSeconds, lagErr := router.GetReplicationLag(ctx)
		if lagErr != nil {
			lagSeconds = -1.0
		}

		globalObs.mu.RLock()
		defer globalObs.mu.RUnlock()

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP replication_lag_seconds Current replication lag in seconds\n")
		fmt.Fprintf(w, "# TYPE replication_lag_seconds gauge\n")
		fmt.Fprintf(w, "replication_lag_seconds %.4f\n\n", lagSeconds)

		backupTimestamp := float64(globalObs.LastSuccessfulBackup.Unix())
		if globalObs.LastSuccessfulBackup.IsZero() {
			backupTimestamp = 0
		}
		fmt.Fprintf(w, "# HELP last_successful_backup_timestamp Epoch timestamp of last successful backup\n")
		fmt.Fprintf(w, "# TYPE last_successful_backup_timestamp gauge\n")
		fmt.Fprintf(w, "last_successful_backup_timestamp %.0f\n\n", backupTimestamp)

		drillTimestamp := float64(globalObs.LastDrillTime.Unix())
		if globalObs.LastDrillTime.IsZero() {
			drillTimestamp = 0
		}
		fmt.Fprintf(w, "# HELP last_drill_timestamp Epoch timestamp of last drill\n")
		fmt.Fprintf(w, "# TYPE last_drill_timestamp gauge\n")
		fmt.Fprintf(w, "last_drill_timestamp %.0f\n\n", drillTimestamp)

		fmt.Fprintf(w, "# HELP failover_total Total number of automatic failovers performed\n")
		fmt.Fprintf(w, "# TYPE failover_total counter\n")
		fmt.Fprintf(w, "failover_total %d\n", len(globalObs.FailoverHistory))
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		lag, lagErr := router.GetReplicationLag(ctx)
		var lagVal any = lag
		if lagErr != nil {
			lagVal = lagErr.Error()
		}

		globalObs.mu.RLock()
		data := map[string]any{
			"replication_lag":        lagVal,
			"last_successful_backup": globalObs.LastSuccessfulBackup,
			"last_drill_time":        globalObs.LastDrillTime,
			"last_drill_status":      globalObs.LastDrillStatus,
			"failover_history":       globalObs.FailoverHistory,
			"alerts_history":         globalObs.AlertsHistory,
		}
		globalObs.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(data)
	})
}
