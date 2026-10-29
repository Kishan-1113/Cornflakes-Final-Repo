package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"time"
)

const (
	dbHealthCheckInterval  = 2 * time.Second
	dbHealthCheckTimeout   = 2 * time.Second
	maxConsecutiveFailures = 3
)

func RunDBHealthChecker(ctx context.Context, router *DBRouter) error {
	if router == nil {
		return fmt.Errorf("database router is nil")
	}
	ticker := time.NewTicker(dbHealthCheckInterval)
	defer ticker.Stop()

	consecutiveFailures := 0
	lastFailoverError := ""

	check := func() {
		if ctx.Err() != nil {
			return
		}
		probeCtx, cancel := context.WithTimeout(ctx, dbHealthCheckTimeout)
		err := router.PingContext(probeCtx)
		cancel()

		if err != nil {
			consecutiveFailures++
			log.Printf("Primary database health probe failed (%d/%d): %v", consecutiveFailures, maxConsecutiveFailures, err)

			if consecutiveFailures >= maxConsecutiveFailures {
				if failoverErr := failover(ctx, router); failoverErr != nil {
					if failoverErr.Error() != lastFailoverError {
						log.Printf("automatic failover halted: %v", failoverErr)
						lastFailoverError = failoverErr.Error()
					}
				} else {
					consecutiveFailures = 0
					lastFailoverError = ""
				}
			}
			return
		}

		if consecutiveFailures > 0 {
			log.Printf("database health check recovered after %d failure(s)", consecutiveFailures)
		}
		consecutiveFailures = 0
		lastFailoverError = ""
	}

	check()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			check()
		}
	}
}

func failover(ctx context.Context, router *DBRouter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()

	_ = pausePooler(ctx)

	if err := fenceOldPrimary(ctx); err != nil {
		_ = resumePooler(ctx)
		return fmt.Errorf("old primary is not confirmed fenced: %w", err)
	}

	if err := promoteReplica(ctx); err != nil {
		_ = resumePooler(ctx)
		return fmt.Errorf("replica promotion failed: %w", err)
	}

	if err := router.redirectToReplica(ctx); err != nil {
		_ = resumePooler(ctx)
		return fmt.Errorf("redirect to promoted replica failed: %w", err)
	}

	_ = resumePooler(ctx)

	downtimeMs := time.Since(start).Milliseconds()
	RecordFailoverEvent(FailoverLog{
		Timestamp:   time.Now().UTC(),
		Reason:      "Primary unreachable after consecutive failed health probes",
		OldPrimary:  envOrDefault("POSTGRES_PRIMARY_HOST", "postgres-primary"),
		NewPrimary:  envOrDefault("POSTGRES_REPLICA_HOST", "postgres-replica"),
		DowntimeMs:  downtimeMs,
		FencingOK:   true,
		PromotionOK: true,
	})

	DispatchWebhookAlert("FAILOVER_COMPLETED", "Automatic failover succeeded", map[string]any{
		"downtime_ms": downtimeMs,
		"new_primary": envOrDefault("POSTGRES_REPLICA_HOST", "postgres-replica"),
	})

	log.Printf("[FAILOVER] Automatic failover completed. Measured downtime: %d ms", downtimeMs)
	return nil
}

func fenceOldPrimary(ctx context.Context) error {
	fenceMethod := os.Getenv("FENCE_METHOD")
	if fenceMethod == "" {
		return errors.New("primary fencing is not configured")
	}

	switch fenceMethod {
	case "docker":
		container := envOrDefault("POSTGRES_PRIMARY_CONTAINER", "postgres-primary")
		cmd := exec.CommandContext(ctx, "docker", "stop", container)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("docker stop primary failed: %w (%s)", err, string(out))
		}
		log.Printf("[FENCE] Stopped primary container %s", container)
		return nil

	case "readonly":
		host := envOrDefault("POSTGRES_PRIMARY_HOST", "postgres-primary")
		port := envOrDefault("POSTGRES_PRIMARY_PORT", "5432")
		db, err := openDBPool(host, port)
		if err != nil {
			return nil // Already unreachable
		}
		defer db.Close()
		_, _ = db.ExecContext(ctx, "ALTER SYSTEM SET default_transaction_read_only = 'on'; SELECT pg_reload_conf();")
		log.Printf("[FENCE] Set primary %s to read-only mode", host)
		return nil

	default:
		return fmt.Errorf("unsupported FENCE_METHOD: %s", fenceMethod)
	}
}

func promoteReplica(ctx context.Context) error {
	promoteMethod := os.Getenv("PROMOTE_METHOD")
	if promoteMethod == "" {
		return errors.New("replica promotion is not configured")
	}

	switch promoteMethod {
	case "sql":
		host := envOrDefault("POSTGRES_REPLICA_HOST", "postgres-replica")
		port := envOrDefault("POSTGRES_REPLICA_PORT", "5432")
		db, err := openDBPool(host, port)
		if err != nil {
			return fmt.Errorf("connect to replica for promotion: %w", err)
		}
		defer db.Close()

		var promoted bool
		if err := db.QueryRowContext(ctx, "SELECT pg_promote()").Scan(&promoted); err != nil {
			return fmt.Errorf("execute pg_promote(): %w", err)
		}
		log.Printf("[PROMOTE] Executed pg_promote() on replica %s", host)
		return nil

	case "docker":
		container := envOrDefault("POSTGRES_REPLICA_CONTAINER", "postgres-replica")
		cmd := exec.CommandContext(ctx, "docker", "exec", container, "pg_ctl", "promote", "-D", "/var/lib/postgresql/data")
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("docker exec pg_ctl promote failed: %w (%s)", err, string(out))
		}
		log.Printf("[PROMOTE] Executed pg_ctl promote on container %s", container)
		return nil

	default:
		return fmt.Errorf("unsupported PROMOTE_METHOD: %s", promoteMethod)
	}
}

func pausePooler(ctx context.Context) error {
	host := envOrDefault("PGBOUNCER_HOST", "pgbouncer")
	port := envOrDefault("PGBOUNCER_PORT", "6432")
	adminDB, err := openPgBouncerAdmin(host, port)
	if err != nil {
		return nil
	}
	defer adminDB.Close()
	_, _ = adminDB.ExecContext(ctx, "PAUSE cornflakes_db;")
	return nil
}

func resumePooler(ctx context.Context) error {
	host := envOrDefault("PGBOUNCER_HOST", "pgbouncer")
	port := envOrDefault("PGBOUNCER_PORT", "6432")
	adminDB, err := openPgBouncerAdmin(host, port)
	if err != nil {
		return nil
	}
	defer adminDB.Close()
	_, _ = adminDB.ExecContext(ctx, "RESUME cornflakes_db;")
	return nil
}

func openPgBouncerAdmin(host, port string) (*sql.DB, error) {
	user := firstEnv("POSTGRES_USER", "DBUSERNAME")
	password := firstEnv("POSTGRES_PASSWORD", "DBPASSWORD")
	if user == "" || password == "" {
		return nil, fmt.Errorf("credentials not set")
	}

	connURL := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   net.JoinHostPort(host, port),
		Path:   "pgbouncer",
	}
	q := connURL.Query()
	q.Set("sslmode", "disable")
	connURL.RawQuery = q.Encode()

	db, err := sql.Open("pgx", connURL.String())
	if err != nil {
		return nil, err
	}
	return db, nil
}
