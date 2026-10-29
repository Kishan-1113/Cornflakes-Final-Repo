package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestDatabaseGuards(t *testing.T) {
	ctx := context.Background()

	for _, name := range []string{
		"POSTGRES_USER", "DBUSERNAME",
		"POSTGRES_PASSWORD", "DBPASSWORD",
		"POSTGRES_DB", "DBNAME",
	} {
		t.Setenv(name, "")
	}
	if _, err := openDBPool("localhost", "5432"); err == nil {
		t.Fatal("openDBPool without credentials should fail")
	}
	if _, err := OpenDB(ctx); err == nil {
		t.Fatal("OpenDB without credentials should fail")
	}
	if _, err := NewDBRouter(); err == nil {
		t.Fatal("NewDBRouter without credentials should fail")
	}

	if err := (&DBRouter{}).PingContext(ctx); err == nil {
		t.Fatal("PingContext on an unconfigured router should fail")
	}
	if _, err := (&DBRouter{}).ExecQuery(ctx, "SELECT 1"); err == nil {
		t.Fatal("ExecQuery on an unconfigured router should fail")
	}
	if _, err := (&DBRouter{}).QueryRows(ctx, "SELECT 1"); err == nil {
		t.Fatal("QueryRows on an unconfigured router should fail")
	}
	if _, err := ExecQuery(ctx, nil, "SELECT 1"); err == nil {
		t.Fatal("ExecQuery with a nil pool should fail")
	}
	if _, err := QueryRows(ctx, nil, "SELECT 1"); err == nil {
		t.Fatal("QueryRows with a nil pool should fail")
	}
	if err := RunDBHealthChecker(ctx, nil); err == nil {
		t.Fatal("RunDBHealthChecker with a nil router should fail")
	}
	t.Setenv("REPLICA_PGBOUNCER_HOST", "")
	if err := (&DBRouter{}).redirectToReplica(ctx); err == nil {
		t.Fatal("redirectToReplica without a configured host should fail")
	}
	if err := (&DBRouter{}).Close(); err != nil {
		t.Fatalf("Close on an unconfigured router: %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := failover(cancelled, &DBRouter{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("failover with a cancelled context = %v, want context.Canceled", err)
	}
	if err := fenceOldPrimary(ctx); err == nil {
		t.Fatal("fenceOldPrimary should report that fencing is not configured")
	}
	if err := promoteReplica(ctx); err == nil {
		t.Fatal("promoteReplica should report that promotion is not configured")
	}

	t.Setenv("CORNFLAKES_TEST_DEFAULT", "")
	if got := envOrDefault("CORNFLAKES_TEST_DEFAULT", "fallback"); got != "fallback" {
		t.Fatalf("envOrDefault = %q, want fallback", got)
	}
	t.Setenv("CORNFLAKES_TEST_FIRST", "first")
	t.Setenv("CORNFLAKES_TEST_SECOND", "second")
	if got := firstEnv("CORNFLAKES_TEST_FIRST", "CORNFLAKES_TEST_SECOND"); got != "first" {
		t.Fatalf("firstEnv = %q, want first", got)
	}
}

func TestDatabaseFunctionsIntegration(t *testing.T) {
	host := os.Getenv("DB_TEST_HOST")
	if host == "" {
		t.Skip("set DB_TEST_HOST to run read-only PostgreSQL integration checks")
	}
	port := envOrDefault("DB_TEST_PORT", "5432")
	t.Setenv("PGBOUNCER_HOST", host)
	t.Setenv("PGBOUNCER_PORT", port)
	t.Setenv("REPLICA_PGBOUNCER_HOST", host)
	t.Setenv("REPLICA_PGBOUNCER_PORT", port)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := openDBPool(host, port)
	if err != nil {
		t.Fatalf("openDBPool: %v", err)
	}
	defer pool.Close()
	if err := pool.PingContext(ctx); err != nil {
		t.Fatalf("openDBPool PingContext: %v", err)
	}

	db, err := OpenDB(ctx)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	if _, err := ExecQuery(ctx, db, "SELECT 1"); err != nil {
		t.Fatalf("ExecQuery: %v", err)
	}
	rows, err := QueryRows(ctx, db, "SELECT 1")
	if err != nil {
		t.Fatalf("QueryRows: %v", err)
	}
	var result int
	if !rows.Next() {
		rows.Close()
		t.Fatal("QueryRows returned no rows")
	}
	if err := rows.Scan(&result); err != nil {
		rows.Close()
		t.Fatalf("scan QueryRows result: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close QueryRows result: %v", err)
	}
	if result != 1 {
		t.Fatalf("QueryRows result = %d, want 1", result)
	}

	if err := verifyWritablePrimary(ctx, db); err != nil {
		t.Fatalf("verifyWritablePrimary (integration target must be a primary): %v", err)
	}

	router, err := NewDBRouter()
	if err != nil {
		t.Fatalf("NewDBRouter: %v", err)
	}
	if router.get() == nil {
		t.Fatal("NewDBRouter returned a router without a pool")
	}
	if err := router.PingContext(ctx); err != nil {
		t.Fatalf("DBRouter PingContext: %v", err)
	}
	if _, err := router.ExecQuery(ctx, "SELECT 1"); err != nil {
		t.Fatalf("DBRouter ExecQuery: %v", err)
	}
	routedRows, err := router.QueryRows(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("DBRouter QueryRows: %v", err)
	}
	if err := routedRows.Close(); err != nil {
		t.Fatalf("close DBRouter QueryRows result: %v", err)
	}
	if err := router.redirectToReplica(ctx); err != nil {
		t.Fatalf("redirectToReplica (test endpoint is the current primary): %v", err)
	}
	if err := router.Close(); err != nil {
		t.Fatalf("DBRouter Close: %v", err)
	}

	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if err := RunDBHealthChecker(cancelled, &DBRouter{}); err != nil {
		t.Fatalf("RunDBHealthChecker with a cancelled context: %v", err)
	}
	if err := failover(ctx, router); err == nil {
		t.Fatal("failover should stop because fencing is not configured")
	}
}
