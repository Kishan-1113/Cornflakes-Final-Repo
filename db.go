package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func OpenDB(ctx context.Context) (*sql.DB, error) {
	db, err := openDBPool(
		envOrDefault("PGBOUNCER_HOST", "localhost"),
		envOrDefault("PGBOUNCER_PORT", "6432"),
	)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return nil, fmt.Errorf("connect to PgBouncer: %w (close pool: %v)", err, closeErr)
		}
		return nil, fmt.Errorf("connect to PgBouncer: %w", err)
	}
	return db, nil
}

func openDBPool(host, port string) (*sql.DB, error) {
	user := firstEnv("POSTGRES_USER", "DBUSERNAME")
	password := firstEnv("POSTGRES_PASSWORD", "DBPASSWORD")
	database := firstEnv("POSTGRES_DB", "DBNAME")

	if user == "" || password == "" || database == "" {
		return nil, fmt.Errorf("POSTGRES_USER/DBUSERNAME, POSTGRES_PASSWORD/DBPASSWORD, and POSTGRES_DB/DBNAME must be set")
	}

	connectionURL := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   net.JoinHostPort(host, port),
		Path:   database,
	}
	query := connectionURL.Query()
	query.Set("sslmode", envOrDefault("PGSSLMODE", "disable"))
	connectionURL.RawQuery = query.Encode()

	db, err := sql.Open("pgx", connectionURL.String())
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL pool: %w", err)
	}

	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(30 * time.Minute)
	return db, nil
}

type DBRouter struct {
	mu      sync.RWMutex
	current *sql.DB
	retired []*sql.DB
}

func NewDBRouter() (*DBRouter, error) {
	db, err := openDBPool(
		envOrDefault("PGBOUNCER_HOST", "localhost"),
		envOrDefault("PGBOUNCER_PORT", "6432"),
	)
	if err != nil {
		return nil, err
	}
	return &DBRouter{current: db}, nil
}

func (r *DBRouter) PingContext(ctx context.Context) error {
	db := r.get()
	if db == nil {
		return fmt.Errorf("database pool is not configured")
	}
	return db.PingContext(ctx)
}

func (r *DBRouter) ExecQuery(ctx context.Context, query string, args ...any) (sql.Result, error) {
	db := r.get()
	if db == nil {
		return nil, fmt.Errorf("database pool is not configured")
	}
	return db.ExecContext(ctx, query, args...)
}

func (r *DBRouter) QueryRows(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db := r.get()
	if db == nil {
		return nil, fmt.Errorf("database pool is not configured")
	}
	return db.QueryContext(ctx, query, args...)
}

func (r *DBRouter) redirectToReplica(ctx context.Context) error {
	host := os.Getenv("REPLICA_PGBOUNCER_HOST")
	if host == "" {
		return fmt.Errorf("REPLICA_PGBOUNCER_HOST is not set")
	}
	port := envOrDefault("REPLICA_PGBOUNCER_PORT", "6432")

	replica, err := openDBPool(host, port)
	if err != nil {
		return err
	}
	verifyCtx, cancel := context.WithTimeout(ctx, dbHealthCheckTimeout)
	err = verifyWritablePrimary(verifyCtx, replica)
	cancel()
	if err != nil {
		if closeErr := replica.Close(); closeErr != nil {
			return fmt.Errorf("replica verification failed: %w (close pool: %v)", err, closeErr)
		}
		return err
	}

	r.mu.Lock()
	r.retired = append(r.retired, r.current)
	r.current = replica
	r.mu.Unlock()
	return nil
}

func (r *DBRouter) get() *sql.DB {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.current
}

func (r *DBRouter) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var firstErr error
	if r.current != nil {
		firstErr = r.current.Close()
	}
	for _, db := range r.retired {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func verifyWritablePrimary(ctx context.Context, db *sql.DB) error {
	var inRecovery bool
	if err := db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return fmt.Errorf("verify promoted replica role: %w", err)
	}
	if inRecovery {
		return fmt.Errorf("replica is still in recovery; refusing to route writes")
	}
	return nil
}

func ExecQuery(ctx context.Context, db *sql.DB, query string, args ...any) (sql.Result, error) {
	if db == nil {
		return nil, fmt.Errorf("database pool is nil")
	}
	return db.ExecContext(ctx, query, args...)
}

func QueryRows(ctx context.Context, db *sql.DB, query string, args ...any) (*sql.Rows, error) {
	if db == nil {
		return nil, fmt.Errorf("database pool is nil")
	}
	return db.QueryContext(ctx, query, args...)
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}
