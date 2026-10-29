package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultLagThresholdSeconds = 2.0
)

type DBRouter struct {
	mu           sync.RWMutex
	current      *sql.DB   // Write pool (Primary)
	replicaPool  *sql.DB   // Read pool (Replica)
	retired      []*sql.DB
	lagThreshold float64
}

type Payment struct {
	ID             string    `json:"id"`
	UserID         string    `json:"user_id"`
	PaymentID      string    `json:"payment_id"`
	OrderID        string    `json:"order_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	AmountCents    int64     `json:"amount_cents"`
	Currency       string    `json:"currency"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

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

func NewDBRouter() (*DBRouter, error) {
	primary, err := openDBPool(
		envOrDefault("PGBOUNCER_HOST", "localhost"),
		envOrDefault("PGBOUNCER_PORT", "6432"),
	)
	if err != nil {
		return nil, err
	}

	threshold := defaultLagThresholdSeconds
	if val := os.Getenv("REPLICATION_LAG_THRESHOLD_SEC"); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			threshold = parsed
		}
	}

	var replica *sql.DB
	replicaHost := os.Getenv("REPLICA_PGBOUNCER_HOST")
	if replicaHost != "" {
		replicaPort := envOrDefault("REPLICA_PGBOUNCER_PORT", "6432")
		rPool, err := openDBPool(replicaHost, replicaPort)
		if err == nil {
			replica = rPool
		} else {
			log.Printf("note: replica pool not initialized immediately: %v", err)
		}
	}

	return &DBRouter{
		current:      primary,
		replicaPool:  replica,
		lagThreshold: threshold,
	}, nil
}

func (r *DBRouter) PingContext(ctx context.Context) error {
	db := r.get()
	if db == nil {
		return fmt.Errorf("database pool is not configured")
	}
	return db.PingContext(ctx)
}

// ExecWrite routes exclusively to the primary writable database.
func (r *DBRouter) ExecWrite(ctx context.Context, query string, args ...any) (sql.Result, error) {
	db := r.get()
	if db == nil {
		return nil, fmt.Errorf("primary database pool is not configured")
	}
	return db.ExecContext(ctx, query, args...)
}

// QueryFresh guarantees read-your-own-writes by executing on the primary.
func (r *DBRouter) QueryFresh(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db := r.get()
	if db == nil {
		return nil, fmt.Errorf("primary database pool is not configured")
	}
	return db.QueryContext(ctx, query, args...)
}

// QueryLagAware checks replication lag. If lag <= threshold, queries replica; otherwise queries primary.
func (r *DBRouter) QueryLagAware(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	r.mu.RLock()
	replica := r.replicaPool
	primary := r.current
	threshold := r.lagThreshold
	r.mu.RUnlock()

	if replica != nil {
		lag, err := r.GetReplicationLag(ctx)
		if err == nil && lag <= threshold {
			return replica.QueryContext(ctx, query, args...)
		}
	}

	if primary == nil {
		return nil, fmt.Errorf("primary database pool is not configured")
	}
	return primary.QueryContext(ctx, query, args...)
}

// GetReplicationLag measures current replication lag in seconds.
func (r *DBRouter) GetReplicationLag(ctx context.Context) (float64, error) {
	r.mu.RLock()
	replica := r.replicaPool
	primary := r.current
	r.mu.RUnlock()

	// 1. Try querying lag from primary via pg_stat_replication
	if primary != nil {
		var lagSeconds sql.NullFloat64
		query := `SELECT EXTRACT(EPOCH FROM (now() - reply_time)) FROM pg_stat_replication LIMIT 1;`
		err := primary.QueryRowContext(ctx, query).Scan(&lagSeconds)
		if err == nil {
			if lagSeconds.Valid {
				return lagSeconds.Float64, nil
			}
			return 0.0, nil
		}
	}

	// 2. Try querying lag from replica directly
	if replica != nil {
		var lagSeconds sql.NullFloat64
		query := `SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()));`
		err := replica.QueryRowContext(ctx, query).Scan(&lagSeconds)
		if err == nil {
			if lagSeconds.Valid {
				return lagSeconds.Float64, nil
			}
			return 0.0, nil
		}
	}

	return 0.0, fmt.Errorf("unable to determine replication lag")
}

// ExecQuery executes on current primary pool (backwards-compatible).
func (r *DBRouter) ExecQuery(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return r.ExecWrite(ctx, query, args...)
}

// QueryRows executes query on current primary pool (backwards-compatible).
func (r *DBRouter) QueryRows(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return r.QueryFresh(ctx, query, args...)
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
	r.replicaPool = nil
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
	if r.replicaPool != nil {
		if err := r.replicaPool.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, db := range r.retired {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// CreatePaymentIdempotent inserts a payment with duplicate suppression via idempotency_key.
func (r *DBRouter) CreatePaymentIdempotent(ctx context.Context, p Payment) (string, bool, error) {
	if r == nil {
		return "", false, fmt.Errorf("database router is nil")
	}
	db := r.get()
	if db == nil {
		return "", false, fmt.Errorf("primary database pool is not configured")
	}

	query := `
		INSERT INTO payments (
			user_id, payment_id, order_id, idempotency_key, amount_cents, currency, status
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7
		)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id;
	`
	var id string
	err := db.QueryRowContext(ctx, query,
		p.UserID, p.PaymentID, p.OrderID, p.IdempotencyKey, p.AmountCents, p.Currency, p.Status,
	).Scan(&id)

	if err == sql.ErrNoRows {
		// Already applied earlier with same idempotency key - fetch existing ID
		var existingID string
		errSelect := db.QueryRowContext(ctx,
			"SELECT id FROM payments WHERE idempotency_key = $1", p.IdempotencyKey,
		).Scan(&existingID)
		if errSelect != nil {
			return "", false, fmt.Errorf("fetch existing idempotent payment: %w", errSelect)
		}
		return existingID, false, nil // false = not newly inserted
	}
	if err != nil {
		return "", false, fmt.Errorf("insert payment: %w", err)
	}
	return id, true, nil // true = newly inserted
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
