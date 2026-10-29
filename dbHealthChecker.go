package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

const (
	dbHealthCheckInterval = 5 * time.Second
	dbHealthCheckTimeout  = 3 * time.Second
)

func RunDBHealthChecker(ctx context.Context, router *DBRouter) error {
	if router == nil {
		return fmt.Errorf("database router is nil")
	}
	ticker := time.NewTicker(dbHealthCheckInterval)
	defer ticker.Stop()

	failureReported := false
	lastFailoverError := ""
	check := func() {
		if ctx.Err() != nil {
			return
		}
		probeCtx, cancel := context.WithTimeout(ctx, dbHealthCheckTimeout)
		err := router.PingContext(probeCtx)
		cancel()

		if err != nil {
			if !failureReported {
				log.Printf("PgBouncer database health check failed: %v", err)
				failureReported = true
			}
			if failoverErr := failover(ctx, router); failoverErr != nil {
				if failoverErr.Error() != lastFailoverError {
					log.Printf("automatic failover halted: %v", failoverErr)
					lastFailoverError = failoverErr.Error()
				}
			} else {
				lastFailoverError = ""
			}
			return
		}

		if failureReported {
			log.Print("database health check recovered")
		}
		failureReported = false
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
	if err := fenceOldPrimary(ctx); err != nil {
		return fmt.Errorf("old primary is not confirmed fenced: %w", err)
	}
	if err := promoteReplica(ctx); err != nil {
		return fmt.Errorf("replica promotion failed: %w", err)
	}
	if err := router.redirectToReplica(ctx); err != nil {
		return fmt.Errorf("redirect to promoted replica failed: %w", err)
	}
	return nil
}

func fenceOldPrimary(context.Context) error {
	// TODO: Fence the old primary through the deployment platform and confirm it cannot accept writes.
	return errors.New("primary fencing is not configured")
}

func promoteReplica(context.Context) error {
	// TODO: Promote the replica after fencing is implemented and confirmed.
	return errors.New("replica promotion is not configured")
}
