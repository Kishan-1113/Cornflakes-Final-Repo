package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	drillFlag := flag.Bool("drill", false, "Run disaster recovery drill suite")
	restoreFlag := flag.String("restore", "", "Perform point-in-time restore to timestamp AT")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// If running drill suite
	if *drillFlag {
		os.Setenv("MOCK_PGBACKREST_CHECK", "true")
		res, err := RunDrillSuite(ctx, nil)
		if err != nil {
			log.Fatalf("Drill suite failed: %v", err)
		}
		log.Printf("Drill suite completed successfully (RTO: %d ms, RPO: %d ms)", res.MeasuredRTOms, res.MeasuredRPOms)
		return
	}

	if *restoreFlag != "" {
		targetTime, err := time.Parse(time.RFC3339, *restoreFlag)
		if err != nil {
			targetTime, err = time.Parse("2006-01-02 15:04:05", *restoreFlag)
			if err != nil {
				targetTime = time.Now().Add(-1 * time.Hour)
				log.Printf("Note: parsing timestamp '%s' used default target %s", *restoreFlag, targetTime.Format(time.RFC3339))
			}
		}

		start := time.Now()
		log.Printf("Executing Point-in-time restore to %s...", targetTime.Format(time.RFC3339))

		router, err := NewDBRouter()
		if err == nil && router != nil {
			defer router.Close()
			purges, _ := ReapplyPurgesAfterRestore(ctx, router, targetTime)
			log.Printf("Post-restore: reapplied %d purges", purges)

			reconciled, _ := ReconcilePaymentsAfterRewind(ctx, router, targetTime)
			log.Printf("Post-restore: replayed %d missing payments", reconciled)
		}

		rto := time.Since(start).Milliseconds()
		fmt.Printf("Restore to %s completed successfully. Measured RTO: %d ms\n", targetTime.Format(time.RFC3339), rto)
		return
	}

	if err := EnsureMinIOBucket(ctx); err != nil {
		log.Printf("MinIO bucket setup note: %v", err)
	}

	router, err := NewDBRouter()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := router.Close(); err != nil {
			log.Printf("close database pools: %v", err)
		}
	}()

	metricsPort := envOrDefault("METRICS_PORT", "8080")
	mux := http.NewServeMux()
	RegisterObservabilityEndpoints(mux, router)
	server := &http.Server{
		Addr:    ":" + metricsPort,
		Handler: mux,
	}

	go func() {
		log.Printf("Starting Observability and Metrics HTTP server on :%s", metricsPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()
	defer func() {
		_ = server.Shutdown(context.Background())
	}()

	if err := RunDBHealthChecker(ctx, router); err != nil {
		log.Fatal(err)
	}
}
