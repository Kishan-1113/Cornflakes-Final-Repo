package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	bakupToMinIO()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	router, err := NewDBRouter()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := router.Close(); err != nil {
			log.Printf("close database pools: %v", err)
		}
	}()

	if err := RunDBHealthChecker(ctx, router); err != nil {
		log.Fatal(err)
	}

}
