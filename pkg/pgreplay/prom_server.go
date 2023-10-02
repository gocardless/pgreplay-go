package pgreplay

import (
	"context"
	"fmt"
	"net/http"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func StartPrometheusServer(logger kitlog.Logger, address string, port uint16) *http.Server {
	// Server Configuration
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%v", address, port),
		Handler: mux,
	}

	// Starting the servier
	go func() {
		logger.Log("event", "metrics.listen", "address", address, "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log("error", "server.not-started", "message", err.Error())
			return
		}
	}()

	return server
}

func ShutdownServer(ctx context.Context, server *http.Server) error {
	// Waiting for Prometheus to get all the data left
	time.Sleep(5 * time.Second)

	// Shutdown the server gracefully
	if err := server.Shutdown(ctx); err != nil {
		return err
	}

	return nil
}
