package main

import (
	"fmt"
	stdlog "log"
	"net/http"
	"os"
	"time"

	"github.com/alecthomas/kingpin"
	kitlog "github.com/go-kit/kit/log"
	level "github.com/go-kit/kit/log/level"
	pgreplay "github.com/gocardless/pgreplay-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jackc/pgx"
)

var logger kitlog.Logger

var (
	host           = kingpin.Flag("host", "PostgreSQL database host").Required().String()
	port           = kingpin.Flag("port", "PostgreSQL database port").Default("5432").Uint16()
	datname        = kingpin.Flag("database", "PostgreSQL root database").Default("postgres").String()
	user           = kingpin.Flag("user", "PostgreSQL root user").Default("postgres").String()
	errlogFile     = kingpin.Flag("errlog-file", "Path to PostgreSQL errlog").Required().ExistingFile()
	debug          = kingpin.Flag("debug", "Enable debug logging").Default("false").Bool()
	pollInterval   = kingpin.Flag("poll-interval", "Interval between polling for finish").Default("5s").Duration()
	metricsAddress = kingpin.Flag("metrics-address", "Address to bind HTTP metrics listener").Default("127.0.0.1").String()
	metricsPort    = kingpin.Flag("metrics-port", "Port to bind HTTP metrics listener").Default("9445").Uint16()
)

func main() {
	kingpin.Parse()

	logger = kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, level.AllowInfo())

	if *debug {
		logger = level.NewFilter(logger, level.AllowDebug())
	}

	logger = kitlog.With(logger, "ts", kitlog.DefaultTimestampUTC, "caller", kitlog.DefaultCaller)
	stdlog.SetOutput(kitlog.NewStdlibAdapter(logger))

	go func() {
		logger.Log("event", "metrics.listen", "address", *metricsAddress, "port", *metricsPort)
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(fmt.Sprintf("%s:%v", *metricsAddress, *metricsPort), nil)
	}()

	errlog, err := os.Open(*errlogFile)
	if err != nil {
		logger.Log("event", "logfile.error", "error", err)
		os.Exit(255)
	}

	database, err := pgreplay.NewDatabase(pgx.ConnConfig{
		Host:     *host,
		Port:     *port,
		Database: *datname,
		User:     *user,
	})

	if err != nil {
		logger.Log("event", "postgres.error", "error", err)
		os.Exit(255)
	}

	items, logerrs, done := pgreplay.Parse(errlog)

	go func() {
		logger.Log("event", "parse.finished", "error", <-done)
	}()

	go func() {
		for err := range logerrs {
			level.Debug(logger).Log("event", "parse.error", "error", err)
		}
	}()

	errs, consumeDone := database.Consume(items)
	poller := time.NewTicker(*pollInterval)

	var status int

	for {
		select {
		case err := <-errs:
			if err != nil {
				logger.Log("event", "consume.error", "error", err)
			}
		case err := <-consumeDone:
			if err != nil {
				status = 255
			}

			logger.Log("event", "consume.finished", "error", err, "status", status)
			os.Exit(status)

		// Poll our consumer to determine how much work remains
		case <-poller.C:
			if conns, pending := database.Pending(); pending > 0 {
				logger.Log("event", "consume.pending", "connections", len(conns), "items", pending)
			}
		}
	}
}
