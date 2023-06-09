package main

import (
	"bufio"
	"context"
	"fmt"
	stdlog "log"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/alecthomas/kingpin"
	kitlog "github.com/go-kit/kit/log"
	level "github.com/go-kit/kit/log/level"
	"github.com/gocardless/pgreplay-go/pkg/pgreplay"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jackc/pgx/v5"
)

var logger kitlog.Logger

var (
	app = kingpin.New("pgreplay", "Replay Postgres logs against database").Version(versionStanza())

	// Global flags applying to every command
	debug          = app.Flag("debug", "Enable debug logging").Default("false").Bool()
	startFlag      = app.Flag("start", "Play logs from this time onward ("+pgreplay.PostgresTimestampFormat+")").String()
	finishFlag     = app.Flag("finish", "Stop playing logs at this time ("+pgreplay.PostgresTimestampFormat+")").String()
	metricsAddress = app.Flag("metrics-address", "Address to bind HTTP metrics listener").Default("127.0.0.1").String()
	metricsPort    = app.Flag("metrics-port", "Port to bind HTTP metrics listener").Default("9445").Uint16()

	filter            = app.Command("filter", "Process an errlog file into a pgreplay preprocessed JSON log")
	filterJsonInput   = filter.Flag("json-input", "JSON input file").ExistingFile()
	filterErrlogInput = filter.Flag("errlog-input", "Postgres errlog input file").ExistingFile()
	filterCsvLogInput = filter.Flag("csvlog-input", "Postgres CSV log input file").ExistingFile()
	filterOutput      = filter.Flag("output", "JSON output file").String()
	filterNullOutput  = filter.Flag("null-output", "Don't output anything, for testing parsing only").Bool()

	run            = app.Command("run", "Replay from log files against a real database")
	runHost        = run.Flag("host", "PostgreSQL database host").Required().String()
	runPort        = run.Flag("port", "PostgreSQL database port").Default("5432").Uint16()
	runDatname     = run.Flag("database", "PostgreSQL root database").Default("postgres").String()
	runUser        = run.Flag("user", "PostgreSQL root user").Default("postgres").String()
	runPassword    = run.Flag("password", "PostgreSQL root password").String()
	runReplayRate  = run.Flag("replay-rate", "Rate of playback, will execute queries at Nx speed").Default("1").Float()
	runErrlogInput = run.Flag("errlog-input", "Path to PostgreSQL errlog").ExistingFile()
	runCsvLogInput = run.Flag("csvlog-input", "Path to PostgreSQL CSV log").ExistingFile()
	runJsonInput   = run.Flag("json-input", "Path to preprocessed pgreplay JSON log file").ExistingFile()
)

func main() {
	command := kingpin.MustParse(app.Parse(os.Args[1:]))

	logger = kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = kitlog.With(logger, "ts", kitlog.DefaultTimestampUTC, "caller", kitlog.DefaultCaller)
	stdlog.SetOutput(kitlog.NewStdlibAdapter(logger))

	if *debug {
		logger = level.NewFilter(logger, level.AllowDebug())
	} else {
		logger = level.NewFilter(logger, level.AllowInfo())
	}

	go func() {
		logger.Log("event", "metrics.listen", "address", *metricsAddress, "port", *metricsPort)
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(fmt.Sprintf("%s:%v", *metricsAddress, *metricsPort), nil)
	}()

	var err error
	var start, finish *time.Time

	if start, err = parseTimestamp(*startFlag); err != nil {
		kingpin.Fatalf("--start flag %s", err)
	}

	if finish, err = parseTimestamp(*finishFlag); err != nil {
		kingpin.Fatalf("--finish flag %s", err)
	}

	switch command {
	case filter.FullCommand():
		var items chan pgreplay.Item

		switch checkSingleFormat(filterJsonInput, filterErrlogInput, filterCsvLogInput) {
		case filterJsonInput:
			items = parseLog(*filterJsonInput, pgreplay.ParseJSON)
		case filterErrlogInput:
			items = parseLog(*filterErrlogInput, pgreplay.ParseErrlog)
		case filterCsvLogInput:
			items = parseLog(*filterCsvLogInput, pgreplay.ParseCsvLog)
		}

		// Apply the start and end filters
		items = pgreplay.NewStreamer(start, finish).Filter(items)

		if *filterNullOutput {
			logger.Log("event", "filter.null_output", "msg", "Null output enabled, logs won't be serialized")
			for _ = range items {
				// no-op
			}

			return
		}

		if *filterOutput == "" {
			kingpin.Fatalf("must provide output file when no --null-output")
		}

		outputFile, err := os.Create(*filterOutput)
		if err != nil {
			kingpin.Fatalf("failed to create output file: %v", err)
		}

		// Buffer the writes by 32MB to enable much faster filtering
		buffer := bufio.NewWriterSize(outputFile, 32*1000*1000)

		for item := range items {
			bytes, err := pgreplay.ItemMarshalJSON(item)
			if err != nil {
				kingpin.Fatalf("failed to serialize item: %v", err)
			}

			if _, err := buffer.Write(append(bytes, byte('\n'))); err != nil {
				kingpin.Fatalf("failed to write to output file: %v", err)
			}
		}

		buffer.Flush()
		outputFile.Close()

	case run.FullCommand():
		ctx := context.Background()
		config, err := pgx.ParseConfig(fmt.Sprintf(
			"host=%s port=%d dbname=%s user=%s password=%s",
			*runHost,
			*runPort,
			*runDatname,
			*runUser,
			*runPassword,
		))
		if err != nil {
			kingpin.Fatalf("failed to parse postgres connection config: %v", err)
		}

		database, err := pgreplay.NewDatabase(ctx, config)
		if err != nil {
			logger.Log("event", "postgres.error", "error", err)
			os.Exit(255)
		}

		var items chan pgreplay.Item

		switch checkSingleFormat(runJsonInput, runErrlogInput, runCsvLogInput) {
		case runJsonInput:
			items = parseLog(*runJsonInput, pgreplay.ParseJSON)
		case runErrlogInput:
			items = parseLog(*runErrlogInput, pgreplay.ParseErrlog)
		case runCsvLogInput:
			items = parseLog(*runCsvLogInput, pgreplay.ParseCsvLog)
		}

		stream, err := pgreplay.NewStreamer(start, finish).Stream(items, *runReplayRate)
		if err != nil {
			kingpin.Fatalf("failed to start streamer: %s", err)
		}

		errs, consumeDone := database.Consume(ctx, stream)

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
			}
		}
	}
}

// Set by goreleaser
var (
	Version   = "dev"
	Commit    = "none"
	Date      = "unknown"
	GoVersion = runtime.Version()
)

func versionStanza() string {
	return fmt.Sprintf(
		"pgreplay Version: %v\nGit SHA: %v\nGo Version: %v\nGo OS/Arch: %v/%v\nBuilt at: %v",
		Version, Commit, GoVersion, runtime.GOOS, runtime.GOARCH, Date,
	)
}

func checkSingleFormat(formats ...*string) (result *string) {
	var supplied = 0
	for _, format := range formats {
		if *format != "" {
			result = format
			supplied++
		}
	}

	if supplied != 1 {
		kingpin.Fatalf("must provide exactly one input format")
	}

	return result // which becomes the one that isn't empty
}

func parseLog(path string, parser pgreplay.ParserFunc) chan pgreplay.Item {
	file, err := os.Open(path)
	if err != nil {
		kingpin.Fatalf("failed to open logfile: %s", err)
	}

	items, logerrs, done := parser(file)

	go func() {
		logger.Log("event", "parse.finished", "error", <-done)
	}()

	go func() {
		for err := range logerrs {
			level.Debug(logger).Log("event", "parse.error", "error", err)
		}
	}()

	return items
}

// parseTimestamp parsed a Postgres friendly timestamp
func parseTimestamp(in string) (*time.Time, error) {
	if in == "" {
		return nil, nil
	}

	t, err := time.Parse(pgreplay.PostgresTimestampFormat, in)
	return &t, errors.Wrapf(
		err, "must be a valid timestamp (%s)", pgreplay.PostgresTimestampFormat,
	)
}
