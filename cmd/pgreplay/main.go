package main

import (
	"bufio"
	"fmt"
	stdlog "log"
	"net/http"
	"os"
	"time"

	"github.com/alecthomas/kingpin"
	kitlog "github.com/go-kit/kit/log"
	level "github.com/go-kit/kit/log/level"
	"github.com/gocardless/pgreplay-go/pkg/pgreplay"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jackc/pgx"
)

var logger kitlog.Logger
var Version string // assigned during build

var (
	app = kingpin.New("pgreplay", "Replay Postgres logs against database").Version(Version)

	// Global flags applying to every command
	debug          = app.Flag("debug", "Enable debug logging").Default("false").Bool()
	startFlag      = app.Flag("start", "Play logs from this time onward ("+pgreplay.PostgresTimestampFormat+")").String()
	finishFlag     = app.Flag("finish", "Stop playing logs at this time ("+pgreplay.PostgresTimestampFormat+")").String()
	metricsAddress = app.Flag("metrics-address", "Address to bind HTTP metrics listener").Default("127.0.0.1").String()
	metricsPort    = app.Flag("metrics-port", "Port to bind HTTP metrics listener").Default("9445").Uint16()

	preprocess           = app.Command("preprocess", "Process an errlog file into a pgreplay preprocessed log")
	preprocessJSONOutput = preprocess.Flag("json-output", "JSON output file").Required().String()
	preprocessErrlogFile = preprocess.Flag("errlog-file", "Path to PostgreSQL errlog").Required().ExistingFile()

	run             = app.Command("run", "Replay from log files against a real database")
	runHost         = run.Flag("host", "PostgreSQL database host").Required().String()
	runPort         = run.Flag("port", "PostgreSQL database port").Default("5432").Uint16()
	runDatname      = run.Flag("database", "PostgreSQL root database").Default("postgres").String()
	runUser         = run.Flag("user", "PostgreSQL root user").Default("postgres").String()
	runReplayRate   = run.Flag("replay-rate", "Rate of playback, will execute queries at Nx speed").Default("1").Float()
	runPollInterval = run.Flag("poll-interval", "Interval between polling for finish").Default("5s").Duration()
	runErrlogFile   = run.Flag("errlog-file", "Path to PostgreSQL errlog").ExistingFile()
	runJSONFile     = run.Flag("json-file", "Path to preprocessed pgreplay JSON log file").ExistingFile()
)

func main() {
	command := kingpin.MustParse(app.Parse(os.Args[1:]))

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

	var err error
	var start, finish *time.Time

	if start, err = parseTimestamp(*startFlag); err != nil {
		kingpin.Fatalf("--start flag %s", err)
	}

	if finish, err = parseTimestamp(*finishFlag); err != nil {
		kingpin.Fatalf("--finish flag %s", err)
	}

	switch command {
	case preprocess.FullCommand():
		output, err := os.Create(*preprocessJSONOutput)
		if err != nil {
			kingpin.Fatalf("failed to create output file: %v", err)
		}

		items := parseErrlog(openLogfile(*preprocessErrlogFile))
		for item := range pgreplay.NewStreamer(start, finish).Filter(items) {
			bytes, err := pgreplay.ItemMarshalJSON(item)
			if err != nil {
				kingpin.Fatalf("failed to serialize item: %v", err)
			}

			if _, err := output.Write(append(bytes, byte('\n'))); err != nil {
				kingpin.Fatalf("failed to write to output file: %v", err)
			}
		}

	case run.FullCommand():
		var items chan pgreplay.Item

		if *runJSONFile != "" {
			items = parseJSONlog(openLogfile(*runJSONFile))
		} else if *runErrlogFile != "" {
			items = parseErrlog(openLogfile(*runErrlogFile))
		} else {
			kingpin.Fatalf("must provide either an errlog or jsonlog")
		}

		database, err := pgreplay.NewDatabase(
			pgx.ConnConfig{
				Host:     *runHost,
				Port:     *runPort,
				Database: *runDatname,
				User:     *runUser,
			},
		)

		if err != nil {
			logger.Log("event", "postgres.error", "error", err)
			os.Exit(255)
		}

		stream, err := pgreplay.NewStreamer(start, finish).Stream(items, *runReplayRate)
		if err != nil {
			kingpin.Fatalf("failed to start streamer: %s", err)
		}

		errs, consumeDone := database.Consume(stream)
		poller := time.NewTicker(*runPollInterval)

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
				if connections, items := database.Pending(); connections > 0 {
					logger.Log("event", "consume.pending", "connections", connections, "items", items)
				}
			}
		}
	}
}

func parseJSONlog(file *os.File) chan pgreplay.Item {
	items := make(chan pgreplay.Item)

	go func() {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			item, err := pgreplay.ItemUnmarshalJSON([]byte(line))
			if err == nil {
				items <- item
			}
		}

		close(items)
	}()

	return items
}

func parseErrlog(errlog *os.File) chan pgreplay.Item {
	items, logerrs, done := pgreplay.Parse(errlog)

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

func openLogfile(path string) *os.File {
	file, err := os.Open(path)
	if err != nil {
		logger.Log("event", "logfile.error", "path", path, "error", err)
		os.Exit(255)
	}

	return file
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
