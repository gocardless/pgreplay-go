package pgreplay

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	LogLinesParsedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_log_lines_parsed_total",
			Help: "Number of log lines parsed since boot",
		},
	)
	LogLinesErrorTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_log_lines_error_total",
			Help: "Number of log lines that failed to parse",
		},
	)
)

func init() {
	prometheus.MustRegister(LogLinesParsedTotal)
	prometheus.MustRegister(LogLinesErrorTotal)
}

// ItemBufferSize defines the size of the channel buffer when parsing Items.
// Allowing the channel to buffer makes a significant throughput improvement to the
// parsing.
var ItemBufferSize = 100

// PostgresTimestampFormat is the Go template format that we expect to find our errlog
var PostgresTimestampFormat = "2006-01-02 15:04:05.000 MST"

// Parse generates a stream of Items from the given PostgreSQL errlog. Log line
// parsing errors are returned down the errs channel, and we signal having finished our
// parsing by sending a value down the done channel.
func Parse(errlog io.Reader) (chan Item, chan error, chan error) {
	unbounds := map[SessionID]*Execute{}
	loglinebuffer, parsebuffer := make([]byte, MaxLogLineSize), make([]byte, MaxLogLineSize)
	scanner := NewLogScanner(errlog, loglinebuffer)

	items, errs, done := make(chan Item, ItemBufferSize), make(chan error), make(chan error)

	go func() {
		for scanner.Scan() {
			item, err := ParseItem(scanner.Text(), unbounds, parsebuffer)
			if err != nil {
				LogLinesErrorTotal.Inc()
				errs <- err
			}

			if item != nil {
				LogLinesParsedTotal.Inc()
				items <- item
			}
		}

		// Flush the item channel by pushing nil values up-to capacity
		for i := 0; i < ItemBufferSize; i++ {
			items <- nil
		}

		close(items)
		close(errs)

		done <- scanner.Err()
		close(done)
	}()

	return items, errs, done
}

// MaxLogLineSize denotes the maximum size, in bytes, that we can scan in a single log
// line. It is possible to pass really large arrays of parameters to Postgres queries
// which is why this has to be so large.
var MaxLogLineSize = 10 * 1024 * 1024
var InitialScannerBufferSize = 10 * 10

const (
	LogConnectionAuthorized       = "LOG:  connection authorized: "
	LogConnectionReceived         = "LOG:  connection received: "
	LogConnectionDisconnect       = "LOG:  disconnection: "
	LogStatement                  = "LOG:  statement: "
	LogDuration                   = "LOG:  duration: "
	LogExtendedProtocolExecute    = "LOG:  execute <unnamed>: "
	LogExtendedProtocolParameters = "DETAIL:  parameters: "
	LogNamedPrepareExecute        = "LOG:  execute "
	LogDetail                     = "DETAIL:  "
	LogError                      = "ERROR:  "
)

// ParseItem constructs a Item from Postgres errlogs. The format we accept is
// log_line_prefix='%m|%u|%d|%c|', so we can split by | to discover each component.
//
// The unbounds map allows retrieval of an Execute that was previously parsed for a
// session, as we expect following log lines to complete the Execute with the parameters
// it should use.
func ParseItem(logline string, unbounds map[SessionID]*Execute, buffer []byte) (Item, error) {
	tokens := strings.SplitN(logline, "|", 5)
	if len(tokens) != 5 {
		return nil, fmt.Errorf("failed to parse log line: '%s'", logline)
	}

	ts, err := time.Parse(PostgresTimestampFormat, tokens[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse log timestamp: '%s': %v", tokens[0], err)
	}

	// 2018-06-04 13:00:52.366 UTC|postgres|postgres|5b153804.964|<msg>
	user, database, session, msg := tokens[1], tokens[2], tokens[3], tokens[4]

	details := Details{
		Timestamp: ts,
		SessionID: SessionID(session),
		User:      user,
		Database:  database,
	}

	// LOG:  duration: 0.043 ms
	// Duration logs mark completion of replay items, and are not of interest for
	// reproducing traffic. We should only take an action if there exists an unbound item
	// for this session, as this log line will confirm the unbound query has no parameters.
	if strings.HasPrefix(msg, LogDuration) {
		if unbound, ok := unbounds[details.SessionID]; ok {
			return unbound.Bind(nil), nil
		}

		return nil, nil
	}

	// LOG:  statement: select pg_reload_conf();
	if strings.HasPrefix(msg, LogStatement) {
		return &Statement{details, strings.TrimPrefix(msg, LogStatement)}, nil
	}

	// LOG:  execute <unnamed>: select pg_sleep($1)
	// An execute log represents a potential statement. When running the extended protocol,
	// even queries that don't have any arguments will be sent as an unamed prepared
	// statement. We need to wait for a following DETAIL or duration log to confirm the
	// statement has been executed.
	if strings.HasPrefix(msg, LogExtendedProtocolExecute) {
		query := strings.TrimPrefix(msg, LogExtendedProtocolExecute)
		unbounds[details.SessionID] = &Execute{details, query}

		return nil, nil
	}

	// LOG:  execute name: select pg_sleep($1)
	if strings.HasPrefix(msg, LogNamedPrepareExecute) {
		preambleNameColonQuery := msg
		nameColonQuery := strings.TrimPrefix(preambleNameColonQuery, LogNamedPrepareExecute)
		query := strings.SplitN(nameColonQuery, ":", 2)[1]

		// TODO: This doesn't exactly replicate what we'd expect from named prepares. Instead
		// of creating a genuine named prepare, we implement them as unnamed prepared
		// statements instead. If this parse signature allowed us to return arbitrary items
		// then we'd be able to create an initial prepare statement followed by a matching
		// execute, but we can hold off doing this until it becomes a problem.
		unbounds[details.SessionID] = &Execute{details, query}

		return nil, nil
	}

	// DETAIL:  parameters: $1 = '1', $2 = NULL
	if strings.HasPrefix(msg, LogExtendedProtocolParameters) {
		if unbound, ok := unbounds[details.SessionID]; ok {
			parameters, err := ParseBindParameters(strings.TrimPrefix(msg, LogExtendedProtocolParameters), buffer)
			if err != nil {
				return nil, fmt.Errorf("failed to parse bind parameters: %s", err.Error())
			}

			// Remove the unbound from our cache and bind it
			delete(unbounds, details.SessionID)
			return unbound.Bind(parameters), nil
		}

		// It's quite normal for us to get here, as Postgres will log the following when
		// log_min_duration_statement = 0:
		//
		//   1. LOG:  duration: 0.XXX ms  parse name: <statement>
		//   2. LOG:  duration: 0.XXX ms  bind name: <statement>
		//   3. DETAIL:  parameters: $1 = '<param>', $2 = '<param>', ...
		//   4. LOG:  execute name: <statement>
		//   5. DETAIL:  parameters: $1 = '<param>', $2 = '<param>', ...
		//
		// The 3rd and 5th entry are the same, but we expect to be matching our detail against
		// a prior execute log-line. This is just an artifact of Postgres extended query
		// protocol and the activation of two logging systems which duplicate the same entry.
		return nil, fmt.Errorf("cannot process bind parameters without previous execute item: %s", msg)
	}

	// LOG:  connection authorized: user=postgres database=postgres
	if strings.HasPrefix(msg, LogConnectionAuthorized) {
		return &Connect{details}, nil
	}

	// LOG:  disconnection: session time: 0:00:03.861 user=postgres database=postgres host=192.168.99.1 port=51529
	if strings.HasPrefix(msg, LogConnectionDisconnect) {
		return &Disconnect{details}, nil
	}

	// LOG:  connection received: host=192.168.99.1 port=52188
	// We use connection authorized for replay, and can safely ignore connection received
	if strings.HasPrefix(msg, LogConnectionReceived) {
		return nil, nil
	}

	// ERROR:  invalid value for parameter \"log_destination\": \"/var\"
	// We don't replicate errors as this should be the minority of our traffic. Can safely
	// ignore.
	if strings.HasPrefix(msg, LogError) {
		return nil, nil
	}

	// DETAIL:  Unrecognized key word: \"/var/log/postgres/postgres.log\"
	// The previous condition catches the extended query bind detail statements, and any
	// other DETAIL logs we can safely ignore.
	if strings.HasPrefix(msg, LogDetail) {
		return nil, nil
	}

	return nil, fmt.Errorf("no parser matches line: %s", msg)
}

// ParseBindParameters constructs an interface slice from the suffix of a DETAIL parameter
// Postgres errlog. An example input to this function would be:
//
// $1 = '', $2 = '30', $3 = '2018-05-03 10:26:27.905086+00'
//
// ...and this would be parsed into []interface{"", "30", "2018-05-03 10:26:27.905086+00"}
func ParseBindParameters(input string, buffer []byte) ([]interface{}, error) {
	if buffer == nil {
		buffer = make([]byte, InitialScannerBufferSize)
	}

	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Buffer(buffer, MaxLogLineSize)
	scanner.Split(bindParametersSplitFunc)

	parameters := make([]interface{}, 0)

	for scanner.Scan() {
		token := scanner.Text()
		switch token {
		case "NULL":
			parameters = append(parameters, nil)
		default:
			parameters = append(parameters, strings.Replace(
				token[1:len(token)-1], "''", "'", -1,
			))
		}
	}

	return parameters, scanner.Err()
}

var prefixMatcher = regexp.MustCompile(`^(, )?\$\d+ = `)

func bindParametersSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	prefix := prefixMatcher.Find(data)
	if len(prefix) == 0 {
		return 0, nil, fmt.Errorf("could not parse parameter: %s", string(data))
	}

	advance = len(prefix)
	if bytes.HasPrefix(data[advance:], []byte("NULL")) {
		return advance + 4, []byte("NULL"), nil
	}

	closingIdx := findClosingTag(string(data[advance+1:]), "'", "''")
	if closingIdx == -1 {
		return 0, nil, fmt.Errorf("could not find closing ' for parameter: %s", string(data))
	}

	token = data[advance : advance+2+closingIdx]
	advance += 2 + closingIdx

	return
}

// findClosingTag will search the given input for the provided marker, finding the first
// index of the marker that does not also match the escapeSequence.
func findClosingTag(input, marker, escapeSequence string) (idx int) {
	for idx < len(input) {
		if strings.HasPrefix(input[idx:], marker) {
			if strings.HasPrefix(input[idx:], escapeSequence) {
				idx += len(escapeSequence)
				continue
			}

			return idx
		}

		idx++
	}

	return -1
}

// NewLogScanner constructs a scanner that will produce a single token per errlog line
// from Postgres logs. Postgres errlog format looks like this:
//
// 2018-05-03|gc|LOG:  duration: 0.096 ms  parse <unnamed>:
//					DELETE FROM que_jobs
// 					WHERE queue    = $1::text
//
// ...where a log line can spill over multiple lines, with trailing lines marked with a
// preceding \t.
func NewLogScanner(input io.Reader, buffer []byte) *bufio.Scanner {
	if buffer == nil {
		buffer = make([]byte, InitialScannerBufferSize)
	}

	scanner := bufio.NewScanner(input)
	scanner.Buffer(buffer, MaxLogLineSize)
	scanner.Split(logLineSplitFunc)

	return scanner
}

func logLineSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	for {
		// Only seek to the penultimate byte in data, because we want to safely access the
		// byte beyond this if we find a newline.
		offset := bytes.Index(data[advance:len(data)-1], []byte("\n"))

		// If we have no more data to consume but we can't find our delimiter, assume the
		// entire data slice is our token.
		if atEOF && offset == -1 {
			advance = len(data)
			break
		}

		// If we can't peek past our newline, then we'll never know if this is a genuine log
		// line terminator. Signal that we need more content and try again.
		if offset == -1 {
			return 0, nil, nil
		}

		advance += offset + 1

		// If the character immediately proceeding our newline is not a tab, then we know
		// we've come to the end of a valid log, and should break out of our loop. We should
		// only do this if we've genuinely consumed input, i.e., our token is not just
		// whitespace.
		if data[advance] != '\t' && len(bytes.TrimSpace(data[0:advance])) > 0 {
			break
		}
	}

	// We should replace any \n\t with just \n, as that is what a leading \t implies.
	token = bytes.Replace(data[0:advance], []byte("\n\t"), []byte("\n"), -1)
	token = bytes.TrimSpace(token)

	return advance, token, nil
}
