package pgreplay

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	logLinesParsedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_log_lines_parsed_total",
			Help: "Number of log lines parsed since boot",
		},
	)
	logLinesErrorTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_log_lines_error_total",
			Help: "Number of log lines that failed to parse",
		},
	)
)

const (
	// ItemBufferSize defines the size of the channel buffer when parsing Items.
	// Allowing the channel to buffer makes a significant throughput improvement to the
	// parsing.
	ItemBufferSize = 100

	// MaxLogLineSize denotes the maximum size, in bytes, that we can scan in a single log
	// line. It is possible to pass really large arrays of parameters to Postgres queries
	// which is why this has to be so large.
	MaxLogLineSize           = 10 * 1024 * 1024
	InitialScannerBufferSize = 10 * 10

	// PostgresTimestampFormat is the Go template format that we expect to find our errlog
	PostgresTimestampFormat = "2006-01-02 15:04:05.000 MST"
)

// ParserFunc is the standard interface to provide items from a parsing source
type ParserFunc func(io.Reader) (items chan Item, errs chan error, done chan error)

// ParseJSON operates on a file of JSON serialized Item elements, and pushes the parsed
// items down the returned channel.
func ParseJSON(jsonlog io.Reader) (items chan Item, errs chan error, done chan error) {
	items, errs, done = make(chan Item, ItemBufferSize), make(chan error), make(chan error)

	go func() {
		scanner := bufio.NewScanner(jsonlog)
		scanner.Buffer(make([]byte, InitialScannerBufferSize), MaxLogLineSize)

		for scanner.Scan() {
			line := scanner.Text()
			item, err := ItemUnmarshalJSON([]byte(line))
			if err != nil {
				errs <- err
			} else {
				items <- item
			}
		}

		close(items)
		close(errs)

		done <- scanner.Err()
		close(done)
	}()

	return
}

func ParseCsvLog(csvlog io.Reader) (items chan Item, errs chan error, done chan error) {
	reader := csv.NewReader(csvlog)
	unbounds := map[SessionID]*Execute{}
	parsebuffer := make([]byte, MaxLogLineSize)
	items, errs, done = make(chan Item, ItemBufferSize), make(chan error), make(chan error)

	go func() {
		for {
			logline, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				logLinesErrorTotal.Inc()
				errs <- err
			}
			item, err := ParseCsvItem(logline, unbounds, parsebuffer)
			if err != nil {
				logLinesErrorTotal.Inc()
				errs <- err
			}

			if item != nil {
				logLinesParsedTotal.Inc()
				items <- item
			}
		}

		// Flush the item channel by pushing nil values up-to capacity
		for i := 0; i < ItemBufferSize; i++ {
			items <- nil
		}

		close(items)
		close(errs)
		close(done)
	}()

	return
}

// ParseErrlog generates a stream of Items from the given PostgreSQL errlog. Log line
// parsing errors are returned down the errs channel, and we signal having finished our
// parsing by sending a value down the done channel.
func ParseErrlog(errlog io.Reader) (items chan Item, errs chan error, done chan error) {
	unbounds := map[SessionID]*Execute{}
	loglinebuffer, parsebuffer := make([]byte, MaxLogLineSize), make([]byte, MaxLogLineSize)
	scanner := NewLogScanner(errlog, loglinebuffer)

	items, errs, done = make(chan Item, ItemBufferSize), make(chan error), make(chan error)

	go func() {
		for scanner.Scan() {
			item, err := ParseItem(scanner.Text(), unbounds, parsebuffer)
			if err != nil {
				logLinesErrorTotal.Inc()
				errs <- err
			}

			if item != nil {
				logLinesParsedTotal.Inc()
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

	return
}

const (
	// File Type Conversion
	ParsedFromCsv    = "csv"
	ParsedFromErrLog = "errlog"
	// Log Detail Message
	ActionLog    = "LOG:  "
	ActionDetail = "DETAIL:  "
	ActionError  = "ERROR:  "
)

var (
	LogConnectionAuthorized = LogMessage{
		ActionLog, "connection authorized: ",
		regexp.MustCompile(`^connection authorized\: `),
	}
	LogConnectionReceived = LogMessage{
		ActionLog, "connection received: ",
		regexp.MustCompile(`^connection received\: `),
	}
	LogConnectionDisconnect = LogMessage{
		ActionLog, "disconnection: ",
		regexp.MustCompile(`^disconnection\: `),
	}
	LogStatement = LogMessage{
		ActionLog, "statement: ",
		regexp.MustCompile(`^.*statement\: `),
	}
	LogDuration = LogMessage{
		ActionLog, "duration: ",
		regexp.MustCompile(`^duration\: (\d+)\.(\d+) ms$`),
	}
	LogExtendedProtocolExecute = LogMessage{
		ActionLog, "execute <unnamed>: ",
		regexp.MustCompile(`^.*execute <unnamed>\: `),
	}
	LogExtendedProtocolParameters = LogMessage{
		ActionDetail, "parameters: ",
		regexp.MustCompile(`^parameters\: `),
	}
	LogNamedPrepareExecute = LogMessage{
		ActionLog, "execute ",
		regexp.MustCompile(`^.*execute (\w+)\: `),
	}
	LogError  = LogMessage{ActionError, "", regexp.MustCompile(`^ERROR\: .+`)}
	LogDetail = LogMessage{ActionDetail, "", regexp.MustCompile(`^DETAIL\: .+`)}
)

// ParseCsvItem constructs a Item from a CSV log line. The format we accept is log_destination='csvlog'.
func ParseCsvItem(logline []string, unbounds map[SessionID]*Execute, buffer []byte) (Item, error) {
	if len(logline) < 15 {
		return nil, fmt.Errorf("failed to parse log line: '%s'", logline)
	}

	ts, err := time.Parse(PostgresTimestampFormat, logline[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse log timestamp: '%s': %v", logline[0], err)
	}

	// 2023-06-09 01:50:01.825 UTC,"postgres","postgres",,,64828549.7698,,,,,,,,<msg>,<params>, ....
	user, database, session, actionLog, msg, params := logline[1], logline[2], logline[5], logline[11], logline[13], logline[14]

	extractedLog := ExtractedLog{
		Details: Details{
			Timestamp: ts,
			SessionID: SessionID(session),
			User:      user,
			Database:  database,
		},
		ActionLog:  actionLog,
		Message:    msg,
		Parameters: params,
	}

	return parseDetailToItem(extractedLog, ParsedFromCsv, unbounds, buffer)
}

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

	extractedLog := ExtractedLog{
		Details: Details{
			Timestamp: ts,
			SessionID: SessionID(session),
			User:      user,
			Database:  database,
		},
		ActionLog:  "",
		Message:    msg,
		Parameters: "",
	}

	return parseDetailToItem(extractedLog, ParsedFromErrLog, unbounds, buffer)
}

func parseDetailToItem(el ExtractedLog, parsedFrom string, unbounds map[SessionID]*Execute, buff []byte) (Item, error) {
	// LOG:  duration: 0.043 ms
	// Duration logs mark completion of replay items, and are not of interest for
	// reproducing traffic. We should only take an action if there exists an unbound item
	// for this session, as this log line will confirm the unbound query has no parameters.
	if LogDuration.Match(el.Message, parsedFrom) {
		if unbound, ok := unbounds[el.SessionID]; ok {
			delete(unbounds, el.SessionID)
			return unbound.Bind(nil), nil
		}

		return nil, nil
	}

	// LOG:  statement: select pg_reload_conf();
	if LogStatement.Match(el.Message, parsedFrom) {
		return Statement{el.Details, LogStatement.RenderQuery(el.Message, parsedFrom)}, nil
	}

	// LOG:  execute <unnamed>: select pg_sleep($1)
	// An execute log represents a potential statement. When running the extended protocol,
	// even queries that don't have any arguments will be sent as an unamed prepared
	// statement. We need to wait for a following DETAIL or duration log to confirm the
	// statement has been executed.
	if LogExtendedProtocolExecute.Match(el.Message, parsedFrom) {
		query := LogExtendedProtocolExecute.RenderQuery(el.Message, parsedFrom)

		if parsedFrom == ParsedFromCsv {
			params, err := ParseBindParameters(LogExtendedProtocolParameters.RenderQuery(el.Parameters, parsedFrom), buff)
			if err != nil {
				return nil, fmt.Errorf("[UnNamedExecute]: failed to parse bind parameters: %s", err.Error())
			}

			return Execute{el.Details, query}.Bind(params), nil
		}

		unbounds[el.SessionID] = &Execute{el.Details, query}

		return nil, nil
	}

	// LOG:  execute name: select pg_sleep($1)
	if LogNamedPrepareExecute.Match(el.Message, parsedFrom) {
		if parsedFrom == ParsedFromCsv {
			query := LogNamedPrepareExecute.RenderQuery(el.Message, parsedFrom)
			params, err := ParseBindParameters(LogExtendedProtocolParameters.RenderQuery(el.Parameters, parsedFrom), buff)
			if err != nil {
				return nil, fmt.Errorf("[NamedExecute]: failed to parse bind parameters: %s", err.Error())
			}

			return Execute{el.Details, query}.Bind(params), nil
		}

		query := strings.SplitN(
			LogNamedPrepareExecute.RenderQuery(el.Message, parsedFrom), ":", 2,
		)[1]

		// TODO: This doesn't exactly replicate what we'd expect from named prepares. Instead
		// of creating a genuine named prepare, we implement them as unnamed prepared
		// statements instead. If this parse signature allowed us to return arbitrary items
		// then we'd be able to create an initial prepare statement followed by a matching
		// execute, but we can hold off doing this until it becomes a problem.
		unbounds[el.SessionID] = &Execute{el.Details, query}

		return nil, nil
	}

	// DETAIL:  parameters: $1 = '1', $2 = NULL
	if LogExtendedProtocolParameters.Match(el.Message, parsedFrom) {
		if unbound, ok := unbounds[el.SessionID]; ok {
			parameters, err := ParseBindParameters(LogExtendedProtocolParameters.RenderQuery(el.Message, parsedFrom), buff)
			if err != nil {
				return nil, fmt.Errorf("failed to parse bind parameters: %s", err.Error())
			}

			// Remove the unbound from our cache and bind it
			delete(unbounds, el.SessionID)
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
		return nil, fmt.Errorf("cannot process bind parameters without previous execute item: %s", el.Message)
	}

	// LOG:  connection authorized: user=postgres database=postgres
	if LogConnectionAuthorized.Match(el.Message, parsedFrom) {
		return Connect{el.Details}, nil
	}

	// LOG:  disconnection: session time: 0:00:03.861 user=postgres database=postgres host=192.168.99.1 port=51529
	if LogConnectionDisconnect.Match(el.Message, parsedFrom) {
		return Disconnect{el.Details}, nil
	}

	// LOG:  connection received: host=192.168.99.1 port=52188
	// We use connection authorized for replay, and can safely ignore connection received
	if LogConnectionReceived.Match(el.Message, parsedFrom) {
		return nil, nil
	}

	// ERROR:  invalid value for parameter \"log_destination\": \"/var\"
	// We don't replicate errors as this should be the minority of our traffic. Can safely
	// ignore.
	if el.ActionLog == "ERROR" || LogError.Match(el.Message, parsedFrom) {
		return nil, nil
	}

	// DETAIL:  Unrecognized key word: \"/var/log/postgres/postgres.log\"
	// The previous condition catches the extended query bind detail statements, and any
	// other DETAIL logs we can safely ignore.
	if el.ActionLog == "DETAIL" || LogDetail.Match(el.Message, parsedFrom) {
		return nil, nil
	}

	return nil, fmt.Errorf("no parser matches line: %s", el.Message)
}

// ParseBindParameters constructs an interface slice from the suffix of a DETAIL parameter
// Postgres errlog. An example input to this function would be:
//
// $1 = ”, $2 = '30', $3 = '2018-05-03 10:26:27.905086+00'
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
//
//	DELETE FROM que_jobs
//	WHERE queue    = $1::text
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
