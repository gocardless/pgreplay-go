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

// ItemBufferSize defines the size of the channel buffer when parsing ReplayItems.
// Allowing the channel to buffer makes a significant throughput improvement to the
// parsing.
var ItemBufferSize = 100

// PostgresTimestampFormat is the Go template format that we expect to find our errlog
var PostgresTimestampFormat = "2006-01-02 15:04:05.000 MST"

// Parse generates a stream of ReplayItems from the given PostgreSQL errlog. Log line
// parsing errors are returned down the errs channel, and we signal having finished our
// parsing by sending a value down the done channel.
func Parse(errlog io.Reader) (chan ReplayItem, chan error, chan error) {
	unbounds := map[SessionID]*ExecuteItem{}
	loglinebuffer, parsebuffer := make([]byte, MaxLogLineSize), make([]byte, MaxLogLineSize)
	scanner := NewLogScanner(errlog, loglinebuffer)

	items, errs, done := make(chan ReplayItem, ItemBufferSize), make(chan error), make(chan error)

	go func() {
		for scanner.Scan() {
			item, err := ParseReplayItem(scanner.Text(), unbounds, parsebuffer)
			if err != nil {
				errs <- err
				LogLinesErrorTotal.Inc()
			}

			if item != nil {
				// If we've successfully parsed an item, and we had an unbound execute stored in
				// our buffer for this connection, then we must assume our parsing set the
				// parameters and send this down our channel.
				if unbound, ok := unbounds[item.SessionID()]; ok {
					delete(unbounds, unbound.SessionID())
					items <- unbound
				}

				// If this is an execute item, then we don't yet know if we're complete. Our bind
				// parameters may be yet to follow.
				if unbound, ok := item.(*ExecuteItem); ok {
					unbounds[item.SessionID()] = unbound
				} else {
					items <- item
				}

				LogLinesParsedTotal.Inc()
			}
		}

		// We've reached the end of our logs, so any unprocessed unbounds will never have
		// subsequent statements, so we must assume them to be bound and push then down our
		// channel.
		for _, maybeBound := range unbounds {
			items <- maybeBound
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

type ReplayType int
type SessionID string

type ReplayItem interface {
	SessionID() SessionID
	Time() time.Time
	User() string
	Database() string
}

type Item struct {
	ts       time.Time
	session  SessionID
	user     string
	database string
}

func (i Item) Time() time.Time      { return i.ts }
func (i Item) SessionID() SessionID { return i.session }
func (i Item) User() string         { return i.user }
func (i Item) Database() string     { return i.database }

type QueryItem struct {
	Query      string
	Parameters []interface{}
}

// We can parse our log lines into the following types
type ConnectItem struct{ Item }
type DisconnectItem struct{ Item }
type StatementItem struct {
	Item
	QueryItem
}
type ExecuteItem struct {
	Item
	QueryItem
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
	LogDetail                     = "DETAIL:  "
	LogError                      = "ERROR:  "
)

// ParseReplayItem constructs a ReplayItem from Postgres errlogs. The format we accept is
// log_line_prefix='%m|%u|%d|%c|', so we can split by | to discover each component.
//
// The previousItems map allows retrieval of the ReplayItem that was previously parsed for
// a given session, as extended query bind log lines will be parsed into parameters for
// the previous execute, rather than producing an item themselves.
func ParseReplayItem(logline string, unbounds map[SessionID]*ExecuteItem, buffer []byte) (ReplayItem, error) {
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

	item := Item{
		ts:       ts,
		user:     user,
		database: database,
		session:  SessionID(session),
	}

	// LOG:  duration: 0.043 ms
	// Duration logs mark completion of replay items, and are not of interest for
	// reproducing traffic. Can safely ignore.
	if strings.HasPrefix(msg, LogDuration) {
		return nil, nil
	}

	// LOG:  statement: select pg_reload_conf();
	if strings.HasPrefix(msg, LogStatement) {
		return &StatementItem{
			Item: item,
			QueryItem: QueryItem{
				Query: strings.TrimPrefix(msg, LogStatement),
			},
		}, nil
	}

	// LOG:  execute <unnamed>: select pg_sleep($1)
	if strings.HasPrefix(msg, LogExtendedProtocolExecute) {
		return &ExecuteItem{
			Item: item,
			QueryItem: QueryItem{
				Query:      strings.TrimPrefix(msg, LogExtendedProtocolExecute),
				Parameters: make([]interface{}, 0), // this will potentially be replaced by subsequent bind
			},
		}, nil
	}

	// DETAIL:  parameters: $1 = '1', $2 = NULL
	if strings.HasPrefix(msg, LogExtendedProtocolParameters) {
		if unbound, ok := unbounds[item.SessionID()]; ok {
			parameters, err := ParseBindParameters(strings.TrimPrefix(msg, LogExtendedProtocolParameters), buffer)
			if err != nil {
				return nil, fmt.Errorf("failed to parse bind parameters: %s", err.Error())
			}

			// These parameters are assigned to the previous item, as this bind should be
			// associated with a preceding unnamed prepared exec.
			unbound.Parameters = parameters
			return nil, nil
		}

		return nil, fmt.Errorf("cannot process bind parameters without previous execute item: %s", msg)
	}

	// LOG:  connection authorized: user=postgres database=postgres
	if strings.HasPrefix(msg, LogConnectionAuthorized) {
		return &ConnectItem{item}, nil
	}

	// LOG:  disconnection: session time: 0:00:03.861 user=postgres database=postgres host=192.168.99.1 port=51529
	if strings.HasPrefix(msg, LogConnectionDisconnect) {
		return &DisconnectItem{item}, nil
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
