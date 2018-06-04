package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

type ReplayType int
type SessionID string
type QueryString string
type StatementName string

const (
	LogConnectionAuthorized       = "LOG:  connection authorized: "
	LogConnectionDisconnect       = "LOG:  disconnection: "
	LogStatement                  = "LOG:  statement: "
	LogExtendedProtocolExecute    = "LOG:  execute <unnamed>: "
	LogExtendedProtocolParameters = "DETAIL:  parameters: "
)

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

func (i *Item) Time() time.Time      { return i.ts }
func (i *Item) SessionID() SessionID { return i.session }
func (i *Item) User() string         { return i.user }
func (i *Item) Database() string     { return i.database }

type ConnectItem struct{ Item }
type DisconnectItem struct{ Item }

type ExecuteItem struct {
	Item
	Query      QueryString
	Parameters []interface{}
}

func Parse(errlog io.Reader) (chan ReplayItem, error) {
	lastItems := map[SessionID]ReplayItem{}
	previousForSession := func(id SessionID) ReplayItem { return lastItems[id] }

	scanner := NewLogScanner(errlog)
	for scanner.Scan() {
		item, err := ParseReplayItem(scanner.Text(), previousForSession)
		if err != nil {
			logger.Log("error", err.Error())
		}

		if item != nil {
			lastItems[item.SessionID()] = item
		}
	}

	return nil, nil
}

// ParseReplayItem constructs a ReplayItem from Postgres errlogs. The format we accept is
// log_line_prefix='%m|%u|%d|%c|', so we can split by | to discover each component.
func ParseReplayItem(logline string, previousForSession func(SessionID) ReplayItem) (ReplayItem, error) {
	tokens := strings.SplitN(logline, "|", 5)
	if len(tokens) != 5 {
		return nil, fmt.Errorf("failed to parse log line: '%s'", logline)
	}

	ts, err := time.Parse("2006-01-02 15:04:05.000 MST", tokens[0])
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

	// LOG:  connection authorized: user=postgres database=postgres
	if strings.HasPrefix(msg, LogConnectionAuthorized) {
		return &ConnectItem{item}, nil
	}

	// LOG:  disconnection: session time: 0:00:03.861 user=postgres database=postgres host=192.168.99.1 port=51529
	if strings.HasPrefix(msg, LogConnectionDisconnect) {
		return &DisconnectItem{item}, nil
	}

	// LOG:  statement: select pg_reload_conf();
	if strings.HasPrefix(msg, LogStatement) {
		return &ExecuteItem{
			Item:       item,
			Query:      QueryString(strings.TrimPrefix(msg, LogStatement)),
			Parameters: make([]interface{}, 0),
		}, nil
	}

	// LOG:  execute <unnamed>: select pg_sleep($1)
	if strings.HasPrefix(msg, LogExtendedProtocolExecute) {
		return &ExecuteItem{
			Item:       item,
			Query:      QueryString(strings.TrimPrefix(msg, LogStatement)),
			Parameters: nil, // this should be populated by a subsequent bind DETAIL
		}, nil
	}

	// DETAIL:  parameters: $1 = '1', $2 = NULL
	if strings.HasPrefix(msg, LogExtendedProtocolParameters) {
		previous := previousForSession(item.SessionID())
		if previous == nil {
			return nil, fmt.Errorf("cannot process bind parameters without previous item: %s", msg)
		}

		parameters, err := ParseBindParameters(strings.TrimPrefix(msg, LogExtendedProtocolParameters))
		if err != nil {
			return nil, fmt.Errorf("failed to parse bind parameters: %s", err.Error())
		}

		if previous, ok := previous.(*ExecuteItem); ok {
			// These parameters are assigned to the previous item, as this bind should be
			// associated with a preceding unnamed prepared exec.
			previous.Parameters = parameters
			return nil, nil
		}

		return nil, fmt.Errorf("bind statements must be preceded by execute statements, not %v", previous)
	}

	return nil, fmt.Errorf("no parser matches line: %s", msg)
}

// ParseBindParameters constructs an interface slice from the suffix of a DETAIL parameter
// Postgres errlog. An example input to this function would be:
//
// $1 = '', $2 = '30', $3 = '2018-05-03 10:26:27.905086+00'
//
// ...and this would be parsed into []interface{"", "30", "2018-05-03 10:26:27.905086+00"}
func ParseBindParameters(input string) ([]interface{}, error) {
	parameters := make([]interface{}, 0)
	prefixMatcher := regexp.MustCompile(`^(, )?\$\d+ = `)

	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
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
	})

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
func NewLogScanner(input io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(input)
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
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
	})

	return scanner
}
