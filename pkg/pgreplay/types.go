package pgreplay

import (
	"time"

	"github.com/jackc/pgx"
)

type (
	ReplayType int
	SessionID  string
)

// We support the following types of ReplayItem
var _ Item = &Connect{}
var _ Item = &Disconnect{}
var _ Item = &Statement{}
var _ Item = &BoundExecute{}

type Item interface {
	GetTimestamp() time.Time
	GetSessionID() SessionID
	GetUser() string
	GetDatabase() string
	Handle(*pgx.Conn) error
}

type Details struct {
	Timestamp time.Time `json:"timestamp"`
	SessionID SessionID `json:"session_id"`
	User      string    `json:"user"`
	Database  string    `json:"database"`
}

func (e Details) GetTimestamp() time.Time { return e.Timestamp }
func (e Details) GetSessionID() SessionID { return e.SessionID }
func (e Details) GetUser() string         { return e.User }
func (e Details) GetDatabase() string     { return e.Database }

type Connect struct{ Details }

func (_ Connect) Handle(_ *pgx.Conn) error {
	return nil // Database will manage opening connections
}

type Disconnect struct{ Details }

func (_ Disconnect) Handle(conn *pgx.Conn) error {
	return conn.Close()
}

type Statement struct {
	Details
	Query string `json:"query"`
}

func (s Statement) Handle(conn *pgx.Conn) error {
	_, err := conn.Exec(s.Query)
	return err
}

// Execute is parsed and awaiting arguments. It deliberately lacks a Handle method as it
// shouldn't be possible this statement to have been parsed without a following duration
// or detail line that bound it.
type Execute struct {
	Details
	Query string
}

func (e Execute) Bind(parameters []interface{}) BoundExecute {
	if parameters == nil {
		parameters = make([]interface{}, 0)
	}

	return BoundExecute{&e, parameters}
}

// BoundExecute represents an Execute that is now successfully bound with parameters
type BoundExecute struct {
	*Execute
	Parameters []interface{}
}

func (e BoundExecute) Handle(conn *pgx.Conn) error {
	_, err := conn.Exec(e.Query, e.Parameters...)
	return err
}
