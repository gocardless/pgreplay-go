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
var _ Item = &Execute{}
var _ Item = &BoundExecute{}

type Item interface {
	GetTimestamp() time.Time
	GetSessionID() SessionID
	GetUser() string
	GetDatabase() string
	Handle(*pgx.Conn) error
}

type Details struct {
	Timestamp time.Time
	SessionID SessionID
	User      string
	Database  string
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
	Query string
}

func (s Statement) Handle(conn *pgx.Conn) error {
	_, err := conn.Exec(s.Query)
	return err
}

type Execute struct {
	Details
	Query      string
	Parameters []interface{}
}

func (e Execute) Handle(conn *pgx.Conn) error {
	_, err := conn.Exec(e.Query, e.Parameters...)
	return err
}

// BoundExecute represents an Execute that is now successfully bound with parameters
type BoundExecute struct{ *Execute }
