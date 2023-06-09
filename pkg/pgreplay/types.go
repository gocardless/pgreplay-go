package pgreplay

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type (
	ReplayType int
	SessionID  string
)

const (
	ConnectLabel      = "Connect"
	StatementLabel    = "Statement"
	BoundExecuteLabel = "BoundExecute"
	DisconnectLabel   = "Disconnect"
)

func ItemMarshalJSON(item Item) ([]byte, error) {
	type envelope struct {
		Type string `json:"type"`
		Item Item   `json:"item"`
	}

	switch item.(type) {
	case Connect, *Connect:
		return json.Marshal(envelope{Type: ConnectLabel, Item: item})
	case Statement, *Statement:
		return json.Marshal(envelope{Type: StatementLabel, Item: item})
	case BoundExecute, *BoundExecute:
		return json.Marshal(envelope{Type: BoundExecuteLabel, Item: item})
	case Disconnect, *Disconnect:
		return json.Marshal(envelope{Type: DisconnectLabel, Item: item})
	default:
		return nil, nil // it's not important for us to serialize this
	}
}

func ItemUnmarshalJSON(payload []byte) (Item, error) {
	envelope := struct {
		Type string             `json:"type"`
		Item stdjson.RawMessage `json:"item"`
	}{}

	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, err
	}

	var item Item

	switch envelope.Type {
	case ConnectLabel:
		item = &Connect{}
	case StatementLabel:
		item = &Statement{}
	case BoundExecuteLabel:
		item = &BoundExecute{}
	case DisconnectLabel:
		item = &Disconnect{}
	default:
		return nil, fmt.Errorf("did not recognise type: %s", envelope.Type)
	}

	return item, json.Unmarshal(envelope.Item, item)
}

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
	Handle(context.Context, *pgx.Conn) error
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

func (_ Connect) Handle(ctx context.Context, _ *pgx.Conn) error {
	return nil // Database will manage opening connections
}

type Disconnect struct{ Details }

func (_ Disconnect) Handle(ctx context.Context, conn *pgx.Conn) error {
	return conn.Close(ctx)
}

type Statement struct {
	Details
	Query string `json:"query"`
}

func (s Statement) Handle(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, s.Query)
	return err
}

// Execute is parsed and awaiting arguments. It deliberately lacks a Handle method as it
// shouldn't be possible this statement to have been parsed without a following duration
// or detail line that bound it.
type Execute struct {
	Details
	Query string `json:"query"`
}

func (e Execute) Bind(parameters []interface{}) BoundExecute {
	if parameters == nil {
		parameters = make([]interface{}, 0)
	}

	return BoundExecute{e, parameters}
}

// BoundExecute represents an Execute that is now successfully bound with parameters
type BoundExecute struct {
	Execute
	Parameters []interface{} `json:"parameters"`
}

func (e BoundExecute) Handle(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, e.Query, e.Parameters...)
	return err
}
