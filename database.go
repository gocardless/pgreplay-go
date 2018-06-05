package pgreplay

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/eapache/channels"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

func NewDatabase(cfg pgx.ConnConfig) (Database, error) {
	conn, err := pgx.Connect(cfg)
	if err != nil {
		return Database{}, err
	}

	return Database{cfg, conn.ConnInfo, map[SessionID]Connection{}}, conn.Close()
}

type Database struct {
	cfg         pgx.ConnConfig
	ConnInfo    *pgtype.ConnInfo
	connections map[SessionID]Connection
}

// Consume iterates through all the items in the given channel and attempts to process
// them against the item's session connection. Consume returns two error channels, the
// first for per item errors that should be used for diagnostics only, and the second to
// indicate unrecoverable failures.
//
// Once all items have finished processing, both channels will be closed.
func (d Database) Consume(items chan ReplayItem) (chan error, chan error) {
	errs, done := make(chan error), make(chan error)
	var wg sync.WaitGroup

	go func() {
		for item := range items {
			if item == nil {
				continue
			}

			if item, ok := item.(*ConnectItem); ok {
				conn, err := d.Connect(&wg, item.Database(), item.User())
				if err != nil {
					errs <- err
				}

				d.connections[item.SessionID()] = conn
				go conn.Start()

				continue
			}

			if conn, ok := d.connections[item.SessionID()]; ok {
				conn.items.In() <- item
			} else {
				errs <- fmt.Errorf("no connection for session %s", item.SessionID)
			}
		}

		pendingConns, _ := d.Pending()
		for _, conn := range pendingConns {
			conn.items.In() <- &DisconnectItem{}
		}

		wg.Wait()

		close(errs)
		close(done)
	}()

	return errs, done
}

// Pending returns a slice of connections that are yet to be closed, and the number of
// pending items that are still to be processed by all connections.
func (d Database) Pending() (conns []Connection, pending int) {
	conns = []Connection{}
	for _, conn := range d.connections {
		if !conn.closed {
			conns = append(conns, conn)
			pending += conn.items.Len()
		}
	}

	return
}

// Connect establishes a new connection to the database, reusing the ConnInfo that was
// generated when the Database was constructed. The wg is incremented whenever we
// establish a new connection and decremented when we disconnect.
func (d Database) Connect(wg *sync.WaitGroup, database, user string) (Connection, error) {
	cfg := d.cfg
	cfg.Database, cfg.User = database, user
	cfg.CustomConnInfo = func(_ *pgx.Conn) (*pgtype.ConnInfo, error) {
		return d.ConnInfo.DeepCopy(), nil
	}

	conn, err := pgx.Connect(cfg)
	if err == nil {
		wg.Add(1)
	}

	return Connection{conn, channels.NewInfiniteChannel(), wg, false, nil}, err
}

type Connection struct {
	*pgx.Conn
	items  channels.Channel
	wg     *sync.WaitGroup
	closed bool
	err    error
}

func (c Connection) Start() {
	for item := range c.items.Out() {
		if item == nil || c.closed {
			continue
		}

		switch item := item.(type) {
		case *DisconnectItem:
			if c.Conn != nil {
				c.err = c.Close()
			}
			c.closed = true
			c.items.Close()
			c.wg.Done()

			return
		case *ExecuteItem:
			c.Exec(string(item.Query), item.Parameters...)
		default:
			panic("sent wrong item to Start " + reflect.TypeOf(item).String())
		}
	}
}
