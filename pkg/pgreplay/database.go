package pgreplay

import (
	"fmt"
	"sync"

	"github.com/eapache/channels"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ConnectionsActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pgreplay",
			Name:      "connections_active",
			Help:      "Number of connections currently open against Postgres",
		},
	)
	ConnectionsEstablishedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pgreplay",
			Name:      "connections_established_total",
			Help:      "Number of connections established against Postgres",
		},
	)
	ItemsProcessedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pgreplay",
			Name:      "items_processed_total",
			Help:      "Total count of replay items that have been sent to the database",
		},
	)
	ItemsMostRecentTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pgreplay",
			Name:      "items_most_recent_timestamp",
			Help:      "Most recent timestamp of processed items",
		},
	)
)

func init() {
	prometheus.MustRegister(ConnectionsActive)
	prometheus.MustRegister(ConnectionsEstablishedTotal)
	prometheus.MustRegister(ItemsProcessedTotal)
	prometheus.MustRegister(ItemsMostRecentTimestamp)
}

func NewDatabase(cfg pgx.ConnConfig) (*Database, error) {
	conn, err := pgx.Connect(cfg)
	if err != nil {
		return nil, err
	}

	return &Database{
		cfg:         cfg,
		ConnInfo:    conn.ConnInfo,
		connections: map[SessionID]Connection{},
	}, conn.Close()
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
func (d *Database) Consume(items chan ReplayItem) (chan error, chan error) {
	errs, done := make(chan error), make(chan error)
	var wg sync.WaitGroup

	go func() {
		for item := range items {
			if item == nil {
				continue
			}

		Connect:

			if item, ok := item.(*ConnectItem); ok {
				if conn, err := d.Connect(&wg, item.Database(), item.User()); err == nil {
					d.connections[item.SessionID()] = conn
					go conn.Start()
				}

				continue
			}

			if conn, ok := d.connections[item.SessionID()]; ok {
				conn.items.In() <- item

				// If we're going to disconnect, then remove this connection from our pool and
				// close the channel.
				if _, ok := item.(*DisconnectItem); ok {
					conn.items.Close()
					delete(d.connections, item.SessionID())
				}
			} else {
				errs <- fmt.Errorf("no connection for session %s", item.SessionID())
				item = &ConnectItem{
					Item{session: item.SessionID(), database: item.Database(), user: item.User()},
				}

				goto Connect
			}
		}

		for _, conn := range d.connections {
			if !conn.closed {
				// Non-blocking channel op to avoid race between thinking the conn is closed and
				// it actually being closed.
				select {
				case conn.items.In() <- &DisconnectItem{}:
				default:
				}
			}

			conn.items.Close()
		}

		wg.Wait()

		close(errs)
		close(done)
	}()

	return errs, done
}

// Pending returns a slice of connections that are yet to be closed, and the number of
// pending items that are still to be processed by all connections.
func (d *Database) Pending() (conns []Connection, pending int) {
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
func (d *Database) Connect(wg *sync.WaitGroup, database, user string) (Connection, error) {
	cfg := d.cfg
	cfg.Database, cfg.User = database, user
	// cfg.CustomConnInfo = func(_ *pgx.Conn) (*pgtype.ConnInfo, error) {
	// 	return d.ConnInfo.DeepCopy(), nil
	// }

	conn, err := pgx.Connect(cfg)
	if err == nil {
		wg.Add(1)
		ConnectionsEstablishedTotal.Inc()
		ConnectionsActive.Inc()
	}

	return Connection{
		Conn:     conn,
		database: d,
		items:    channels.NewInfiniteChannel(),
		wg:       wg,
	}, err
}

type Connection struct {
	*pgx.Conn
	database *Database
	items    channels.Channel
	wg       *sync.WaitGroup
	closed   bool
	err      error
}

// Start begins to process the items that are placed into the Connection's channel. For
// every item we'll run the appropriate action for the current connection.
func (c Connection) Start() {
	items := make(chan ReplayItem)
	channels.Unwrap(c.items, items)

	for item := range items {
		if item == nil {
			continue
		}

		ItemsProcessedTotal.Inc()
		ItemsMostRecentTimestamp.Set(float64(item.Time().Unix()))

		switch item := item.(type) {
		case *ExecuteItem:
			c.Exec(item.Query, item.Parameters...)
		case *StatementItem:
			c.Exec(item.Query)
		case *DisconnectItem:
			if c.Conn != nil {
				c.err = c.Close()
			}
			c.closed = true
			c.wg.Done()

			ConnectionsActive.Dec()
			return
		}
	}
}
