package pgreplay

import (
	"sync"

	"github.com/eapache/channels"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ConnectionsActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "pgreplay_connections_active",
			Help: "Number of connections currently open against Postgres",
		},
	)
	ConnectionsEstablishedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_connections_established_total",
			Help: "Number of connections established against Postgres",
		},
	)
	ItemsProcessedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_items_processed_total",
			Help: "Total count of replay items that have been sent to the database",
		},
	)
	ItemsMostRecentTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "pgreplay_items_most_recent_timestamp",
			Help: "Most recent timestamp of processed items",
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

	return &Database{cfg, conn.ConnInfo}, conn.Close()
}

type Database struct {
	pgx.ConnConfig
	*pgtype.ConnInfo
}

// Consume iterates through all the items in the given channel and attempts to process
// them against the item's session connection. Consume returns two error channels, the
// first for per item errors that should be used for diagnostics only, and the second to
// indicate unrecoverable failures.
//
// Once all items have finished processing, both channels will be closed.
func (d *Database) Consume(items chan Item) (chan error, chan error) {
	var wg sync.WaitGroup

	conns := map[SessionID]*Conn{}
	errs, done := make(chan error, 10), make(chan error)

	go func() {
		for item := range items {
			var err error
			conn, ok := conns[item.GetSessionID()]

			// Connection did not exist, so create a new one
			if !ok {
				if conn, err = d.Connect(item); err != nil {
					errs <- err
					continue
				}

				conns[item.GetSessionID()] = conn

				wg.Add(1)
				ConnectionsEstablishedTotal.Inc()
				ConnectionsActive.Inc()

				go func(conn *Conn) {
					defer wg.Done()
					defer ConnectionsActive.Dec()

					if err := conn.Start(); err != nil {
						errs <- err
					}
				}(conn)
			}

			conn.In() <- item
		}

		// Flush disconnects down each of our connection sessions, ensuring even connections
		// that we don't have disconnects for in our logs get closed.
		for _, conn := range conns {
			if !conn.IsAlive() {
				// Non-blocking channel op to avoid read-write-race between checking whether the
				// connection is alive and the channel having been closed
				select {
				case conn.In() <- &Disconnect{}:
				default:
				}
			}
		}

		// Wait for every connection to terminate
		wg.Wait()

		close(errs)
		close(done)
	}()

	return errs, done
}

// Connect establishes a new connection to the database, reusing the ConnInfo that was
// generated when the Database was constructed. The wg is incremented whenever we
// establish a new connection and decremented when we disconnect.
func (d *Database) Connect(item Item) (*Conn, error) {
	cfg := d.ConnConfig
	cfg.Database, cfg.User = item.GetDatabase(), item.GetUser()
	cfg.CustomConnInfo = func(_ *pgx.Conn) (*pgtype.ConnInfo, error) {
		return d.ConnInfo.DeepCopy(), nil
	}

	conn, err := pgx.Connect(cfg)
	if err != nil {
		return nil, err
	}

	return &Conn{conn, channels.NewInfiniteChannel()}, nil
}

// Conn represents a single database connection handling a stream of work Items
type Conn struct {
	*pgx.Conn
	channels.Channel
}

// Start begins to process the items that are placed into the Conn's channel. We'll finish
// once the connection has died or we run out of items to process.
func (c *Conn) Start() error {
	items := make(chan Item)
	channels.Unwrap(c.Channel, items)
	defer c.Channel.Close()

	for item := range items {
		if item == nil {
			continue
		}

		ItemsProcessedTotal.Inc()
		ItemsMostRecentTimestamp.Set(float64(item.GetTimestamp().Unix()))

		err := item.Handle(c.Conn)

		// If we're no longer alive, then we know we can no longer process items
		if !c.IsAlive() {
			return err
		}
	}

	return nil
}
