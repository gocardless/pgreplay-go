package pgreplay

import (
	"sync"

	"github.com/eapache/channels"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	connectionsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "pgreplay_connections_active",
			Help: "Number of connections currently open against Postgres",
		},
	)
	connectionsEstablishedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_connections_established_total",
			Help: "Number of connections established against Postgres",
		},
	)
	itemsProcessedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_items_processed_total",
			Help: "Total count of replay items that have been sent to the database",
		},
	)
	itemsMostRecentTimestamp = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "pgreplay_items_most_recent_timestamp",
			Help: "Most recent timestamp of processed items",
		},
	)
)

func NewDatabase(cfg pgx.ConnConfig) (*Database, error) {
	conn, err := pgx.Connect(cfg)
	if err != nil {
		return nil, err
	}

	return &Database{cfg, conn.ConnInfo, map[SessionID]*Conn{}}, conn.Close()
}

type Database struct {
	pgx.ConnConfig
	*pgtype.ConnInfo
	conns map[SessionID]*Conn
}

// Consume iterates through all the items in the given channel and attempts to process
// them against the item's session connection. Consume returns two error channels, the
// first for per item errors that should be used for diagnostics only, and the second to
// indicate unrecoverable failures.
//
// Once all items have finished processing, both channels will be closed.
func (d *Database) Consume(items chan Item) (chan error, chan error) {
	var wg sync.WaitGroup

	errs, done := make(chan error, 10), make(chan error)

	go func() {
		for item := range items {
			var err error
			conn, ok := d.conns[item.GetSessionID()]

			// Connection did not exist, so create a new one
			if !ok {
				if conn, err = d.Connect(item); err != nil {
					errs <- err
					continue
				}

				d.conns[item.GetSessionID()] = conn

				wg.Add(1)
				connectionsEstablishedTotal.Inc()
				connectionsActive.Inc()

				go func(conn *Conn) {
					defer wg.Done()
					defer connectionsActive.Dec()

					if err := conn.Start(); err != nil {
						errs <- err
					}
				}(conn)
			}

			conn.In() <- item
		}

		for _, conn := range d.conns {
			conn.Close()
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

	return &Conn{conn, channels.NewInfiniteChannel(), sync.Once{}}, nil
}

// Conn represents a single database connection handling a stream of work Items
type Conn struct {
	*pgx.Conn
	channels.Channel
	sync.Once
}

func (c *Conn) Close() {
	c.Once.Do(c.Channel.Close)
}

// Start begins to process the items that are placed into the Conn's channel. We'll finish
// once the connection has died or we run out of items to process.
func (c *Conn) Start() error {
	items := make(chan Item)
	channels.Unwrap(c.Channel, items)
	defer c.Close()

	for item := range items {
		if item == nil {
			continue
		}

		itemsProcessedTotal.Inc()
		itemsMostRecentTimestamp.Set(float64(item.GetTimestamp().Unix()))

		err := item.Handle(c.Conn)

		// If we're no longer alive, then we know we can no longer process items
		if !c.IsAlive() {
			return err
		}
	}

	// If we're still alive after consuming all our items, assume that we finished
	// processing our logs before we saw this connection be disconnected. We should
	// terminate ourselves by handling our own disconnect, so we can know when all our
	// connection are done.
	if c.IsAlive() {
		Disconnect{}.Handle(c.Conn)
	}

	return nil
}
