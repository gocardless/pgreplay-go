package main

import (
	"fmt"
	stdlog "log"
	"os"

	kitlog "github.com/go-kit/kit/log"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

var logger kitlog.Logger

func main() {
	logger = kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = kitlog.With(logger, "ts", kitlog.DefaultTimestampUTC, "caller", kitlog.DefaultCaller)
	stdlog.SetOutput(kitlog.NewStdlibAdapter(logger))

	/*
		var items = []*ReplayItem{
			&ReplayItem{Connect, "1", "postgres", "postgres", ""},
			&ReplayItem{Execute, "1", "postgres", "postgres", "select pg_sleep(2)"},
			&ReplayItem{Execute, "1", "postgres", "postgres", "set client_encoding to 'UTF8'"},
			&ReplayItem{Disconnect, "1", "postgres", "postgres", ""},
		}
	*/

	errlog, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}

	var noOfItems, noOfLogErrs int
	items, logerrs, errs := Parse(errlog)

	for {
		select {
		case item := <-items:
			if item != nil {
				noOfItems++
			}
		case <-logerrs:
			noOfLogErrs++
		case err := <-errs:
			if err != nil {
				panic(err)
			}

			fmt.Printf("Parsed %d replay items, with %d errors\n", noOfItems, noOfLogErrs)
			return
		}
	}

	return

	connector, err := NewConnector(pgx.ConnConfig{
		Host:     "192.168.99.100",
		Port:     32772,
		Database: "postgres",
		User:     "postgres",
	})

	if err != nil {
		panic(err.Error())
	}

	conn, _ := connector.Connect("postgres", "postgres")
	fmt.Println(
		conn.Exec("select pg_sleep($1)", "1"),
	)

	conn, _ = connector.Connect("postgres", "postgres")
	fmt.Println(
		conn.Exec(`select * from
payments where msg = $1`, "hello'\nworld"),
	)

	return

	/*
		connections := map[SessionID]*Connection{}

		logger.Log("event", "consume.start")
		for _, item := range items {
			if item.Type == Connect {
				conn, err := connector.Connect(item.Database, item.User)
				go conn.Start()

				if err != nil {
					ItemLogger(item).Log("event", "connect.error", "error", err)
				} else {
					ItemLogger(item).Log("event", "connect.success", "database", item.Database, "user", item.User)
					connections[item.SessionID] = conn
				}

				continue
			}

			conn, ok := connections[item.SessionID]
			if !ok {
				ItemLogger(item).Log("event", "connection.find", "error", "failed to find connection")
				continue
			}

			conn.Items <- item
		}

		for {
			pending := []*Connection{}
			for _, conn := range connections {
				if conn.Done() {
					continue
				}

				pending = append(pending, conn)
			}

			if len(pending) == 0 {
				break
			}

			logger.Log("event", "connections.pending", "count", len(pending))
			<-time.After(time.Second)
		}

		logger.Log("event", "consume.finish")
	*/
}

func ItemLogger(item ReplayItem) kitlog.Logger {
	return kitlog.With(logger, "SessionID", item.SessionID())
}

type Connector struct {
	cfg      pgx.ConnConfig
	ConnInfo *pgtype.ConnInfo
}

func NewConnector(cfg pgx.ConnConfig) (*Connector, error) {
	conn, err := pgx.Connect(cfg)
	if err != nil {
		return nil, err
	}

	return &Connector{cfg, conn.ConnInfo}, conn.Close()
}

func (c *Connector) Connect(database, user string) (*Connection, error) {
	cfg := c.cfg
	cfg.Database, cfg.User = database, user
	cfg.CustomConnInfo = func(_ *pgx.Conn) (*pgtype.ConnInfo, error) {
		return c.ConnInfo.DeepCopy(), nil
	}

	conn, err := pgx.Connect(cfg)
	return &Connection{conn, make(chan *ReplayItem, 10000), false}, err
}

type Connection struct {
	*pgx.Conn
	Items  chan *ReplayItem
	closed bool
}

func (c *Connection) Done() bool {
	return c.closed
}

func (c *Connection) Start() {
	if c.closed {
		panic("trying to start a closed connection")
	}

	/*
		for item := range c.Items {
			logger := ItemLogger(item)

			switch item.Type {
			case Disconnect:
				if err := c.Close(); err != nil {
					logger.Log("event", "disconnect.error", "error", err)
				} else {
					logger.Log("event", "disconnect.success")
				}

				c.closed = true
				return
			case Execute:
				if tag, err := c.Exec(item.Query); err != nil {
					logger.Log("event", "execute.error", "error", err)
				} else {
					logger.Log("event", "execute.success", "tag", tag)
				}
			default:
				logger.Log("error", "type.missing", "type", item.Type)
			}
		}
	*/
}
