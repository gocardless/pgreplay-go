package main

import (
	"context"
	"fmt"

	pgx "github.com/jackc/pgx/v5"
)

func connect(ctx context.Context, user string) *pgx.Conn {
	connConfig, _ := pgx.ParseConfig(
		fmt.Sprintf("postgres://%s@%s:%d/%s", user, "127.0.0.1", 5432, "pgreplay_test"),
	)

	conn, _ := pgx.ConnectConfig(
		ctx, connConfig,
	)

	return conn
}

var someoneSeesUser = `insert into logs (author, message) (
  select $1, format('sees %s of %s''s logs', count(*), $2::text) from logs where author = $2
);`

func main() {
	ctx := context.Background()
	alice := connect(ctx, "alice")
	alice.Exec(ctx, `insert into logs (author, message) values ('alice', 'says hello');`)
	alice.Exec(ctx, `insert into logs (author, message) (
  		select 'alice', format('sees %s logs', count(*)) from logs
	);`)

	// Named prepared statement
	alice.Prepare(ctx, "someone_sees_user", someoneSeesUser)
	alice.Exec(ctx, "someone_sees_user", "alice", "alice")

	// Unnamed prepared statement
	alice.Exec(ctx, someoneSeesUser, "alice", "bob")
}
