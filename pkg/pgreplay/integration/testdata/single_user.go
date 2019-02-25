package main

import (
	"github.com/jackc/pgx"
)

func connect(user string) *pgx.Conn {
	conn, _ := pgx.Connect(
		pgx.ConnConfig{
			Host: "127.0.0.1", Port: 5432, Database: "pgreplay_test", User: user,
		},
	)

	return conn
}

var someoneSeesUser = `insert into logs (author, message) (
  select $1, format('sees %s of %s''s logs', count(*), $2::text) from logs where author = $2
);`

func main() {
	alice := connect("alice")
	alice.Exec(`insert into logs (author, message) values ('alice', 'says hello');`)
	alice.Exec(`insert into logs (author, message) (
  select 'alice', format('sees %s logs', count(*)) from logs
);`)

	// Named prepared statement
	alice.Prepare("someone_sees_user", someoneSeesUser)
	alice.Exec("someone_sees_user", "alice", "alice")

	// Unnamed prepared statement
	alice.Exec(someoneSeesUser, "alice", "bob")
}
