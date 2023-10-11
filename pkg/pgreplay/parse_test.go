package pgreplay

import (
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var time20190225, _ = time.Parse(PostgresTimestampFormat, "2019-02-25 15:08:27.222 GMT")

var _ = Describe("ParseCsvLog", func() {
	DescribeTable("Parses",
		func(input string, expected []Item) {
			var items = []Item{}
			itemsChan, errs, done := ParseCsvLog(strings.NewReader(input))
			go func() {
				for range errs {
					// no-op, just drain the channel
				}
			}()

			for item := range itemsChan {
				if item != nil {
					items = append(items, item)
				}
			}

			Eventually(done).Should(BeClosed())
			Expect(len(items)).To(Equal(len(expected)))

			for idx, item := range items {
				Expect(item).To(BeEquivalentTo(expected[idx]))
			}
		},
		Entry(
			"queries and duration logs",
			`
2019-02-25 15:08:27.222 GMT,"postgres","postgres",7283,"199.167.158.43:57426",6480e39e.1c73,6374,"SELECT",2019-02-25 15:08:27.222 GMT,4/286618,0,LOG,00000,"connection received: host=127.0.0.1 port=59103",,,,,,,,,"","client backend"
2019-02-25 15:08:27.222 GMT,"postgres","postgres",7283,"199.167.158.43:57426",6480e39e.1c73,6374,"SELECT",2019-02-25 15:08:27.222 GMT,4/286618,0,LOG,00000,"connection authorized: user=alice database=pgreplay_test",,,,,,,,,"","client backend"
2019-02-25 15:08:27.222 GMT,"postgres","postgres",7283,"199.167.158.43:57426",6480e39e.1c73,6374,"SELECT",2019-02-25 15:08:27.222 GMT,4/286618,0,LOG,00000,"duration: 71.963 ms",,,,,,,,,"","client backend"
2019-02-25 15:08:27.222 GMT,"postgres","postgres",7283,"199.167.158.43:57426",6480e39e.1c73,6374,"SELECT",2019-02-25 15:08:27.222 GMT,4/286618,0,LOG,00000,"execute <unnamed>: select t.oid",,,,,,,,,"","client backend"
2019-02-25 15:08:27.222 GMT,"postgres","postgres",7283,"199.167.158.43:57426",6480e39e.1c73,6375,"idle in transaction",2019-02-25 15:08:27.222 GMT,4/286618,0,LOG,00000,"statement: SELECT p.name, r.rating
						FROM products p
						JOIN reviews r ON p.id = r.product_id
						WHERE r.rating IN (
						SELECT MIN(rating) FROM reviews
						UNION
						SELECT MAX(rating) FROM reviews
						);
				",,,,,,,,,"","client backend"
2019-02-25 15:08:27.222 GMT,"postgres","postgres",7283,"199.167.158.43:57426",6480e39e.1c73,6376,"SELECT",2019-02-25 15:08:27.222 GMT,4/286618,0,LOG,00000,"duration: 53.774 ms",,,,,,,,,"","client backend"
2019-02-25 15:08:27.222 GMT,"postgres","postgres",7283,"199.167.158.43:57426",6480e39e.1c73,6377,"idle in transaction",2019-02-25 15:08:27.222 GMT,4/286618,0,LOG,00000,"statement: SELECT name, email
						FROM users
						WHERE email LIKE '@gmail.com';
				",,,,,,,,,"","client backend"`,
			[]Item{
				Connect{
					Details{
						Timestamp: time20190225,
						SessionID: "6480e39e.1c73",
						User:      "postgres",
						Database:  "postgres",
					},
				},
				Statement{
					Details: Details{
						Timestamp: time20190225,
						SessionID: "6480e39e.1c73",
						User:      "postgres",
						Database:  "postgres",
					},
					Query: "SELECT p.name, r.rating\n\t\t\t\t\t\tFROM products p\n\t\t\t\t\t\tJOIN reviews r ON p.id = r.product_id\n\t\t\t\t\t\tWHERE r.rating IN (\n\t\t\t\t\t\tSELECT MIN(rating) FROM reviews\n\t\t\t\t\t\tUNION\n\t\t\t\t\t\tSELECT MAX(rating) FROM reviews\n\t\t\t\t\t\t);\n\t\t\t\t",
				},
				BoundExecute{
					Execute: Execute{
						Details: Details{
							Timestamp: time20190225,
							SessionID: "6480e39e.1c73",
							User:      "postgres",
							Database:  "postgres",
						},
						Query: "select t.oid",
					},
					Parameters: []interface{}{},
				},
				Statement{
					Details: Details{
						Timestamp: time20190225,
						SessionID: "6480e39e.1c73",
						User:      "postgres",
						Database:  "postgres",
					},
					Query: "SELECT name, email\n\t\t\t\t\t\tFROM users\n\t\t\t\t\t\tWHERE email LIKE '@gmail.com';\n\t\t\t\t",
				},
			},
		),
	)
})

var _ = Describe("ParseErrlog", func() {
	DescribeTable("Parses",
		func(input string, expected []Item) {
			var items = []Item{}
			itemsChan, errs, done := ParseErrlog(strings.NewReader(input))
			go func() {
				for range errs {
					// no-op, just drain the channel
				}
			}()

			for item := range itemsChan {
				if item != nil {
					items = append(items, item)
				}
			}

			Eventually(done).Should(BeClosed())
			Expect(len(items)).To(Equal(len(expected)))

			for idx, item := range items {
				Expect(item).To(BeEquivalentTo(expected[idx]))
			}
		},
		Entry(
			"Extended protocol with duration logs",
			`
2019-02-25 15:08:27.222 GMT|[unknown]|[unknown]|5c7404eb.d6bd|LOG:  connection received: host=127.0.0.1 port=59103
2019-02-25 15:08:27.222 GMT|alice|pgreplay_test|5c7404eb.d6bd|LOG:  connection authorized: user=alice database=pgreplay_test
2019-02-25 15:08:27.222 GMT|alice|pgreplay_test|5c7404eb.d6bd|LOG:  duration: 0.968 ms  parse <unnamed>: select t.oid
2019-02-25 15:08:27.222 GMT|alice|pgreplay_test|5c7404eb.d6bd|LOG:  duration: 1.100 ms  bind <unnamed>: select t.oid
2019-02-25 15:08:27.222 GMT|alice|pgreplay_test|5c7404eb.d6bd|LOG:  execute <unnamed>: select t.oid
2019-02-25 15:08:27.222 GMT|alice|pgreplay_test|5c7404eb.d6bd|LOG:  duration: 0.326 ms

2019-02-25 15:08:27.222 GMT|alice|pgreplay_test|5c7404eb.d6bd|LOG:  duration: 0.042 ms  parse <unnamed>: insert into logs (author, message) ($1, $2)
2019-02-25 15:08:27.222 GMT|alice|pgreplay_test|5c7404eb.d6bd|LOG:  duration: 0.045 ms  bind <unnamed>: insert into logs (author, message) ($1, $2)
2019-02-25 15:08:27.222 GMT|alice|pgreplay_test|5c7404eb.d6bd|DETAIL:  parameters: $1 = 'alice', $2 = 'bob'
2019-02-25 15:08:27.222 GMT|alice|pgreplay_test|5c7404eb.d6bd|LOG:  execute <unnamed>: insert into logs (author, message) ($1, $2)
2019-02-25 15:08:27.222 GMT|alice|pgreplay_test|5c7404eb.d6bd|DETAIL:  parameters: $1 = 'alice', $2 = 'bob'
2019-02-25 15:08:27.222 GMT|alice|pgreplay_test|5c7404eb.d6bd|LOG:  duration: 0.042 ms`,
			[]Item{
				Connect{
					Details{
						Timestamp: time20190225,
						SessionID: "5c7404eb.d6bd",
						User:      "alice",
						Database:  "pgreplay_test",
					},
				},
				BoundExecute{
					Execute: Execute{
						Details: Details{
							Timestamp: time20190225,
							SessionID: "5c7404eb.d6bd",
							User:      "alice",
							Database:  "pgreplay_test",
						},
						Query: "select t.oid",
					},
					Parameters: []interface{}{},
				},
				BoundExecute{
					Execute: Execute{
						Details: Details{
							Timestamp: time20190225,
							SessionID: "5c7404eb.d6bd",
							User:      "alice",
							Database:  "pgreplay_test",
						},
						Query: "insert into logs (author, message) ($1, $2)",
					},
					Parameters: []interface{}{"alice", "bob"},
				},
			},
		),
	)
})

var _ = Describe("ParseBindParameters", func() {
	DescribeTable("Parses",
		func(input string, expected []interface{}) {
			Expect(ParseBindParameters(input, nil)).To(
				BeEquivalentTo(expected),
			)
		},
		Entry("Single string parameter", "$1 = 'hello'", []interface{}{"hello"}),
		Entry("Single escaped string parameter", "$1 = 'hel''lo'", []interface{}{"hel'lo"}),
		Entry("NULL to nil", "$2 = NULL", []interface{}{nil}),
		Entry("Many string parameters", "$1 = 'hello', $2 = 'world'", []interface{}{"hello", "world"}),
	)
})

var _ = Describe("LogScanner", func() {
	DescribeTable("Scans",
		func(input string, expected []string) {
			scanner := NewLogScanner(strings.NewReader(input), nil)
			lines := []string{}

			for scanner.Scan() {
				lines = append(lines, scanner.Text())
			}

			Expect(scanner.Err()).NotTo(HaveOccurred())
			Expect(lines).To(Equal(expected))
		},
		Entry(
			"Single lines",
			`2010-12-31 10:59:52.243 UTC|postgres`,
			[]string{
				`2010-12-31 10:59:52.243 UTC|postgres`,
			},
		),
		Entry(
			"Multiple lines",
			`
2010-12-31 10:59:52.243 UTC|postgres
2010-12-31 10:59:53.000 UTC|paysvc`,
			[]string{
				`2010-12-31 10:59:52.243 UTC|postgres`,
				`2010-12-31 10:59:53.000 UTC|paysvc`,
			},
		),
		Entry(
			"Multi-line lines",
			`
2018-05-03|gc|LOG:  statement: select max(id),min(id) from pg2pubsub.update_log;
2018-05-03|gc|LOG:  duration: 0.096 ms  parse <unnamed>:
	DELETE FROM que_jobs
	WHERE queue    = $1::text

2018-05-03|gc|LOG:  duration: 0.248 ms
			`,
			[]string{
				`2018-05-03|gc|LOG:  statement: select max(id),min(id) from pg2pubsub.update_log;`,
				"2018-05-03|gc|LOG:  duration: 0.096 ms  parse <unnamed>:\nDELETE FROM que_jobs\nWHERE queue    = $1::text",
				`2018-05-03|gc|LOG:  duration: 0.248 ms`,
			},
		),
	)
})
