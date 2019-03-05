package pgreplay

import (
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

var time20190225, _ = time.Parse(time.RFC3339, "2019-02-25T15:08:27.222+00:00")

var _ = Describe("Parse", func() {
	DescribeTable("Parses",
		func(input string, expected []Item) {
			var items = []Item{}
			itemsChan, errs, done := Parse(strings.NewReader(input))
			go func() {
				for _ = range errs {
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
2019-02-25 15:08:27.232 GMT|[unknown]|[unknown]|5c7404eb.d6bd|LOG:  connection received: host=127.0.0.1 port=59103
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
					Execute: &Execute{
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
					Execute: &Execute{
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
