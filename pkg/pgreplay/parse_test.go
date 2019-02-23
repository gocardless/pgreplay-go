package pgreplay

import (
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

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
			`2010-12-31 10:59:52.243 UTC|postgres
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
