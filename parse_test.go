package pgreplay

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseBindParameters(t *testing.T) {
	testCases := []struct {
		name       string
		input      string
		parameters []interface{}
	}{
		{
			"parses single string parameter",
			"$1 = 'hello'",
			[]interface{}{"hello"},
		},
		{
			"parses escaped string parameter",
			"$1 = 'hel''lo'",
			[]interface{}{"hel'lo"},
		},
		{
			"parses NULL to nil",
			"$2 = NULL",
			[]interface{}{nil},
		},
		{
			"parses many string parameters",
			"$1 = 'hello', $2 = 'world'",
			[]interface{}{"hello", "world"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parameters, err := ParseBindParameters(tc.input, nil)

			assert.Nil(t, err, "expected no error")
			assert.EqualValues(t, tc.parameters, parameters)
		})
	}
}

func TestLogScanner(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		lines []string
	}{
		{
			"scans single lines",
			`2010-12-31 10:59:52.243 UTC|postgres`,
			[]string{
				`2010-12-31 10:59:52.243 UTC|postgres`,
			},
		},
		{
			"scans multiple lines",
			`2010-12-31 10:59:52.243 UTC|postgres
2010-12-31 10:59:53.000 UTC|paysvc`,
			[]string{
				`2010-12-31 10:59:52.243 UTC|postgres`,
				`2010-12-31 10:59:53.000 UTC|paysvc`,
			},
		},
		{
			"scans multi-line logs",
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
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scanner := NewLogScanner(strings.NewReader(tc.input), nil)
			lines := []string{}

			for scanner.Scan() {
				lines = append(lines, scanner.Text())
			}

			assert.Nil(t, scanner.Err(), "did not expect scanner to return error")
			assert.EqualValues(t, tc.lines, lines)
		})
	}
}
