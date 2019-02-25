package integration

import (
	"os"
	"strconv"

	kitlog "github.com/go-kit/kit/log"
	"github.com/gocardless/pgreplay-go/pkg/pgreplay"
	"github.com/jackc/pgx"
	"github.com/onsi/gomega/types"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("pgreplay", func() {
	var (
		conn   *pgx.Conn
		logger = kitlog.NewLogfmtLogger(GinkgoWriter)
		err    error

		// We expect a Postgres database to be running for integration tests, and that
		// environment variables are appropriately configured to permit access.
		cfg = pgx.ConnConfig{
			Database: tryEnviron("PGDATABASE", "pgreplay_test"),
			Host:     tryEnviron("PGHOST", "127.0.0.1"),
			User:     tryEnviron("PGUSER", "pgreplay_test_users"),
			Password: tryEnviron("PGPASSWORD", ""),
			Port:     uint16(mustAtoi(tryEnviron("PGPORT", "5432"))),
		}
	)

	DescribeTable("Replaying logfiles",
		func(errlogfixture string, matchLogs []types.GomegaMatcher) {
			conn, err = pgx.Connect(cfg)
			Expect(err).NotTo(HaveOccurred(), "failed to connect to postgres")

			_, err = conn.Exec(`TRUNCATE logs;`)
			Expect(err).NotTo(HaveOccurred(), "failed to truncate logs table")

			database, err := pgreplay.NewDatabase(cfg)
			Expect(err).NotTo(HaveOccurred())

			errlog, err := os.Open(errlogfixture)
			Expect(err).NotTo(HaveOccurred())

			items, logerrs, parsingDone := pgreplay.Parse(errlog)
			go func() {
				defer GinkgoRecover()
				for err := range logerrs {
					logger.Log("event", "parse.error", "error", err)
				}
			}()

			stream, err := pgreplay.NewStreamer(nil, nil).Stream(items, 1.0)
			Expect(err).NotTo(HaveOccurred())

			errs, consumeDone := database.Consume(stream)

			// Expect that we finish with no errors
			Eventually(consumeDone).Should(BeClosed())
			Eventually(errs).Should(BeClosed())

			// Parsing should complete
			Eventually(parsingDone).Should(BeClosed())

			// Extract the logs that our test will have placed in the database
			logs, err := getLogs(conn)

			Expect(err).NotTo(HaveOccurred())
			Expect(len(logs)).To(Equal(len(matchLogs)))

			for idx, matchLog := range matchLogs {
				Expect(logs[idx]).To(matchLog)
			}
		},
		Entry("Single user", "testdata/single_user.log", []types.GomegaMatcher{
			matchLog("alice", "says hello"),
			matchLog("alice", "sees 1 logs"),
			matchLog("alice", "sees 2 of alice's logs"),
			matchLog("alice", "sees 0 of bob's logs"),
		}),
	)
})

func tryEnviron(key, otherwise string) string {
	if value, found := os.LookupEnv(key); found {
		return value
	}

	return otherwise
}

func mustAtoi(numstr string) int {
	num, err := strconv.Atoi(numstr)
	if err != nil {
		panic(err)
	}

	return num
}

func getLogs(conn *pgx.Conn) ([]interface{}, error) {
	rows, err := conn.Query(`SELECT id::text, author, message FROM logs ORDER BY id;`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var id, author, message string
	var logs = []interface{}{}

	for rows.Next() {
		if err := rows.Scan(&id, &author, &message); err != nil {
			return nil, err
		}

		logs = append(logs, &struct{ ID, Author, Message string }{id, author, message})
	}

	return logs, nil
}

func matchLog(author, message string) types.GomegaMatcher {
	return PointTo(
		MatchFields(IgnoreExtras, Fields{"Author": Equal(author), "Message": Equal(message)}),
	)
}
