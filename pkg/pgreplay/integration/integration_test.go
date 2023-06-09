package integration

import (
	"context"
	"fmt"
	"os"
	"strconv"

	kitlog "github.com/go-kit/kit/log"
	"github.com/gocardless/pgreplay-go/pkg/pgreplay"
	"github.com/jackc/pgx/v5"
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
	)

	DescribeTable("Replaying logfiles",
		func(parser pgreplay.ParserFunc, fixture string, matchLogs []types.GomegaMatcher) {
			// We expect a Postgres database to be running for integration tests, and that
			// environment variables are appropriately configured to permit access.
			ctx := context.Background()
			cfg, err := pgx.ParseConfig(fmt.Sprintf(
				"host=%s port=%d dbname=%s user=%s password=%s",
				tryEnviron("PGHOST", "127.0.0.1"),
				uint16(mustAtoi(tryEnviron("PGPORT", "5432"))),
				tryEnviron("PGDATABASE", "pgreplay_test"),
				tryEnviron("PGUSER", "pgreplay_test_users"),
				tryEnviron("PGPASSWORD", ""),
			))
			if err != nil {
				Fail(fmt.Sprintf("failed to parse postgres connection config: %v", err))
			}

			conn, err = pgx.ConnectConfig(ctx, cfg)
			Expect(err).NotTo(HaveOccurred(), "failed to connect to postgres")

			_, err = conn.Exec(ctx, `TRUNCATE logs;`)
			Expect(err).NotTo(HaveOccurred(), "failed to truncate logs table")

			database, err := pgreplay.NewDatabase(ctx, cfg)
			Expect(err).NotTo(HaveOccurred())

			log, err := os.Open(fixture)
			Expect(err).NotTo(HaveOccurred())

			items, logerrs, parsingDone := parser(log)
			go func() {
				defer GinkgoRecover()
				for err := range logerrs {
					logger.Log("event", "parse.error", "error", err)
				}
			}()

			stream, err := pgreplay.NewStreamer(nil, nil).Stream(items, 1.0)
			Expect(err).NotTo(HaveOccurred())

			errs, consumeDone := database.Consume(ctx, stream)

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
		Entry("Single user (errlog)", pgreplay.ParseErrlog, "testdata/single_user.log", []types.GomegaMatcher{
			matchLog("alice", "says hello"),
			matchLog("alice", "sees 1 logs"),
			matchLog("alice", "sees 2 of alice's logs"),
			matchLog("alice", "sees 0 of bob's logs"),
		}),
		Entry("Single user (json)", pgreplay.ParseJSON, "testdata/single_user.json", []types.GomegaMatcher{
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
	ctx := context.Background()
	rows, err := conn.Query(ctx, `SELECT id::text, author, message FROM logs ORDER BY id;`)
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
