package pgreplay

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Item JSON", func() {
	var (
		details = Details{
			Timestamp: time20190225,
			SessionID: "5c7404eb.d6bd",
			User:      "alice",
			Database:  "pgreplay_test",
		}
	)

	Context("Statement", func() {
		var item = Statement{details, "select now()"}

		It("Generates JSON", func() {
			Expect(ItemMarshalJSON(item)).To(
				MatchJSON(`
{
  "type": "Statement",
  "item": {
    "timestamp": "2019-02-25T15:08:27.222Z",
    "session_id": "5c7404eb.d6bd",
    "user": "alice",
    "database": "pgreplay_test",
    "query": "select now()"
  }
}`),
			)
		})
	})

	Context("BoundExecute", func() {
		var item = BoundExecute{Execute{details, "select $1"}, []interface{}{"hello"}}

		It("Generates JSON", func() {
			Expect(ItemMarshalJSON(item)).To(
				MatchJSON(`
{
  "type": "BoundExecute",
  "item": {
    "timestamp": "2019-02-25T15:08:27.222Z",
    "session_id": "5c7404eb.d6bd",
    "user": "alice",
    "database": "pgreplay_test",
    "query": "select $1",
		"parameters": ["hello"]
  }
}`),
			)
		})
	})
})
