package pgreplay

import (
	"encoding/json"

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

	// We use this as Gomega chokes on these types for some subtle reason I don't
	// understand. Converting to and from json and comparing the serialized result is the
	// most robust equivilence.
	expectEqualUnderJSON := func(a interface{}, b interface{}) {
		var aa, bb interface{}
		var err error

		abytes, err := json.Marshal(a)
		Expect(err).To(Succeed())

		bbytes, err := json.Marshal(b)
		Expect(err).To(Succeed())

		Expect(json.Unmarshal(abytes, &aa)).To(Succeed())
		Expect(json.Unmarshal(bbytes, &bb)).To(Succeed())

		Expect(aa).To(Equal(bb))
	}

	// This confirms that the Marshal and Unmarshal operations are complementary
	expectIsReversible := func(item Item) {
		itemJSON, err := ItemMarshalJSON(item)
		Expect(err).NotTo(HaveOccurred(), "failed to marshal item")

		reversed, err := ItemUnmarshalJSON(itemJSON)
		Expect(err).NotTo(HaveOccurred(), "failed to unmarshal marshalled item")

		expectEqualUnderJSON(item, reversed)
	}

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

		It("Is reversible", func() {
			expectIsReversible(item)
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

		It("Is reversible", func() {
			expectIsReversible(item)
		})
	})
})
