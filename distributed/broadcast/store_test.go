package broadcast

import (
	"strconv"
	"testing"

	"github.com/tursom/GoCollections/util/bloom"
)

func Test_codec(t *testing.T) {
	n := &node{filter: bloom.NewBloom(100_000, 0.03)}
	for i := 0; i < 10_000; i++ {
		n.filter.Add([]byte(strconv.Itoa(i)))
	}

	encode := codecInstance.Encode(n)
	n = codecInstance.Decode(encode)

	for i := 0; i < 100_000; i++ {
		if !n.filter.Contains([]byte(strconv.Itoa(i))) {
			t.Fatal(i)
		}
	}
}
