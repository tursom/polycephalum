package tester

import (
	"bytes"
	"testing"
)

func TestBroadcastChannel_Marshal(t *testing.T) {
	expect := []byte{0, 0, 0, 1, 116, 101, 115, 116}
	marshal := UserChannel("test").Marshal()

	if bytes.Compare(marshal, expect) != 0 {
		t.Fatal(marshal)
	}
}
