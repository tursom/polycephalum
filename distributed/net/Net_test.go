package net

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tursom/GoCollections/lang"
)

type (
	testProcessor struct {
		lang.BaseObject
	}
)

func (t *testProcessor) Send(
	ctx context.Context,
	target []string,
	nextJmp string,
	msg []byte,
	jmp uint32,
) (unreachable []string) {
	return nil
}

func (t *testProcessor) Suspect(id string) {
}

func Test_node_stateChanged(t *testing.T) {
	n := &node{
		lock: &nodeLock{},
	}

	n.stateChanged(1, "", 100)

	n.waitSuspect()

	n.suspect(&testProcessor{})

	go func() {
		time.Sleep(time.Second)
		n.stateChanged(3, "", 100)
	}()

	n.waitSuspect()
	if !n.isOnline() || n.state != 3 {
		t.Fail()
	}
}

func Test_node_stateChanged_offline(t *testing.T) {
	n := &node{
		lock: &nodeLock{},
	}

	n.stateChanged(1, "", 100)

	n.suspect(&testProcessor{})

	go func() {
		time.Sleep(time.Second)
		n.stateChanged(4, "", 100)
	}()

	n.waitSuspect()
	if n.isOnline() || n.state != 4 {
		t.Fail()
	}
}

func Test_nodeCodec(t *testing.T) {
	n := &node{
		state:       102,
		suspectTime: time.Now(),
		nextJmp:     "test next jump",
		distance:    1,
	}

	locks := make(map[string]*nodeLock)

	encode := nodeCodec(locks).Encode(n)
	decode := nodeCodec(locks).Decode(encode)

	fmt.Println(decode)
}
