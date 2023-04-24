package net

import (
	"fmt"
	"testing"
	"time"
)

func Test_node_stateChanged(t *testing.T) {
	n := &node{
		lock: &nodeLock{},
	}

	n.stateChanged(1, "", 100, nil)

	n.waitSuspect()

	n.suspect(func(id string) {})

	go func() {
		time.Sleep(time.Second)
		n.stateChanged(3, "", 100, nil)
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

	n.stateChanged(1, "", 100, nil)

	n.suspect(func(id string) {})

	go func() {
		time.Sleep(time.Second)
		n.stateChanged(4, "", 100, nil)
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
