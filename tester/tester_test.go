package tester

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/tursom/GoCollections/util"
	"github.com/tursom/GoCollections/util/time"

	"github.com/tursom/polycephalum"
	"github.com/tursom/polycephalum/distributed"
	"github.com/tursom/polycephalum/proto/m"
)

func logReceiver(wg *sync.WaitGroup) distributed.Receiver[string] {
	return func(channel m.Channel, msg string, ctx util.ContextMap) {
		fmt.Printf("receive msg: channel: %s, msg: %s\n", string(channel), msg)
		wg.Done()
	}
}

func unreachableReceiver(t *testing.T) distributed.Receiver[string] {
	return func(channel m.Channel, msg string, ctx util.ContextMap) {
		t.Fatalf("this branch should never be rached.")
	}
}

func Test_Polycephalum(t *testing.T) {
	//log.SetLevel(log.DebugLevel)

	var wg sync.WaitGroup
	wg.Add(16)

	ps := []polycephalum.Polycephalum[string]{
		newTestPolycephalum("0", unreachableReceiver(t)),
		newTestPolycephalum("1", unreachableReceiver(t)),
		newTestPolycephalum("2", unreachableReceiver(t)),
		newTestPolycephalum("3", unreachableReceiver(t)),
		newTestPolycephalum("4", logReceiver(&wg)),
	}

	channel := m.Channel("test1")
	_ = ps[4].Listen(channel)

	for _, s := range []struct {
		from  int
		links []int
	}{
		{0, []int{1}},
		{1, []int{2, 3}},
		{2, []int{3, 4}},
		{3, []int{4}},
	} {
		for _, link := range s.links {
			connect(ps[s.from], ps[link])
		}
	}

	time.Sleep(time.Millisecond * 10)

	printNetwork(ps)

	for i := 0; i < 16; i++ {
		ps[0].Broadcast(channel, fmt.Sprintf("hello %d times", i), nil)
	}
	wg.Wait()

	printNetwork(ps)
}

func printNetwork(ps []polycephalum.Polycephalum[string]) {
	for _, p := range ps {
		t := reflect.ValueOf(p)
		netField := t.MethodByName("Net").Call(nil)[0]

		net, ok := netField.Interface().(distributed.Net)
		if !ok {
			continue
		}

		for _, node := range net.Snap() {
			fmt.Printf("node %s: %s(%s,%d), state %d\n", p, node.Id, node.NextJump, node.Jmp, node.State)
		}
		fmt.Println()
	}
}

func Test_testIO(t *testing.T) {
	io := newTestIO()
	go func() {
		_, _ = io.Write([]byte("Hello"))
		_, _ = io.Write([]byte(" "))
		_, _ = io.Write([]byte("world"))
		_, _ = io.Write([]byte("!"))
		_ = io.Close()
	}()

	buffer := bytes.NewBuffer(nil)
	buf := make([]byte, 4)
	for {
		n, err := io.Read(buf)
		if err != nil {
			break
		}

		buffer.Write(buf[:n])
		fmt.Printf("%s", string(buf[:n]))
	}
	fmt.Printf("\n")

	if !bytes.Equal(buffer.Bytes(), []byte("Hello world!")) {
		t.Fatalf("failed to read the right msg")
	}
}
