package tester

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/tursom/GoCollections/util"

	"github.com/tursom/polycephalum"
	"github.com/tursom/polycephalum/distributed"
)

func msgLogger(wg *sync.WaitGroup) func(channelType uint32, channel string, msg string, ctx util.ContextMap) {
	return func(channelType uint32, channel string, msg string, ctx util.ContextMap) {
		fmt.Printf("receive msg: type: %d, channel: %s, msg: %s\n", channelType, channel, msg)
		wg.Done()
	}
}

func Test_Polycephalum(t *testing.T) {
	//log.SetLevel(log.DebugLevel)

	var wg sync.WaitGroup
	wg.Add(16)

	ps := []polycephalum.Polycephalum[string]{
		newTestPolycephalum("0", nil),
		newTestPolycephalum("1", nil),
		newTestPolycephalum("2", nil),
		newTestPolycephalum("3", nil),
		newTestPolycephalum("4", msgLogger(&wg)),
	}

	channel := UserChannel("test1")
	_ = ps[4].Listen(channel)

	for _, s := range []struct{ a, b int }{
		{0, 1},
		{0, 2},
		{1, 2},
		{1, 3},
		{2, 3},
		{2, 4},
		{3, 4},
	} {
		connect(ps[s.a], ps[s.b])
	}

	time.Sleep(time.Millisecond * 10)

	printNetwork(ps)

	for i := 0; i < 16; i++ {
		ps[0].Broadcast(channel.Type, channel.Channel, fmt.Sprintf("hello %d times", i), nil)
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

func Test_testIO(t1 *testing.T) {
	io := newTestIO()
	go func() {
		_, _ = io.Write([]byte("Hello"))
		_, _ = io.Write([]byte(" "))
		_, _ = io.Write([]byte("world"))
		_, _ = io.Write([]byte("!"))
		_ = io.Close()
	}()

	buf := make([]byte, 4)
	for {
		n, err := io.Read(buf)
		if err != nil {
			break
		}

		fmt.Printf("%s", string(buf[:n]))
	}
	fmt.Printf("\n")
}
