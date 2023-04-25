package tester

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tursom/GoCollections/util"

	"github.com/tursom/polycephalum"
)

func msgLogger(wg *sync.WaitGroup) func(channelType uint32, channel string, msg string, ctx util.ContextMap) {
	return func(channelType uint32, channel string, msg string, ctx util.ContextMap) {
		fmt.Printf("receive msg: type: %d, channel: %s, msg: %s\n", channelType, channel, msg)
		wg.Done()
	}
}

func Test_Polycephalum(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(16)

	ps := []polycephalum.Polycephalum[string]{
		newTestPolycephalum(msgLogger(&wg)),
		newTestPolycephalum(msgLogger(&wg)),
	}

	connect(ps[0], ps[1])
	time.Sleep(time.Second)

	channel := UserChannel("test1")
	_ = ps[1].Listen(channel)
	time.Sleep(time.Second)
	for i := 0; i < 16; i++ {
		ps[0].Broadcast(channel.Type, channel.Channel, fmt.Sprintf("hello %d times", i), nil)
	}
	wg.Wait()
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
