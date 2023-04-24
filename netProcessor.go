package polycephalum

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/math"
	"github.com/tursom/GoCollections/lang"

	"github.com/tursom/polycephalum/proto/m"
)

type (
	netProcessor struct {
		lang.BaseObject
		lock sync.RWMutex

		m map[string]lang.SendChannel[*m.Msg]
	}
)

func (n *netProcessor) Send(ctx context.Context, target []string, nextJmp string, msg *m.Msg) (unreachable lang.ReceiveChannel[string]) {
	wc, ok := n.getWriteChannel(nextJmp)
	if !ok {
		return SliceChannel(target)
	}

	if trySend(wc, msg) {
		return nil
	} else {
		return SliceChannel(target)
	}
}

func trySend(wc lang.SendChannel[*m.Msg], msg *m.Msg) bool {
	select {
	case wc.SCh() <- msg:
		return true
	case <-time.After(time.Minute):
		return false
	}
}

func (n *netProcessor) getWriteChannel(node string) (lang.SendChannel[*m.Msg], bool) {
	n.lock.RLock()
	defer n.lock.RUnlock()

	wc, ok := n.m[node]
	return wc, ok
}

func (n *netProcessor) ChangeNodeState(id string, state uint32) {
	msg := m.Build(func(msg *m.Msg) {
		msg.BuildSyncNodeStateRequest(func(syncNodeStateRequest *m.SyncNodeStateRequest) {
			syncNodeStateRequest.States = []*m.NodeState{{
				Id:    id,
				State: state,
				Jump:  math.MaxInt32,
			}}
		})
	})

	n.lock.RLock()
	defer n.lock.RUnlock()

	for _, wc := range n.m {
		go trySend(wc, msg)
	}
}

func SliceChannel[E any](arr []E) lang.ReceiveChannel[E] {
	ch := make(lang.RawChannel[E])
	go func() {
		for _, e := range arr {
			ch <- e
		}

		close(ch)
	}()

	return ch
}
