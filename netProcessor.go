package polycephalum

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/math"
	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util/mr"

	"github.com/tursom/polycephalum/distributed"
	"github.com/tursom/polycephalum/proto/m"
)

type (
	netProcessor struct {
		lang.BaseObject
		lock sync.RWMutex

		m   map[string]lang.SendChannel[*m.Msg]
		nss nodeStateSyncer
	}

	nodeStateSyncer interface {
		NodeOnline(nid string) exceptions.Exception
		NodeOffline(nid string) exceptions.Exception
	}
)

func newNetProcessor(nss nodeStateSyncer) distributed.NetProcessor {
	return &netProcessor{
		m:   make(map[string]lang.SendChannel[*m.Msg]),
		nss: nss,
	}
}

func (n *netProcessor) Send(target []string, nextJmp string, msg *m.Msg) (unreachable lang.ReceiveChannel[string]) {
	wc, ok := n.getWriteChannel(nextJmp)
	if !ok {
		return mr.SliceChannel(target)
	}

	if trySend(wc, msg) {
		return nil
	} else {
		return mr.SliceChannel(target)
	}
}

func trySend(wc lang.SendChannel[*m.Msg], msg *m.Msg) bool {
	if wc == nil {
		return false
	}

	select {
	case wc.SCh() <- msg:
		return true
	case <-time.After(time.Minute):
		return false
	}
}

func (n *netProcessor) setWriteChannel(node string, wc lang.SendChannel[*m.Msg]) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.m[node] = wc
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

	if n.nss != nil {
		if state%1 == 0 {
			_ = n.nss.NodeOffline(id)
		} else {
			_ = n.nss.NodeOnline(id)
		}
	}

	n.NearSend(msg)
}

func (n *netProcessor) NearSend(msg *m.Msg) {
	n.lock.RLock()
	defer n.lock.RUnlock()

	for _, wc := range n.m {
		go trySend(wc, msg)
	}
}
