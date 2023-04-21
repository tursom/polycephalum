package broadcast

import (
	"sync"
	"time"

	"gitea.tursom.cn/tursom/kvs/kv"
	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/util"
	"github.com/tursom/GoCollections/util/bloom"

	"github.com/tursom/polycephalum/distributed"
	"github.com/tursom/polycephalum/proto/msys"
)

type (
	node struct {
		filter      *bloom.Bloom
		suspectTime *time.Time
	}

	broadcast[M any] struct {
		local[M]
		mutex sync.RWMutex
		store *store
	}
)

func New[M any](
	processor distributed.BroadcastProcessor[M],
	persistence kv.Store[string, []byte],
) distributed.Broadcast[M] {
	return &broadcast[M]{
		store: newStore(1024, persistence),
		local: local[M]{
			processor: processor,
		},
	}
}

func (c *broadcast[M]) AddNode(id string) exceptions.Exception {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.store.addNode(id)
}

func (c *broadcast[M]) SuspectNode(id string) exceptions.Exception {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.store.suspectNode(id)
	return nil
}

func (c *broadcast[M]) UpdateFilter(id string, filter *bloom.Bloom) exceptions.Exception {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.store.updateFilter(id, filter)
}

func (c *broadcast[M]) RemoteListen(id string, channel *msys.BroadcastChannel) exceptions.Exception {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.store.remoteListen(id, channel)
}

func (c *broadcast[M]) Send(channel *msys.BroadcastChannel, msg M, cm util.ContextMap) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	go c.local.Send(channel, msg, cm)

	nodes := c.store.nodes(channel)
	go c.processor.SendToRemote(nodes, channel, msg)
}
