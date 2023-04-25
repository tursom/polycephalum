package broadcast

import (
	"sync"

	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util"
	"github.com/tursom/GoCollections/util/bloom"

	"github.com/tursom/polycephalum/distributed"
	"github.com/tursom/polycephalum/proto/m"
)

type (
	local[M any] struct {
		lang.BaseObject
		localId string
		version uint32

		processor       distributed.BroadcastProcessor[M]
		channelGroupMap map[string]struct{}
		lock            sync.RWMutex
		filter          *bloom.Bloom

		deleted uint32
	}
)

func (c *local[M]) updateFilterLF() (*bloom.Bloom, uint32) {
	if c.deleted == 0 {
		return c.filter, 0
	}

	bn := uint(len(c.channelGroupMap))
	bp := 0.03

	if bn < 1024 {
		bn = 4 * 1024
	} else if bn < 1024*1024 {
		bn = 4 * 1024 * 1024
	} else {
		bn = 1024 * 1024 * 1024
		bp = 0.1
	}

	c.filter = bloom.NewBloom(bn, bp)
	for ch := range c.channelGroupMap {
		c.filter.Add([]byte(ch))
	}

	c.deleted = 0
	c.version++
	return c.filter, c.version
}

func (c *local[M]) Listen(channel *m.BroadcastChannel) exceptions.Exception {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.channelGroupMap[channel.MarshalString()] = struct{}{}

	c.filter.Add(channel.Marshal())

	if c.filter.C() > uint(len(c.channelGroupMap)) {
		c.releaseNewFilterVersion()
	} else {
		msg := m.Build(func(msg *m.Msg) {
			msg.BuildAddBroadcastListen(func(addBroadcastListen *m.AddBroadcastListen) {
				addBroadcastListen.Node = c.localId
				addBroadcastListen.Version = c.version
				addBroadcastListen.Channel = []*m.BroadcastChannel{channel}
			})
		})
		c.processor.SendToNear(msg)
	}

	return nil
}

func (c *local[M]) CancelListen(channel *m.BroadcastChannel) exceptions.Exception {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.deleted++
	delete(c.channelGroupMap, channel.MarshalString())

	if float64(len(c.channelGroupMap))/float64(c.deleted) > 0.1 {
		c.releaseNewFilterVersion()
	}

	return nil
}

func (c *local[M]) releaseNewFilterVersion() {
	filter, version := c.updateFilterLF()

	c.processor.SendToNear(m.Build(func(msg *m.Msg) {
		msg.BuildListenBroadcastBloom(func(listenBroadcastBloom *m.ListenBroadcastBloom) {
			listenBroadcastBloom.Node = c.localId
			listenBroadcastBloom.Version = version
			listenBroadcastBloom.Bloom = marshalFilter(filter)
		})
	}))
}

func (c *local[M]) Receive(channel *m.BroadcastChannel, msg M, ctx util.ContextMap) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if _, ok := c.channelGroupMap[channel.MarshalString()]; !ok {
		return
	}

	go c.processor.SendToLocal(channel, msg, ctx)
}
