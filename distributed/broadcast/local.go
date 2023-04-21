package broadcast

import (
	"github.com/tursom/GoCollections/concurrent"
	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util"
	"github.com/tursom/GoCollections/util/bloom"

	"github.com/tursom/polycephalum/distributed"
	"github.com/tursom/polycephalum/proto/msys"
)

type (
	local[M any] struct {
		lang.BaseObject
		processor       distributed.BroadcastProcessor[M]
		channelGroupMap map[*msys.BroadcastChannel]struct{}
		mutex           concurrent.RWLock
		filter          *bloom.Bloom
	}
)

func (c *local[M]) Filter() *bloom.Bloom {
	//TODO implement me
	panic("implement me")
}

func (c *local[M]) Listen(channel *msys.BroadcastChannel) exceptions.Exception {
	//TODO implement me
	panic("implement me")
}

func (c *local[M]) CancelListen(channel *msys.BroadcastChannel) exceptions.Exception {
	//TODO implement me
	panic("implement me")
}

func (c *local[M]) Send(channel *msys.BroadcastChannel, msg M, cm util.ContextMap) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if _, ok := c.channelGroupMap[channel]; !ok {
		return
	}

	c.processor.SendToLocal(channel, msg, cm)
}

func (c *local[M]) isListen(channel *msys.BroadcastChannel) {

}
