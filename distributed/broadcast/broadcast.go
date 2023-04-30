package broadcast

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"

	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/util"
	"github.com/tursom/GoCollections/util/bloom"
	"github.com/tursom/GoCollections/util/time"
	"github.com/tursom/kvs/kv"

	"github.com/tursom/polycephalum/distributed"
	"github.com/tursom/polycephalum/proto/m"
)

type (
	node struct {
		filterVersion uint32
		filter        *bloom.Bloom
		suspectTime   *time.Time
	}

	broadcast[M any] struct {
		local[M]
		mutex sync.RWMutex
		store *store
	}
)

func New[M any](
	localId string,
	version uint32,
	processor distributed.BroadcastProcessor[M],
	persistence kv.Store[string, []byte],
) distributed.Broadcast[M] {
	if version == 0 {
		version = 1
	}

	return &broadcast[M]{
		store: newStore(1024, persistence),
		local: local[M]{
			localId:         localId,
			version:         version,
			processor:       processor,
			channelGroupMap: make(map[string]struct{}),
			filter:          bloom.NewBloom(1024, 0.03),
		},
	}
}

func (c *broadcast[M]) NodeOnline(id string) exceptions.Exception {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.store.nodeOnline(id)
}

func (c *broadcast[M]) NodeOffline(id string) exceptions.Exception {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.store.nodeOffline(id)
	return nil
}

func (c *broadcast[M]) UpdateFilter(nid string, filter []byte, filterVersion uint32) (distributed.UpdateResult, exceptions.Exception) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reader, err := gzip.NewReader(bytes.NewReader(filter))
	if err != nil {
		return distributed.UpdateResult_Failed, exceptions.Package(err)
	}

	bs, err := io.ReadAll(reader)
	if err != nil {
		return distributed.UpdateResult_Failed, exceptions.Package(err)
	}

	return c.store.updateFilter(nid, bloom.Unmarshal(bs), filterVersion)
}

func (c *broadcast[M]) RemoteListen(nid string, channel []m.Channel, filterVersion uint32) (distributed.UpdateResult, exceptions.Exception) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.store.remoteListen(nid, channel, filterVersion)
}

func (c *broadcast[M]) Send(channel m.Channel, msg M, ctx util.ContextMap) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	c.local.Receive(channel, msg, ctx)

	nodes := c.store.nodes(channel)
	go c.processor.SendToRemote(nodes, channel, c.processor.Encode(msg))
}

func (c *broadcast[M]) NodeFilter(nid string) ([]byte, uint32) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if nid == c.localId {
		c.lock.RLock()
		defer c.lock.RUnlock()

		return marshalFilter(c.filter), c.version
	}

	node, e := c.store.load(nid)
	if e != nil {
		return nil, 0
	} else if node == nil {
		return nil, 0
	}

	return marshalFilter(node.filter), node.filterVersion
}

func marshalFilter(filter *bloom.Bloom) []byte {
	buffer := bytes.NewBuffer(nil)
	writer := gzip.NewWriter(buffer)
	filter.Marshal(writer)
	_ = writer.Close()

	return buffer.Bytes()
}

func (c *broadcast[M]) NodeHash(nid string) (version uint32, hash uint32) {
	if nid == c.localId {
		c.local.lock.RLock()
		defer c.local.lock.RUnlock()

		return c.version, uint32(c.filter.HashCode())
	}

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	node, e := c.store.load(nid)
	if e != nil {
		return 0, 0
	} else if node == nil {
		return 0, 0
	}

	if node.filter == nil {
		return 0, 0
	}

	return node.filterVersion, uint32(node.filter.HashCode())
}

func (c *broadcast[M]) Snap() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	res := []string{c.localId}

	for id := range c.store.onlineNodes {
		res = append(res, id)
	}

	return res
}
