package broadcast

import (
	"bytes"
	"compress/gzip"
	"io"

	"gitea.tursom.cn/tursom/kvs/kv"
	lru "github.com/hashicorp/golang-lru/v2"
	log "github.com/sirupsen/logrus"
	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util/bloom"

	"github.com/tursom/tursom-im/proto/msys"
)

type (
	codec struct {
		lang.BaseObject
	}

	store struct {
		cache   *lru.Cache[string, *node]
		store   kv.Store[string, *node]
		nodeSet map[string]struct{}
	}
)

var (
	codecInstance kv.Codec[[]byte, *node] = &codec{}
)

func newStore(size int, persistence kv.Store[string, []byte]) *store {
	codecStore := kv.VCodecStore(persistence, codecInstance)

	c, _ := lru.NewWithEvict[string, *node](size, func(key string, value *node) {
		_ = codecStore.Put(key, value)
	})

	return &store{
		cache:   c,
		store:   codecStore,
		nodeSet: make(map[string]struct{}),
	}
}

func (s *store) load(id string) (*node, exceptions.Exception) {
	if _, ok := s.nodeSet[id]; !ok {
		return nil, nil
	}

	return s.store.Get(id)
}

func (s *store) addNode(id string) exceptions.Exception {
	if s.cache.Contains(id) {
		return nil
	}

	n, exception := s.store.Get(id)
	if exception != nil {
		return exception
	}

	s.cache.Add(id, n)
	s.nodeSet[id] = struct{}{}

	return nil
}

func (s *store) suspectNode(id string) {
	if _, ok := s.nodeSet[id]; !ok {
		return
	}

	delete(s.nodeSet, id)
}

func (s *store) updateFilter(id string, filter *bloom.Bloom) exceptions.Exception {
	n, ok := s.cache.Get(id)
	if !ok {
		var exception exceptions.Exception
		n, exception = s.store.Get(id)
		if exception != nil {
			return exception
		}

		s.cache.Add(id, n)
	}

	n.filter = filter

	return nil
}

func (s *store) remoteListen(id string, channel *msys.BroadcastChannel) exceptions.Exception {
	n, ok := s.cache.Get(id)
	if !ok {
		var exception exceptions.Exception
		n, exception = s.store.Get(id)
		if exception != nil {
			return exception
		}
	}

	if n.filter == nil {
		n.filter = bloom.NewBloom(10_000, 0.1)
	}

	n.filter.Add(channel.Marshal())

	return nil
}

func (s *store) nodes(channel *msys.BroadcastChannel) []string {
	nodes := make([]string, 16)

	for id := range s.nodeSet {
		n, ok := s.cache.Get(id)
		if !ok {
			fromStore, exception := s.store.Get(id)
			if exception != nil {
				log.WithField("exception", exception).
					Errorf("failed to get from store")
				continue
			}

			n = fromStore
		}

		if n != nil && n.filter != nil && n.filter.Contains(channel.Marshal()) {
			nodes = append(nodes, id)
		}
	}

	return nodes
}

func (c *codec) Encode(v2 *node) []byte {
	if v2.filter == nil {
		return []byte{}
	}

	buffer := bytes.NewBuffer(nil)
	writer := gzip.NewWriter(buffer)

	v2.filter.Marshal(writer)
	if err := writer.Close(); err != nil {
		panic(exceptions.Package(err))
	}

	return buffer.Bytes()
}

func (c *codec) Decode(v1 []byte) *node {
	if len(v1) == 0 {
		return &node{}
	}

	reader, err := gzip.NewReader(bytes.NewReader(v1))

	data, err := io.ReadAll(reader)
	if err != nil {
		panic(exceptions.Package(err))
	}

	return &node{
		filter: bloom.Unmarshal(data),
	}
}
