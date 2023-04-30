package broadcast

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"

	lru "github.com/hashicorp/golang-lru/v2"
	log "github.com/sirupsen/logrus"
	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util/bloom"
	"github.com/tursom/kvs/kv"

	"github.com/tursom/polycephalum/distributed"
	"github.com/tursom/polycephalum/proto/m"
)

type (
	codec struct {
		lang.BaseObject
	}

	store struct {
		cache       *lru.Cache[string, *node]
		store       kv.Store[string, *node]
		onlineNodes map[string]struct{}
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
		cache:       c,
		store:       codecStore,
		onlineNodes: make(map[string]struct{}),
	}
}

func (s *store) load(id string) (*node, exceptions.Exception) {
	if _, ok := s.onlineNodes[id]; !ok {
		return nil, nil
	}

	if node, ok := s.cache.Get(id); ok {
		return node, nil
	}

	return s.store.Get(id)
}

func (s *store) nodeOnline(id string) exceptions.Exception {
	if s.cache.Contains(id) {
		s.onlineNodes[id] = struct{}{}
		return nil
	}

	n, exception := s.store.Get(id)
	if exception != nil {
		return exception
	}

	s.cache.Add(id, n)
	s.onlineNodes[id] = struct{}{}

	return nil
}

func (s *store) nodeOffline(id string) {
	delete(s.onlineNodes, id)
}

func (s *store) updateFilter(id string, filter *bloom.Bloom, filterVersion uint32) (distributed.UpdateResult, exceptions.Exception) {
	s.onlineNodes[id] = struct{}{}

	n, ok := s.cache.Get(id)
	if !ok {
		var exception exceptions.Exception
		n, exception = s.store.Get(id)
		if exception != nil {
			return distributed.UpdateResult_Failed, exception
		}

		s.cache.Add(id, n)
	}

	if n.filterVersion > filterVersion {
		return distributed.UpdateResult_Newer, nil
	} else if n.filterVersion > filterVersion {
		n.filter = filter
		return distributed.UpdateResult_Older, nil
	} else if n.filter.Equals(filter) {
		return distributed.UpdateResult_Unchanged, nil
	} else {
		n.filter.Merge(filter)
		return distributed.UpdateResult_Updated, nil
	}
}

func (s *store) remoteListen(id string, channel []m.Channel, filterVersion uint32) (distributed.UpdateResult, exceptions.Exception) {
	n, ok := s.cache.Get(id)
	if !ok {
		var exception exceptions.Exception
		n, exception = s.store.Get(id)
		if exception != nil {
			return distributed.UpdateResult_Failed, exception
		}
	}

	if n.filterVersion > filterVersion {
		return distributed.UpdateResult_Newer, nil
	} else if n.filterVersion < filterVersion {
		return distributed.UpdateResult_Older, nil
	}

	if n.filter == nil {
		n.filter = bloom.NewBloom(10_000, 0.1)
	}

	result := distributed.UpdateResult_Unchanged

	for _, c := range channel {
		bs := c.Marshal()
		if result == distributed.UpdateResult_Unchanged && !n.filter.Contains(bs) {
			result = distributed.UpdateResult_Updated
		}

		n.filter.Add(bs)
	}

	return result, nil
}

func (s *store) nodes(channel m.Channel) []string {
	nodes := make([]string, 0)

	for id := range s.onlineNodes {
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

	_ = binary.Write(writer, binary.BigEndian, v2.filterVersion)
	v2.filter.Marshal(writer)
	if err := writer.Close(); err != nil {
		panic(exceptions.Package(err))
	}

	return buffer.Bytes()
}

func (c *codec) Decode(v1 []byte) *node {
	if len(v1) == 0 {
		return &node{
			filter: bloom.NewBloom(1024, 0.03),
		}
	}

	reader, err := gzip.NewReader(bytes.NewReader(v1))

	var filterVersion uint32
	if err := binary.Read(reader, binary.BigEndian, &filterVersion); err != nil {
		panic(exceptions.Package(err))
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		panic(exceptions.Package(err))
	}

	return &node{
		filter:        bloom.Unmarshal(data),
		filterVersion: filterVersion,
	}
}
