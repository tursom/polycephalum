package net

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitea.tursom.cn/tursom/kvs/kv"
	lru "github.com/hashicorp/golang-lru/v2"
	log "github.com/sirupsen/logrus"
	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util/mr"

	"github.com/tursom/polycephalum/distributed"
)

type (
	netImpl[M any] struct {
		lang.BaseObject

		processor distributed.NetProcessor[M]

		lock sync.RWMutex

		cache *lru.Cache[string, *node]
		store kv.Store[string, *node]
		locks map[string]*nodeLock
	}

	node struct {
		lang.BaseObject

		lock *nodeLock

		id string

		state       uint32
		suspectTime time.Time
		nextJmp     string
		distance    uint32
	}

	nodeLock struct {
		sync.RWMutex
		waitChannel chan struct{}
	}

	sendOp struct {
		nextJmp string
		target  []string
	}
)

func New[M any](kvs kv.Store[string, []byte]) distributed.Net[M] {
	locks := make(map[string]*nodeLock)

	store := kv.VCodecStore(kvs, nodeCodec(locks))

	cache, err := lru.NewWithEvict[string, *node](1024, func(key string, value *node) {
		_ = store.Put(key, value)
	})
	if err != nil {
		panic(exceptions.Package(err))
	}

	return &netImpl[M]{
		cache: cache,
		store: store,
		locks: locks,
	}
}

func (n *netImpl[M]) Send(ctx context.Context, ids []string, msg []byte, jmp uint32) exceptions.Exception {
	targetMap := make(map[string][]string)
	mayOffline := make(map[string]struct{})

	if e := n.mapTargets(ids, mayOffline, targetMap); e != nil {
		return e
	}

	if len(targetMap) != 0 {
		n.doSend(ctx, targetMap, msg, jmp, mayOffline)
	}

	if len(mayOffline) == 0 {
		return nil
	}

	for id := range mayOffline {
		id := id
		go func() {
			n.retrySend(ctx, id, msg, jmp, 1)
		}()
	}

	return nil
}

func (n *netImpl[M]) mapTargets(
	ids []string,
	mayOffline map[string]struct{},
	targetMap map[string][]string,
) exceptions.Exception {
	for _, id := range ids {
		node, e := n.getNode(id)
		if e != nil {
			return e
		} else if n == nil {
			continue
		}

		if !node.isOnline() {
			if node.timeout() {
				continue
			}

			mayOffline[id] = struct{}{}
			continue
		}

		targetMap[node.nextJmp] = append(targetMap[node.nextJmp], id)
	}

	return nil
}

func (n *netImpl[M]) doSend(
	ctx context.Context,
	targetMap map[string][]string,
	msg []byte,
	jmp uint32,
	mayOffline map[string]struct{},
) {
	ch := make(chan *sendOp)
	go func() {
		for nextJmp, target := range targetMap {
			ch <- &sendOp{nextJmp, target}
		}

		close(ch)
	}()

	for unreachable := range mr.MultiMap(ch, func(value *sendOp) []string {
		return n.processor.Send(ctx, value.target, value.nextJmp, msg, jmp+1)
	}) {
		for _, id := range unreachable {
			n.suspect(id, mayOffline)
		}
	}
}

func (n *netImpl[M]) retrySend(
	ctx context.Context,
	id string,
	msg []byte,
	jmp uint32,
	retryTimes uint32,
) {
	if retryTimes > 3 {
		return
	}

	node := n.tryGetNode(id)
	if node == nil {
		return
	}

	node.waitSuspect()

	if !node.isOnline() {
		return
	}

	if unreachable := n.processor.Send(ctx, []string{id}, node.nextJmp, msg, jmp+1); len(unreachable) == 0 {
		return
	}

	node.suspect(n.processor.Suspect)

	n.retrySend(ctx, id, msg, jmp, retryTimes+1)
}

func (n *netImpl[M]) suspect(id string, mayOffline map[string]struct{}) {
	node := n.tryGetNode(id)
	if node == nil {
		return
	}

	mayOffline[id] = struct{}{}
	node.suspect(n.processor.Suspect)
}

func (n *netImpl[M]) UpdateNodeState(from, id string, state, jmp uint32) {
	n.lock.Lock()
	defer n.lock.Unlock()

	target, e := n.getNode(id)
	if e != nil {
		log.WithFields(log.Fields{
			"id":         id,
			"e":          e,
			"stackTrace": exceptions.GetStackTraceString(e),
		}).Errorf("failed to get node")

		return
	}

	if target == nil {
		lock, ok := n.locks[id]
		if !ok || lock == nil {
			lock = &nodeLock{}
			n.locks[id] = lock
		}

		target = &node{
			id:   id,
			lock: lock,
		}
	}

	target.stateChanged(state, from, jmp)
}

func (n *netImpl[M]) tryGetNode(id string) *node {
	node, e := n.getNode(id)
	if e != nil {
		log.WithFields(log.Fields{
			"id":         id,
			"e":          e,
			"stackTrace": exceptions.GetStackTraceString(e),
		}).Errorf("failed to get node")
		return nil
	}

	return node
}

func (n *netImpl[M]) getNode(id string) (*node, exceptions.Exception) {
	value, ok := n.cache.Get(id)
	if ok {
		return value, nil
	}

	get, exception := n.store.Get(id)
	if exception != nil {
		return nil, exception
	}

	if get != nil {
		n.cache.Add(id, get)
	}

	return get, nil
}

func (n *node) String() string {
	return fmt.Sprintf("node(state=%d, suspectTime=%s, nextJump=%s, distance=%d)",
		n.state, n.suspectTime.Format("060102-150405.000"), n.nextJmp, n.distance)
}

func (n *node) stateChanged(state uint32, nextJmp string, distance uint32) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if state <= n.state {
		return
	}

	state0 := n.state
	n.state = state
	n.nextJmp = nextJmp
	n.distance = distance

	if isOnline(state) {
		if n.lock.waitChannel != nil {
			close(n.lock.waitChannel)
		}
	} else if !isOnline(state0) {
		n.suspectTime = time.Now()
		n.lock.waitChannel = make(chan struct{})
	}
}

func (n *node) suspect(suspect func(id string)) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if !n.isOnlineLF() {
		return
	}

	n.state += 1
	n.suspectTime = time.Now()
	n.lock.waitChannel = make(chan struct{})

	go suspect(n.id)
}

func (n *node) waitSuspect() {
	timeout := time.Second*3 - time.Now().Sub(n.suspectTime)
	if timeout < 0 {
		return
	}

	wc := n.wc()
	if wc == nil {
		return
	}

	select {
	case <-wc:
	case <-time.NewTimer(timeout).C:
	}
}

func (n *node) timeout() bool {
	return n.wc() == nil
}

func (n *node) wc() <-chan struct{} {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return n.lock.waitChannel
}

func (n *node) isOnline() bool {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return n.isOnlineLF()
}

// isOnlineLF lock free version of isOnline
func (n *node) isOnlineLF() bool {
	return isOnline(n.state)
}

func isOnline(state uint32) bool {
	return state&1 == 1
}
