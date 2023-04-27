package net

import (
	"context"
	"fmt"
	"sync"

	"gitea.tursom.cn/tursom/kvs/kv"
	lru "github.com/hashicorp/golang-lru/v2"
	log "github.com/sirupsen/logrus"
	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util/mr"
	"github.com/tursom/GoCollections/util/time"

	"github.com/tursom/polycephalum/distributed"
	"github.com/tursom/polycephalum/proto/m"
)

type (
	netImpl struct {
		lang.BaseObject

		localId   string
		processor distributed.NetProcessor

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

func New(localId string, processor distributed.NetProcessor, kvs kv.Store[string, []byte]) distributed.Net {
	locks := make(map[string]*nodeLock)

	store := kv.VCodecStore(kvs, nodeCodec(locks))

	cache, err := lru.NewWithEvict[string, *node](1024, func(key string, value *node) {
		_ = store.Put(key, value)
	})
	if err != nil {
		panic(exceptions.Package(err))
	}

	return &netImpl{
		localId:   localId,
		processor: processor,
		cache:     cache,
		store:     store,
		locks:     locks,
	}
}

func (n *netImpl) LocalId() string {
	return n.localId
}

func (n *netImpl) Snap() []*distributed.Node {
	n.lock.RLock()
	defer n.lock.RUnlock()

	var nodes []*distributed.Node

	for id := range n.locks {
		node, e := n.getNode(id)
		if e != nil {
			// TODO log
			continue
		} else if node == nil {
			continue
		}

		nodes = append(nodes, node.trans())
	}

	return nodes
}

func (n *netImpl) NearSend(msg *m.Msg) {
	n.processor.NearSend(msg)
}

func (n *netImpl) Send(ctx context.Context, ids []string, msg *m.Msg) exceptions.Exception {
	targetMap := make(map[string][]string)
	mayOffline := make(map[string]struct{})

	if e := n.mapTargets(ids, mayOffline, targetMap); e != nil {
		return e
	}

	if len(targetMap) != 0 {
		n.doSend(ctx, targetMap, msg, mayOffline)
	}

	if len(mayOffline) == 0 {
		return nil
	}

	for id := range mayOffline {
		id := id
		go func() {
			n.retrySend(ctx, id, msg, 1)
		}()
	}

	return nil
}

func (n *netImpl) mapTargets(
	ids []string,
	mayOffline map[string]struct{},
	targetMap map[string][]string,
) exceptions.Exception {
	for _, id := range ids {
		node, e := n.getNode(id)
		if e != nil {
			return e
		} else if node == nil {
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

func (n *netImpl) doSend(
	ctx context.Context,
	targetMap map[string][]string,
	msg *m.Msg,
	mayOffline map[string]struct{},
) {
	ch := make(chan *sendOp)
	go func() {
		for nextJmp, target := range targetMap {
			ch <- &sendOp{nextJmp, target}
		}

		close(ch)
	}()

	for unreachable := range mr.MultiMap(ch, func(value *sendOp) lang.ReceiveChannel[string] {
		return n.processor.Send(value.target, value.nextJmp, msg)
	}) {
		if unreachable == nil {
			continue
		}
		unreachable := unreachable
		go func() {
			for id := range unreachable.RCh() {
				n.suspect(id, mayOffline)
			}
		}()
	}
}

func (n *netImpl) retrySend(
	ctx context.Context,
	id string,
	msg *m.Msg,
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

	unreachable := n.processor.Send([]string{id}, node.nextJmp, msg)
	if unreachable == nil {
		return
	}

	select {
	case _, ok := <-unreachable.RCh():
		if !ok {
			// channel closed
			return
		}
	case <-time.After(time.Minute):
		// after timeout, the default handler is to ignore
		return
	}

	node.suspect(n.processor.ChangeNodeState)

	n.retrySend(ctx, id, msg, retryTimes+1)
}

func (n *netImpl) suspect(id string, mayOffline map[string]struct{}) {
	node := n.tryGetNode(id)
	if node == nil {
		return
	}

	mayOffline[id] = struct{}{}
	node.suspect(n.processor.ChangeNodeState)
}

func (n *netImpl) UpdateNodeState(from, id string, state, jmp uint32) {
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

		n.cache.Add(id, target)
	}

	target.stateChanged(state, from, jmp, n.processor)
}

func (n *netImpl) tryGetNode(id string) *node {
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

func (n *netImpl) getNode(id string) (*node, exceptions.Exception) {
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

func (n *node) stateChanged(state uint32, nextJmp string, distance uint32, p distributed.NetProcessor) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if state < n.state {
		n.sync(state, distance, func(msg *m.Msg) {
			p.Send([]string{nextJmp}, nextJmp, msg)
		})
		return
	}

	if state == n.state && len(n.nextJmp) != 0 && n.distance <= distance {
		return
	}

	defer n.sync(state, distance, p.NearSend)

	state0 := n.state
	n.state = state
	n.nextJmp = nextJmp
	n.distance = distance

	if isOnline(state) {
		if n.lock.waitChannel != nil {
			close(n.lock.waitChannel)
		}

		return
	}

	if isOnline(state0) {
		n.suspectTime = time.Now()
		n.lock.waitChannel = make(chan struct{})
	}
}

func (n *node) sync(state uint32, distance uint32, nearSender func(msg *m.Msg)) {
	var msg m.Msg
	msg.Content = &m.Msg_SyncNodeStateRequest{
		SyncNodeStateRequest: &m.SyncNodeStateRequest{
			States: []*m.NodeState{{
				Id:    n.id,
				State: state,
				Jump:  distance,
			}},
		},
	}

	nearSender(&msg)
}

func (n *node) suspect(suspect func(id string, state uint32)) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if !n.isOnlineLF() {
		return
	}

	n.state += 1
	n.suspectTime = time.Now()
	n.lock.waitChannel = make(chan struct{})

	go suspect(n.id, n.state)
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
	case <-time.After(timeout):
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

func (n *node) trans() *distributed.Node {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return &distributed.Node{
		Id:       n.id,
		NextJump: n.nextJmp,
		State:    n.state,
		Jmp:      n.distance,
	}
}
