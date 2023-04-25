package polycephalum

import (
	"github.com/ethereum/go-ethereum/crypto/secp256k1"
	log "github.com/sirupsen/logrus"
	"github.com/tursom/GoCollections/util"

	"github.com/tursom/polycephalum/distributed"
	"github.com/tursom/polycephalum/proto/m"
)

func (p *impl[M]) handleHandshakeMsg(ctx util.ContextMap, msg *m.Msg) {
	wc := WriteChannelKey.Get(ctx)
	if wc == nil {
		return
	}

	handshake := msg.GetHandshake()

	sign, err := secp256k1.Sign(handshake.Seed, p.privateKey)
	if err != nil {
		// TODO log
		return
	}

	go trySend(wc, m.Build(func(msg *m.Msg) {
		msg.BuildHandshakeResponse(func(handshakeResponse *m.HandshakeResponse) {
			handshakeResponse.Id = p.id
			handshakeResponse.Sign = sign
		})
	}))
}

func (p *impl[M]) handleHandshakeResponse(ctx util.ContextMap, msg *m.Msg) {
	response := msg.GetHandshakeResponse()

	seed := NodeSeedKey.Get(ctx)

	if !secp256k1.VerifySignature(p.privateKey, seed, response.Sign) {
		// TODO log
		return
	}

	NodeIdKey.Set(ctx, response.GetId())

	wc := WriteChannelKey.Get(ctx)
	p.netProcessor.setWriteChannel(response.GetId(), wc)

	_ = p.broadcast.NodeOnline(response.GetId())

	p.startSyncer(wc)
}

func (p *impl[M]) handleSyncNodeStateRequest(ctx util.ContextMap, msg *m.Msg) {
	from := NodeIdKey.Get(ctx)
	if len(from) == 0 {
		log.WithField("ctx", ctx).
			Errorf("failed to get remote node id")
		return
	}

	request := msg.GetSyncNodeStateRequest()
	for _, state := range request.GetStates() {
		if state.GetId() != p.id {
			go p.net.UpdateNodeState(from, state.GetId(), state.GetState(), state.GetJump()+1)
			continue
		}

		if state.GetState() == p.state {
			continue
		}

		p.beenSuspect(state)
	}
}

func (p *impl[M]) beenSuspect(state *m.NodeState) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state.GetState() | 1
	_ = p.u32KvsPut("state", p.state)

	p.netProcessor.NearSend(m.Build(func(msg *m.Msg) {
		msg.BuildSyncNodeStateRequest(func(syncNodeStateRequest *m.SyncNodeStateRequest) {
			syncNodeStateRequest.States = []*m.NodeState{{
				Id:    p.id,
				State: p.state,
				Jump:  0,
			}}
		})
	}))
}

func (p *impl[M]) handleBroadcastRequest(ctx util.ContextMap, msg *m.Msg) {
	request := msg.GetBroadcastMsg()

	var remotes []string
	for _, target := range request.GetTarget() {
		if target == p.id {
			p.broadcast.Receive(request.Channel, p.message.Decode(request.Message), nil)
		} else {
			remotes = append(remotes, target)
		}
	}

	if len(remotes) == 0 {
		return
	}

	request.Target = remotes
	msg.Jump++

	_ = p.net.Send(nil, remotes, msg)
}

func (p *impl[M]) handleListenBroadcastBloom(ctx util.ContextMap, msg *m.Msg) {
	filter := msg.GetListenBroadcastBloom()

	if filter.Node == p.id {
		return
	}

	res, e := p.broadcast.UpdateFilter(filter.Node, filter.Bloom, filter.Version)
	if e != nil {
		// TODO log
		return
	}

	switch res {
	case distributed.UpdateResult_Failed:
		// NO-OP
	case distributed.UpdateResult_Unchanged:
		// NO-OP
	case distributed.UpdateResult_Updated:
		p.netProcessor.NearSend(msg)
	case distributed.UpdateResult_Older:
		p.netProcessor.NearSend(msg)
	case distributed.UpdateResult_Newer:
		p.syncListenBroadcastBloom(ctx, filter.Node, false)
	}
}

func (p *impl[M]) handleAddBroadcastListen(ctx util.ContextMap, msg *m.Msg) {
	listen := msg.GetAddBroadcastListen()

	if listen.Node == p.id {
		return
	}

	res, e := p.broadcast.RemoteListen(listen.Node, listen.Channel, listen.Version)
	if e != nil {
		// TODO log
		return
	}

	switch res {
	case distributed.UpdateResult_Failed:
		// NO-OP
	case distributed.UpdateResult_Unchanged:
		// NO-OP
	case distributed.UpdateResult_Updated:
		p.netProcessor.NearSend(msg)
	case distributed.UpdateResult_Older:
		// NO-OP
	case distributed.UpdateResult_Newer:
		p.syncListenBroadcastBloom(ctx, listen.Node, true)
	}
}

func (p *impl[M]) syncListenBroadcastBloom(ctx util.ContextMap, node string, versionOnly bool) {
	wc := WriteChannelKey.Get(ctx)
	if wc == nil {
		return
	}

	bloom, version := p.broadcast.NodeFilter(node)
	if versionOnly && (len(bloom) == 0 || version == 0) {
		return
	}

	go trySend(wc, m.Build(func(msg *m.Msg) {
		msg.BuildListenBroadcastBloom(func(listenBroadcastBloom *m.ListenBroadcastBloom) {
			listenBroadcastBloom.Node = node
			listenBroadcastBloom.Version = version
			if !versionOnly {
				listenBroadcastBloom.Bloom = bloom
			}
		})
	}))
}

func (p *impl[M]) handleSyncBroadcastMsg(ctx util.ContextMap, msg *m.Msg) {
	for _, snapshot := range msg.GetSyncBroadcastMsg().GetSnapshots() {
		if snapshot.Node == p.id {
			// TODO
			continue
		}

		version, hash := p.broadcast.NodeHash(snapshot.Node)
		if version == snapshot.Version && hash == snapshot.Hash {
			continue
		}

		p.syncListenBroadcastBloom(ctx, snapshot.Node, version >= snapshot.Version)
	}
}
