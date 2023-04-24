package polycephalum

import (
	log "github.com/sirupsen/logrus"
	"github.com/tursom/GoCollections/util"

	"github.com/ethereum/go-ethereum/crypto/secp256k1"

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
		// TODO
		return
	}

	wc.Send(m.Build(func(msg *m.Msg) {
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
		// TODO
		return
	}

	NodeIdKey.Set(ctx, response.GetId())

	_ = p.broadcast.AddNode(response.GetId())
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
		go p.net.UpdateNodeState(from, state.GetId(), state.GetState(), state.GetJump()+1)
	}
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
