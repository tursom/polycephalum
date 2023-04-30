package distributed

import (
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util"

	"github.com/tursom/polycephalum/proto/m"
)

type (
	Receiver[M any] func(channel m.Channel, msg M, ctx util.ContextMap)

	broadcastProcessorImpl[M any] struct {
		lang.BaseObject
		Codec[M]
		net      Net
		receiver Receiver[M]
	}
)

func NetBroadcastProcessor[M any](
	net Net,
	message Codec[M],
	receiver Receiver[M],
) BroadcastProcessor[M] {
	if net == nil {
		panic("nil net")
	}

	if message == nil {
		panic("nil msg codec")
	}

	if receiver == nil {
		panic("nil msg receiver")
	}

	return &broadcastProcessorImpl[M]{
		Codec:    message,
		net:      net,
		receiver: receiver,
	}
}

func (b *broadcastProcessorImpl[M]) SetReceiver(receiver Receiver[M]) {
	if receiver == nil {
		panic("nil msg receiver")
	}

	b.receiver = receiver
}

func (b *broadcastProcessorImpl[M]) SendToLocal(channel m.Channel, msg M, ctx util.ContextMap) {
	b.receiver(channel, msg, ctx)
}

func (b *broadcastProcessorImpl[M]) SendToRemote(id []string, channel m.Channel, msg []byte) {
	_ = b.net.Send(nil, id, m.Build(func(mm *m.Msg) {
		mm.BuildBroadcastMsg(func(broadcastMsg *m.BroadcastMsg) {
			broadcastMsg.Source = b.net.LocalId()
			broadcastMsg.Target = id
			broadcastMsg.Channel = channel
			broadcastMsg.Message = msg
		})
	}))
}

func (b *broadcastProcessorImpl[M]) SendToNear(msg *m.Msg) {
	b.net.NearSend(msg)
}
