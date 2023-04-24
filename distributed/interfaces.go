package distributed

import (
	"context"

	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util"
	"github.com/tursom/GoCollections/util/bloom"

	"github.com/tursom/polycephalum/proto/m"
)

var (
	BroadcastCtx = util.NewContext()
	UidFilterKey = util.AllocateContextKey[map[string]struct{}](BroadcastCtx)
)

type (
	MessageProcessor[M any] interface {
		Encode(msg M) []byte
		Decode(data []byte) M
	}

	Broadcast[M any] interface {
		lang.Object

		Listen(channel *m.BroadcastChannel) exceptions.Exception
		CancelListen(channel *m.BroadcastChannel) exceptions.Exception
		Send(channel *m.BroadcastChannel, msg M, ctx util.ContextMap)
		Receive(channel *m.BroadcastChannel, msg M, ctx util.ContextMap)

		AddNode(nid string) exceptions.Exception
		SuspectNode(nid string) exceptions.Exception
		UpdateFilter(nid string, filter *bloom.Bloom) exceptions.Exception
		RemoteListen(nid string, channel *m.BroadcastChannel) exceptions.Exception
	}

	BroadcastProcessor[M any] interface {
		MessageProcessor[M]
		SendToLocal(channel *m.BroadcastChannel, msg M, ctx util.ContextMap)
		SendToRemote(id []string, channel *m.BroadcastChannel, msg []byte)
	}

	Net interface {
		lang.Object

		LocalId() string

		Send(ctx context.Context, ids []string, msg *m.Msg) exceptions.Exception

		UpdateNodeState(from, id string, state, jmp uint32)
	}

	NetProcessor interface {
		lang.Object

		Send(ctx context.Context, target []string, nextJmp string, msg *m.Msg) (unreachable lang.ReceiveChannel[string])

		ChangeNodeState(id string, state uint32)
	}
)
