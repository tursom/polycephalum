package distributed

import (
	"context"

	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util"
	"github.com/tursom/GoCollections/util/bloom"

	"github.com/tursom/polycephalum/proto/msys"
)

type (
	Broadcast[M any] interface {
		lang.Object

		Listen(channel *msys.BroadcastChannel) exceptions.Exception
		CancelListen(channel *msys.BroadcastChannel) exceptions.Exception
		Send(channel *msys.BroadcastChannel, msg M, cm util.ContextMap)

		AddNode(nid string) exceptions.Exception
		SuspectNode(nid string) exceptions.Exception
		UpdateFilter(nid string, filter *bloom.Bloom) exceptions.Exception
		RemoteListen(nid string, channel *msys.BroadcastChannel) exceptions.Exception
	}

	BroadcastProcessor[M any] interface {
		Encode(msg M) []byte
		SendToLocal(channel *msys.BroadcastChannel, msg M, cm util.ContextMap)
		SendToRemote(id []string, channel *msys.BroadcastChannel, msg []byte)
	}

	Net[M any] interface {
		lang.Object

		Send(ctx context.Context, ids []string, msg []byte, jmp uint32) exceptions.Exception

		UpdateNodeState(from, id string, state, jmp uint32)
	}

	NetProcessor[M any] interface {
		lang.Object

		Receive(msg M)

		Send(
			ctx context.Context,
			target []string,
			nextJmp string,
			msg []byte,
			jmp uint32,
		) (unreachable []string)

		Suspect(id string)
	}
)

var (
	BroadcastCtx = util.NewContext()
	UidFilterKey = util.AllocateContextKey[map[string]struct{}](BroadcastCtx)
)
