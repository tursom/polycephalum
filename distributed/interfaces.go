package distributed

import (
	"context"
	"fmt"

	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util"

	"github.com/tursom/polycephalum/proto/m"
)

type UpdateResult uint32

//goland:noinspection GoSnakeCaseUsage
const (
	UpdateResult_Failed UpdateResult = iota
	UpdateResult_Unchanged
	UpdateResult_Updated
	UpdateResult_Older
	UpdateResult_Newer
)

func (r UpdateResult) String() string {
	switch r {
	case UpdateResult_Failed:
		return "Failed"
	case UpdateResult_Unchanged:
		return "Unchanged"
	case UpdateResult_Updated:
		return "Updated"
	case UpdateResult_Older:
		return "Older"
	case UpdateResult_Newer:
		return "Newer"
	default:
		panic(exceptions.NewRuntimeException(fmt.Sprintf("unknown UpdateResult value: %d", r), nil))
	}
}

var (
	BroadcastCtx = util.NewContext()
	UidFilterKey = util.AllocateContextKey[map[string]struct{}](BroadcastCtx)
)

type (
	Codec[M any] interface {
		Encode(msg M) []byte
		Decode(data []byte) M
	}

	Broadcast[M any] interface {
		lang.Object

		Listen(channel *m.BroadcastChannel) exceptions.Exception
		CancelListen(channel *m.BroadcastChannel) exceptions.Exception
		Send(channel *m.BroadcastChannel, msg M, ctx util.ContextMap)

		Snap() []string
		Receive(channel *m.BroadcastChannel, msg M, ctx util.ContextMap)
		NodeOnline(nid string) exceptions.Exception
		NodeOffline(nid string) exceptions.Exception
		UpdateFilter(nid string, filter []byte, filterVersion uint32) (UpdateResult, exceptions.Exception)
		RemoteListen(nid string, channel []*m.BroadcastChannel, filterVersion uint32) (UpdateResult, exceptions.Exception)
		NodeFilter(nid string) (filter []byte, version uint32)
		NodeHash(nid string) (version uint32, hash uint32)
	}

	BroadcastProcessor[M any] interface {
		Codec[M]
		SendToLocal(channel *m.BroadcastChannel, msg M, ctx util.ContextMap)
		SendToRemote(id []string, channel *m.BroadcastChannel, msg []byte)
		SendToNear(msg *m.Msg)
	}

	Net interface {
		lang.Object

		LocalId() string
		Send(ctx context.Context, ids []string, msg *m.Msg) exceptions.Exception
		NearSend(msg *m.Msg)

		UpdateNodeState(from, id string, state, jmp uint32)
		Snap() []*Node
	}

	Node struct {
		Id, NextJump string
		State, Jmp   uint32
	}

	NetProcessor interface {
		lang.Object

		Send(ctx context.Context, target []string, nextJmp string, msg *m.Msg) (unreachable lang.ReceiveChannel[string])
		NearSend(msg *m.Msg)

		ChangeNodeState(id string, state uint32)
	}
)
