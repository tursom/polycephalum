package polycephalum

import (
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/tursom/GoCollections/lang"

	"github.com/tursom/polycephalum/distributed"
	"github.com/tursom/polycephalum/proto/msys"
)

type (
	Polycephalum interface {
	}

	impl struct {
	}

	Message struct {
		from string
		msg  []byte
	}
)

func New[M any](
	receiveChannel lang.ReceiveChannel[*Message],
	netProcessor distributed.NetProcessor[M],
	broadcastProcessor distributed.BroadcastProcessor[M],
) Polycephalum {
}

func processMessage(receiveChannel lang.ReceiveChannel[*Message]) {
	for ms := range receiveChannel.RCh() {
		msgBytes := ms.msg

		var msg msys.SystemMsg
		if err := proto.Unmarshal(msgBytes, &msg); err != nil {
			log.WithField("err", err).
				WithField("msg", msgBytes).
				Errorf("failed to unmarshal message")
			continue
		}

		switch msg.Content.(type) {
		case *msys.SystemMsg_FindNodeRequest:
		case *msys.SystemMsg_FindNodeResponse:
		case *msys.SystemMsg_ListenBroadcastBloom:
		case *msys.SystemMsg_AddBroadcastListen:
		case *msys.SystemMsg_BroadcastRequest:
		case *msys.SystemMsg_BroadcastResponse:
		}
	}
}
