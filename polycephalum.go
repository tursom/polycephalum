package polycephalum

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"math/rand"
	"time"

	"gitea.tursom.cn/tursom/kvs/kv"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util"

	"github.com/tursom/polycephalum/distributed"
	"github.com/tursom/polycephalum/distributed/broadcast"
	"github.com/tursom/polycephalum/distributed/net"
	"github.com/tursom/polycephalum/proto/m"
)

var (
	Ctx             = util.NewContext()
	NodeIdKey       = util.AllocateContextKey[string](Ctx)
	WriteChannelKey = util.AllocateContextKey[lang.SendChannel[*m.Msg]](Ctx)
	NodeSeedKey     = util.AllocateContextKey[[]byte](Ctx)
)

type (
	Polycephalum[M any] interface {
		NewConn(reader io.Reader, writer io.Writer)
		Broadcast(channelType uint32, channel string, msg M, ctx util.ContextMap)
	}

	impl[M any] struct {
		id string

		netProcessor *netProcessor

		message   distributed.MessageProcessor[M]
		net       distributed.Net
		broadcast distributed.Broadcast[M]

		privateKey []byte
	}

	Message struct {
		from string
		msg  []byte
	}
)

func New[M any](
	id string,
	messageProcessor distributed.MessageProcessor[M],
	kvs kv.Store[string, []byte],
	receiver func(channelType uint32, channel string, msg M, ctx util.ContextMap),
) Polycephalum[M] {
	var netProcessor netProcessor

	netInstance := net.New(id, &netProcessor, kv.KCodecStore(kvs, kv.PrefixCodec("net-")))
	bp := distributed.NetBroadcastProcessor(netInstance, messageProcessor, receiver)

	return &impl[M]{
		id:           id,
		netProcessor: &netProcessor,
		message:      messageProcessor,
		net:          netInstance,
		broadcast:    broadcast.New[M](bp, kv.KCodecStore(kvs, kv.PrefixCodec("broadcast-"))),
	}
}

func (p *impl[M]) Broadcast(channelType uint32, channel string, msg M, ctx util.ContextMap) {
	p.broadcast.Send(&m.BroadcastChannel{
		Type:    channelType,
		Channel: channel,
	}, msg, ctx)
}

func (p *impl[M]) NewConn(reader io.Reader, writer io.Writer) {
	p.newConn(reader, writer)
}

func (p *impl[M]) newConn(reader io.Reader, writer io.Writer) {
	writeCh := make(lang.RawChannel[*m.Msg])

	seed := p.newSeed()

	ctx := Ctx.NewMap()
	NodeSeedKey.Set(ctx, seed)
	WriteChannelKey.Set(ctx, writeCh)

	closer := p.closer(ctx, reader, writer)

	go doWrite(closer, writeCh, writer)
	go p.doRead(reader, closer, ctx)

	writeCh.Send(m.Build(func(msg *m.Msg) {
		msg.BuildHandshake(func(handshake *m.Handshake) {
			handshake.Seed = seed
		})
	}))
}

func (p *impl[M]) newSeed() []byte {
	seedBuffer := bytes.NewBuffer(nil)

	seedBuffer.WriteString(p.id)
	_ = binary.Write(seedBuffer, binary.BigEndian, time.Now().UnixMilli())
	for i := 0; i < 16; i++ {
		seedBuffer.WriteRune(rand.Int31())
	}

	sum256 := sha256.Sum256(seedBuffer.Bytes())
	return sum256[:]
}

func doWrite(closer func(), writeCh lang.RawChannel[*m.Msg], writer io.Writer) {
	defer closer()

	for msg := range writeCh.RCh() {
		data, err := proto.Marshal(msg)
		if err != nil {
			continue
		}

		if err := binary.Write(writer, binary.BigEndian, int32(len(data))); err != nil {
			// TODO log
			return
		}

		if _, err := writer.Write(data); err != nil {
			// TODO log
			return
		}
	}
}

func (p *impl[M]) doRead(reader io.Reader, closer func(), ctx util.ContextMap) {
	defer closer()

	var in [][]byte

	buffer := make([]byte, 1024)

	for {
		n, err := reader.Read(buffer)
		if err != nil {
			// TODO
			return
		} else if n == 0 {
			continue
		}

		in = append(in, buffer[n:])
		if len(buffer)-n < 512 {
			buffer = make([]byte, 1024)
		} else {
			buffer = buffer[n:]
		}

		processRead(&in, p.messageHandler(ctx))
	}
}

func processRead(in *[][]byte, out func([]byte)) {
	for {
		size := size(*in)
		if size < 4 {
			return
		}

		msgSize := int32(readUint32(*in))
		if msgSize < 0 {
			panic(exceptions.NewIllegalAccessException(
				"Msg size is negative! An error may occurred when transmission", nil))
		} else if msgSize == 0 || msgSize+4 < size {
			return
		}

		*in = skip(*in, 4)
		data := read(in, int(msgSize))
		out(data)
	}
}

func skip(in [][]byte, size int) [][]byte {
	for i, bs := range in {
		size -= len(bs)
		if size == 0 {
			return in[i+1:]
		} else if size < 0 {
			return in[i:]
		}
	}

	return in
}

func read(in *[][]byte, size int) []byte {
	data := make([]byte, size)
	id := 0

	for i, bs := range *in {
		copied := copy(data[id:], bs)
		id += copied
		if id == size {
			if copied == len(bs) {
				*in = (*in)[i+1:]
			} else {
				*in = (*in)[i:]
			}

			return data
		}
	}

	panic(exceptions.NewIllegalAccessException(
		"not enough data to read", nil))
}

func readUint32(in [][]byte) uint32 {
	buffer := make([]byte, 4)
	i := 0
	for _, bs := range in {
		for _, b := range bs {
			buffer[i] = b
			i++
			if i == 4 {
				return binary.BigEndian.Uint32(buffer)
			}
		}
	}

	return 0
}

func size(in [][]byte) int32 {
	size := 0
	for _, data := range in {
		size += len(data)
	}
	return int32(size)
}

// closer 将 reader 和 writer 的关闭方法抽象统一
// 返回值 closer 是可重入的，可以任意次调用
func (p *impl[M]) closer(ctx util.ContextMap, reader io.Reader, writer io.Writer) (closer func()) {
	closeSignal := make(lang.RawChannel[struct{}], 1)

	go func() {
		<-closeSignal

		if closer, ok := reader.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				// TODO
			}
		}

		if closer, ok := writer.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				// TODO
			}
		}

		// TODO remove broadcast listen
		nodeId := NodeIdKey.Get(ctx)
		if len(nodeId) == 0 {
			return
		}

		_ = p.broadcast.SuspectNode(nodeId)
	}()

	return func() {
		closeSignal.TrySend(struct{}{})
	}
}

func (p *impl[M]) messageHandler(ctx util.ContextMap) func([]byte) {
	return func(msgBytes []byte) {
		p.handleMessage(ctx, msgBytes)
	}
}

func (p *impl[M]) handleMessage(ctx util.ContextMap, msgBytes []byte) {
	var msg m.Msg
	if err := proto.Unmarshal(msgBytes, &msg); err != nil {
		log.WithField("err", err).
			WithField("msg", msgBytes).
			Errorf("failed to unmarshal message")
		return
	}

	switch msg.Content.(type) {
	case *m.Msg_Handshake:
		p.handleHandshakeMsg(ctx, &msg)
	case *m.Msg_HandshakeResponse:
		p.handleHandshakeResponse(ctx, &msg)
	case *m.Msg_SyncNodeStateRequest:
		p.handleSyncNodeStateRequest(ctx, &msg)
	case *m.Msg_BroadcastMsg:
		p.handleBroadcastRequest(ctx, &msg)
	case *m.Msg_ListenBroadcastBloom:
		// TODO
	case *m.Msg_AddBroadcastListen:
		// TODO
	case *m.Msg_SyncBroadcastMsg:
		// TODO
	default:
		// TODO
	}
}
