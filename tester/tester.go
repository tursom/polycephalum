package tester

import (
	"io"

	"github.com/tursom/GoCollections/lang"
	"github.com/tursom/GoCollections/util"
	"github.com/tursom/kvs/kv"

	"github.com/tursom/polycephalum"
	"github.com/tursom/polycephalum/proto/m"
)

type (
	testIO struct {
		ch      lang.Channel[[]byte]
		closeCh lang.Channel[struct{}]
		readBuf []byte
	}
)

func newTestPolycephalum(id string, receiver func(channel m.Channel, msg string, ctx util.ContextMap)) polycephalum.Polycephalum[string] {
	return polycephalum.New[string](
		id,
		kv.StringToByteCodec,
		kv.MapKvs(),
		publicKey,
		privateKey,
		receiver,
	)
}

func connect(p1, p2 polycephalum.Polycephalum[string]) {
	io1 := newTestIO()
	io2 := newTestIO()

	p1.NewConn(io1, io2)
	p2.NewConn(io2, io1)
}

func newTestIO() *testIO {
	return &testIO{
		ch:      make(lang.RawChannel[[]byte]),
		closeCh: make(lang.RawChannel[struct{}]),
		readBuf: nil,
	}
}

func (t *testIO) Write(p []byte) (n int, err error) {
	select {
	case t.ch.SCh() <- p:
		return len(p), nil
	case <-t.closeCh.RCh():
		return 0, io.EOF
	}

}

func (t *testIO) Read(p []byte) (n int, err error) {
	if len(t.readBuf) != 0 {
		copy(p, t.readBuf)
		if len(p) >= len(t.readBuf) {
			read := len(t.readBuf)
			t.readBuf = nil
			return read, nil
		} else {
			t.readBuf = t.readBuf[len(p):]
			return len(p), nil
		}
	}

	for {
		select {
		case buf := <-t.ch.RCh():
			t.readBuf = buf
			if len(t.readBuf) != 0 {
				return t.Read(p)
			}
		case <-t.closeCh.RCh():
			return 0, io.EOF
		}
	}
}

func (t *testIO) Close() error {
	t.closeCh.TrySend(struct{}{})
	return nil
}
