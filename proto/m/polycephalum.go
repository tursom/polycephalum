package m

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"unsafe"
)

func Build(builder func(msg *Msg)) *Msg {
	var msg Msg
	builder(&msg)
	return &msg
}

func UserChannel(uid string) *BroadcastChannel {
	return &BroadcastChannel{
		Type:    uint32(1),
		Channel: uid,
	}
}

func GroupChannel(channel int32) *BroadcastChannel {
	return &BroadcastChannel{
		Type:    uint32(2),
		Channel: strconv.Itoa(int(channel)),
	}
}

func (c *BroadcastChannel) MarshalString() string {
	marshal := c.Marshal()
	return *(*string)(unsafe.Pointer(&marshal))
}

func (c *BroadcastChannel) Marshal() []byte {
	buffer := bytes.NewBuffer(nil)

	_ = binary.Write(buffer, binary.BigEndian, c.Type)
	buffer.Write([]byte(c.Channel))

	return buffer.Bytes()
}

func (m *Msg) SetHandshake(handshake *Handshake) *Msg {
	m.Content = &Msg_Handshake{Handshake: handshake}
	return m
}

func (m *Msg) SetHandshakeResponse(handshakeResponse *HandshakeResponse) *Msg {
	m.Content = &Msg_HandshakeResponse{HandshakeResponse: handshakeResponse}
	return m
}

func (m *Msg) SetSyncNodeStateRequest(syncNodeStateRequest *SyncNodeStateRequest) *Msg {
	m.Content = &Msg_SyncNodeStateRequest{SyncNodeStateRequest: syncNodeStateRequest}
	return m
}

func (m *Msg) SetBroadcastMsg(broadcastMsg *BroadcastMsg) *Msg {
	m.Content = &Msg_BroadcastMsg{BroadcastMsg: broadcastMsg}
	return m
}

func (m *Msg) SetListenBroadcastBloom(listenBroadcastBloom *ListenBroadcastBloom) *Msg {
	m.Content = &Msg_ListenBroadcastBloom{ListenBroadcastBloom: listenBroadcastBloom}
	return m
}

func (m *Msg) SetAddBroadcastListen(addBroadcastListen *AddBroadcastListen) *Msg {
	m.Content = &Msg_AddBroadcastListen{AddBroadcastListen: addBroadcastListen}
	return m
}

func (m *Msg) SetSyncBroadcastMsg(syncBroadcastMsg *SyncBroadcastMsg) *Msg {
	m.Content = &Msg_SyncBroadcastMsg{SyncBroadcastMsg: syncBroadcastMsg}
	return m
}

func (m *Msg) BuildHandshake(builder func(handshake *Handshake)) *Msg {
	var handshake Handshake
	builder(&handshake)
	m.Content = &Msg_Handshake{Handshake: &handshake}
	return m
}

func (m *Msg) BuildHandshakeResponse(builder func(handshakeResponse *HandshakeResponse)) *Msg {
	var handshakeResponse HandshakeResponse
	builder(&handshakeResponse)
	m.Content = &Msg_HandshakeResponse{HandshakeResponse: &handshakeResponse}
	return m
}

func (m *Msg) BuildSyncNodeStateRequest(builder func(syncNodeStateRequest *SyncNodeStateRequest)) *Msg {
	var syncNodeStateRequest SyncNodeStateRequest
	builder(&syncNodeStateRequest)
	m.Content = &Msg_SyncNodeStateRequest{SyncNodeStateRequest: &syncNodeStateRequest}
	return m
}

func (m *Msg) BuildBroadcastMsg(builder func(broadcastMsg *BroadcastMsg)) *Msg {
	var broadcastMsg BroadcastMsg
	builder(&broadcastMsg)
	m.Content = &Msg_BroadcastMsg{BroadcastMsg: &broadcastMsg}
	return m
}

func (m *Msg) BuildListenBroadcastBloom(builder func(listenBroadcastBloom *ListenBroadcastBloom)) *Msg {
	var listenBroadcastBloom ListenBroadcastBloom
	builder(&listenBroadcastBloom)
	m.Content = &Msg_ListenBroadcastBloom{ListenBroadcastBloom: &listenBroadcastBloom}
	return m
}

func (m *Msg) BuildAddBroadcastListen(builder func(addBroadcastListen *AddBroadcastListen)) *Msg {
	var addBroadcastListen AddBroadcastListen
	builder(&addBroadcastListen)
	m.Content = &Msg_AddBroadcastListen{AddBroadcastListen: &addBroadcastListen}
	return m
}

func (m *Msg) BuildSyncBroadcastMsg(builder func(syncBroadcastMsg *SyncBroadcastMsg)) *Msg {
	var syncBroadcastMsg SyncBroadcastMsg
	builder(&syncBroadcastMsg)
	m.Content = &Msg_SyncBroadcastMsg{SyncBroadcastMsg: &syncBroadcastMsg}
	return m
}
