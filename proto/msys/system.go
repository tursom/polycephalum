package msys

import (
	"bytes"
	"encoding/binary"
	"strconv"
)

func UserChannel(uid string) *BroadcastChannel {
	return &BroadcastChannel{
		Type:    BroadcastChannel_USER,
		Channel: uid,
	}
}

func GroupChannel(channel int32) *BroadcastChannel {
	return &BroadcastChannel{
		Type:    BroadcastChannel_GROUP,
		Channel: strconv.Itoa(int(channel)),
	}
}

func (c *BroadcastChannel) Marshal() []byte {
	buffer := bytes.NewBuffer(nil)

	_ = binary.Write(buffer, binary.BigEndian, int32(c.Type))
	buffer.Write([]byte(c.Channel))

	return buffer.Bytes()
}
