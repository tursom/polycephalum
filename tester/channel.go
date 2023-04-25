package tester

import (
	"strconv"

	"github.com/tursom/polycephalum/proto/m"
)

func UserChannel(uid string) *m.BroadcastChannel {
	return &m.BroadcastChannel{
		Type:    uint32(1),
		Channel: uid,
	}
}

func GroupChannel(channel int32) *m.BroadcastChannel {
	return &m.BroadcastChannel{
		Type:    uint32(2),
		Channel: strconv.Itoa(int(channel)),
	}
}
