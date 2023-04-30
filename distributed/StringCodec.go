package distributed

import "github.com/tursom/kvs/kv"

var (
	StringMessageProcessor Codec[string] = kv.StringToByteCodec
)
