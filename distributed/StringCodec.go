package distributed

import "gitea.tursom.cn/tursom/kvs/kv"

var (
	StringMessageProcessor Codec[string] = kv.StringToByteCodec
)
