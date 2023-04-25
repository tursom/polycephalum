package distributed

type bytesMessageProcessor struct {
}

var (
	BytesMessageProcessor Codec[[]byte] = &bytesMessageProcessor{}
)

func (b *bytesMessageProcessor) Encode(msg []byte) []byte {
	return msg
}

func (b *bytesMessageProcessor) Decode(data []byte) []byte {
	return data
}
