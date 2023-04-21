package net

import (
	"bytes"
	"encoding/binary"

	"gitea.tursom.cn/tursom/kvs/kv"
	"github.com/tursom/GoCollections/exceptions"
	"github.com/tursom/GoCollections/lang"
)

type (
	nodeCodec0 struct {
		lang.BaseObject
		locks map[string]*nodeLock
	}
)

func nodeCodec(locks map[string]*nodeLock) kv.Codec[[]byte, *node] {
	return &nodeCodec0{locks: locks}
}

func (c *nodeCodec0) Encode(v2 *node) []byte {
	buffer := bytes.NewBuffer(nil)

	{
		_ = binary.Write(buffer, binary.BigEndian, uint32(len(v2.id)))
		buffer.WriteString(v2.id)
	}

	{
		_ = binary.Write(buffer, binary.BigEndian, v2.state)
	}

	{
		stb, err := v2.suspectTime.MarshalBinary()
		if err != nil {
			panic(exceptions.Package(err))
		}
		_ = binary.Write(buffer, binary.BigEndian, uint32(len(stb)))
		buffer.Write(stb)
	}

	{
		_ = binary.Write(buffer, binary.BigEndian, uint32(len(v2.nextJmp)))
		buffer.WriteString(v2.nextJmp)
	}

	{
		_ = binary.Write(buffer, binary.BigEndian, v2.distance)
	}

	return buffer.Bytes()
}

func (c *nodeCodec0) Decode(v1 []byte) *node {
	buf := v1
	if len(buf) == 0 {
		return nil
	}

	n := &node{}

	{
		idLength := binary.BigEndian.Uint32(buf)
		buf = buf[4:]

		n.id = string(buf[:idLength])
		buf = buf[idLength:]
	}

	{
		n.state = binary.BigEndian.Uint32(buf)
		buf = buf[4:]
	}

	{
		suspectTimeLength := binary.BigEndian.Uint32(buf)
		buf = buf[4:]

		if err := n.suspectTime.UnmarshalBinary(buf[:suspectTimeLength]); err != nil {
			panic(exceptions.Package(err))
		}
		buf = buf[suspectTimeLength:]
	}

	{
		nextJmpLength := binary.BigEndian.Uint32(buf)
		buf = buf[4:]

		n.nextJmp = string(buf[:nextJmpLength])
		buf = buf[nextJmpLength:]
	}

	{
		n.distance = binary.BigEndian.Uint32(buf)
		buf = buf[4:]
	}

	return n
}
