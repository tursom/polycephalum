package tester

import (
	"github.com/tursom/GoCollections/encoding/hex"
)

var (
	publicKey  = decodeHex("043cda47fa0efe5ccb53c6627165696e2ae844385cc01e8834bbae0f926ff4a866ac2b89ab812a0e93fd2f326da4c85b0efc2e33ecf071e016dcc15d3e9c57692f")
	privateKey = decodeHex("83c7160ac25c99ded9f05ba050d2ed3aabd85d87878c2390b85dd970d1c67328")
)

func decodeHex(h string) []byte {
	decode, err := hex.DecodeString(h)
	if err != nil {
		panic(err)
	}

	return decode
}
