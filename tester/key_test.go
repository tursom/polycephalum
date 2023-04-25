package tester

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/crypto/secp256k1"
)

func Test_keys(t *testing.T) {
	fmt.Println(publicKey)
	fmt.Println(privateKey)

	msg := sha256.Sum256([]byte("hello, world"))
	sign, err := secp256k1.Sign(msg[:], privateKey)
	if err != nil {
		t.Fatal(err)
	}

	if ok := secp256k1.VerifySignature(publicKey, msg[:], sign[:64]); !ok {
		t.Fatal(sign)
	}
}
