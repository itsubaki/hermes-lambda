package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/speps/go-hashids"
)

func NewID(salt string) (string, error) {
	sha := sha256.Sum256([]byte(salt))

	hd := hashids.NewData()
	hd.Salt = hex.EncodeToString(sha[:])

	h, err := hashids.NewWithData(hd)
	if err != nil {
		return "", fmt.Errorf("new hash: %v", err)
	}

	id, err := h.Encode([]int{1, 2, 3, 4})
	if err != nil {
		return "", fmt.Errorf("encode hash: %v", err)
	}

	return id, nil
}
