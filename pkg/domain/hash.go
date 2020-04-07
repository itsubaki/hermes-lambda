package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/speps/go-hashids"
)

func NewID(seed string) (string, error) {
	sha := sha256.Sum256([]byte(seed))
	salt := hex.EncodeToString(sha[:])

	hd := hashids.NewData()
	hd.Salt = salt

	h, err := hashids.NewWithData(hd)
	if err != nil {
		return "", fmt.Errorf("new hash: %v", err)
	}

	id, err := h.Encode([]int{45, 434, 1313, 99})
	if err != nil {
		return "", fmt.Errorf("encode hash: %v", err)
	}

	return id, nil
}
