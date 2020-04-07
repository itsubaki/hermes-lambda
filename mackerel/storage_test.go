package mackerel

import (
	"fmt"
	"os"
	"testing"
)

func TestStorage(t *testing.T) {
	os.Setenv("AWS_PROFILE", "hermes-lambda")
	os.Setenv("AWS_REGION", "ap-northeast-1")
	os.Setenv("PERIOD", "1d")

	s3, err := NewStorage()
	if err != nil {
		t.Errorf("new storage: %v", err)
	}

	b := "hermes-lambda-j96qd0m3kh"
	if err := s3.CreateIfNotExists(b); err != nil {
		t.Errorf("create if not exists: %v", err)
	}

	if err := s3.Write(b, "test/hogehoge.txt", []byte("hogehoge")); err != nil {
		t.Errorf("write :%v", err)
	}

	out, err := s3.Read(b, "test/hogehoge.txt")
	if err != nil {
		t.Errorf("read: %v", err)
	}

	exists, err := s3.Exists(b, "test/hogehoge.txt")
	if err != nil {
		t.Errorf("exists: %v", err)
	}
	fmt.Println(exists)

	fmt.Println(string(out))
}
