package mackerel

import (
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Storage struct {
	client *s3.S3
}

func NewStorage() (*Storage, error) {
	s := session.Must(session.NewSession())
	c := s3.New(s)

	return &Storage{
		client: c,
	}, nil
}

func (s *Storage) CreateIfNotExists(bucketName string) error {
	out, err := s.client.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return fmt.Errorf("list buckets: %v", err)
	}

	found := false
	for _, b := range out.Buckets {
		if *b.Name == bucketName {
			found = true
			break
		}
	}

	if found {
		return nil
	}

	if _, err := s.client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}); err != nil {
		return fmt.Errorf("create bucket: %v", err)
	}

	return nil
}

func (s *Storage) Exists(bucketName, key string) (bool, error) {
	o, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(key),
	})
	if err != nil {
		return false, fmt.Errorf("list object: %v", err)
	}

	if *o.KeyCount == 0 {
		return false, nil
	}

	return true, nil
}

func (s *Storage) Write(bucketName, key string, b []byte) error {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Body:   bytes.NewReader(b),
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})

	if err != nil {
		return fmt.Errorf("put object: %v", err)
	}

	return nil
}

func (s *Storage) Read(bucketName, key string) ([]byte, error) {
	o, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return make([]byte, 0), fmt.Errorf("get object: %v", err)
	}
	defer o.Body.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(o.Body); err != nil {
		return make([]byte, 0), fmt.Errorf("read form: %v", err)
	}

	return buf.Bytes(), nil
}

func (s *Storage) Delete(bucketName, key string) error {
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})

	if err != nil {
		return fmt.Errorf("delete object=s3://%s/%s: %v", bucketName, key, err)
	}

	return nil
}
