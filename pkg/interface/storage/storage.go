package storage

type Storage interface {
	CreateIfNotExists(bucketName string) error
	Exists(bucketName, key string) (bool, error)
	Write(bucketName, key string, b []byte) error
	Read(bucketName, key string) ([]byte, error)
	Delete(bucketName, key string) error
}
