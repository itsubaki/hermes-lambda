package infrastructure

type Storage struct {
}

func NewStorage() *Storage {
	return &Storage{}
}

func (s *Storage) CreateIfNotExists(bucketName string) error {
	return nil
}

func (s *Storage) WriteIfNotExists(bucketName, fileName string) error {
	return nil
}

func (s *Storage) Read(bucketName, fileName string) ([]byte, error) {
	return make([]byte, 0), nil
}
