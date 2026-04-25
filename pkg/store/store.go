package store

// Store is an interface for a basic key value data structure
type Store interface {
	Get(key string) (string, error)
	Set(key, value string)
	Delete(key string)
}
