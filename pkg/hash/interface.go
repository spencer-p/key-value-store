package hash

// hash.Interface is an object that can map keys to target nodes.
type Interface interface {
	Get(string) (string, error)
	Members() []string
	Set([]string)
	TestAndSet([]string) bool
}

// Check for satisfaction from our hash options
var _ Interface = &Modulo{}
