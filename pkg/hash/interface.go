package hash

import (
	"stathat.com/c/consistent"
)

// hash.Interface is an object that can map keys to target nodes.
type Interface interface {
	Get(string) (string, error)
	Members() []string
	Set([]string)
}

// Check for satisfaction from our hash options
var _ Interface = &consistent.Consistent{}
var _ Interface = &Modulo{}
