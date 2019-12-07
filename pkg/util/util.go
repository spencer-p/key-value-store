package util

import (
	"log"
	"net/http"
	"strings"
)

// WithLog wraps an HTTP handler with a log line with the method and path.
func WithLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s\n", r.Method, r.URL)
		next.ServeHTTP(w, r)
	})
}

func StringSet(slice []string) map[string]struct{} {
	set := make(map[string]struct{})
	for i := range slice {
		set[slice[i]] = struct{}{}
	}
	return set
}

func SetEqual(s1, s2 map[string]struct{}) bool {
	if len(s1) != len(s2) {
		return false
	}

	for k := range s1 {
		_, ok := s2[k]
		if !ok {
			return false
		}
	}

	return true
}

// CorrectURL makes sure an address is a real URL.
func CorrectURL(addr string) string {
	if !strings.HasPrefix(addr, "http://") {
		addr = "http://" + addr
	}
	return addr
}
