package util

import (
	"log"
	"net/http"
)

// WithLog wraps an HTTP handler with a log line with the method and path.
func WithLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s\n", r.Method, r.URL)
		next.ServeHTTP(w, r)
	})
}
