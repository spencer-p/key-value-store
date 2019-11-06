package util

import (
	"encoding/csv"
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

// CSVToSlice parses a comma separated string into its constituent strings.
func CSVToSlice(in string) ([]string, error) {
	reader := csv.NewReader(strings.NewReader(in))
	return reader.Read()
}
