package util

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCSVToSlice(t *testing.T) {
	in := "10.10.0.2:13800,10.10.0.3:13800,10.10.0.4:13800"
	want := []string{
		"10.10.0.2:13800",
		"10.10.0.3:13800",
		"10.10.0.4:13800",
	}

	got, err := CSVToSlice(in)
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	if diff := cmp.Diff(&got, &want); diff != "" {
		t.Errorf("Bad parse (-got,+want): %s", diff)
	}
}
