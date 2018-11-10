package dynamodbstore

import (
	"testing"
)

func TestKey(t *testing.T) {
	version := 1
	key := makeKey(version)
	if !isKey(key) {
		t.Fatalf("got false; want true")
	}

	found, err := versionFromKey(key)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if got, want := found, version; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}
}

func TestVersionFromKey(t *testing.T) {
	testCases := map[string]struct {
		Key      string
		Version  int
		HasError bool
	}{
		"simple": {
			Key:     "_1",
			Version: 1,
		},
		"invalid-prefix": {
			Key:      "1",
			HasError: true,
		},
		"invalid-version": {
			Key:      "_a",
			HasError: true,
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			version, err := versionFromKey(tc.Key)
			if tc.HasError {
				if got, want := err, errInvalidKey; got != want {
					t.Fatalf("got %v; want %v", got, want)
				}
			} else {
				if err != nil {
					t.Fatalf("got %v; want nil", err)
				}
				if got, want := version, tc.Version; got != want {
					t.Fatalf("got %v; want %v", got, want)
				}
			}
		})
	}
}
