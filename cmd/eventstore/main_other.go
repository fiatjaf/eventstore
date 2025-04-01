//go:build windows

package main

import (
	"fmt"
	"runtime"

	"github.com/fiatjaf/eventstore"
)

func doMmmInit(path string) (eventstore.Store, error) {
	return nil, fmt.Errorf("unsupported OSs (%v)", runtime.GOOS)
}
