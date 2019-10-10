package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
)

func interruptListener(cf context.CancelFunc) {

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	for {
		select {
		case <-sigChan:
			cf()
			return
		default:
			runtime.Gosched()
		}
	}
}
