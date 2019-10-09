package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
)

func interruptListener(cf context.CancelFunc) {

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)

	for {
		select {
		case <-sigc:
			cf()
			return
		default:
			runtime.Gosched()
		}
	}
}
