package server

import (
	"fmt"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

func StartNATSServer(exe string, opts *server.Options) *server.Server {
	// Create the server with appropriate options.
	s, err := server.NewServer(opts)
	if err != nil {
		server.PrintAndDie(fmt.Sprintf("%s: %s", exe, err))
	}

	// Configure the logger based on the flags
	s.ConfigureLogger()

	s.Start()

	// wait for server to be ready
	for !s.ReadyForConnections(1 * time.Second) {
		s.Logger().Noticef("NATS server %s is not ready for connection", s.Name())
	}
	s.Logger().Noticef("NATS server %s is ready for connection", s.Name())

	// wait for jetstream cluster to be ready
	for !s.JetStreamIsCurrent() {
		s.Logger().Noticef("NATS server %s JetStream cluster is not ready", s.Name())
		time.Sleep(1 * time.Second)
	}
	s.Logger().Noticef("NATS server %s JetStream cluster is ready", s.Name())

	return s
}
