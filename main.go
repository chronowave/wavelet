package main

import "C"
import (
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"runtime/cgo"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/nats-io/nats-server/v2/server"

	"github.com/chronowave/rust-bridge"
	flight "github.com/chronowave/wavelet/internal/flight/server"
	"github.com/chronowave/wavelet/internal/kube"
	"github.com/chronowave/wavelet/internal/nats/client"
	nats "github.com/chronowave/wavelet/internal/nats/server"
	"github.com/chronowave/wavelet/internal/store"
	"github.com/chronowave/wavelet/internal/utils"
)

var usageStr = `Usage: wavelet [options]
NATS options: same as https://github.com/nats-io/nats-server/blob/main/main.go
Arrow flight options:
        --flight.port                Port Arrow flight Server to listen on
`

// usage will print out the flag options for the server.
func usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

type options struct {
	flight uint
	grpc   uint
}

// note: export call back function needs to be present in every main file to satisfy build linkage
//
//export send_to_go
func send_to_go(f unsafe.Pointer, hsz C.int, header *C.uchar, bsz C.int, body *C.uchar) bool {
	h := *(*cgo.Handle)(f)
	return h.Value().(rust_bridge.Callback).Send(unsafe.Slice((*byte)(header), int(hsz)), unsafe.Slice((*byte)(body), int(bsz)))
}

func main() {
	stopper := make(chan struct{}, 1)
	defer close(stopper)
	exe := "wavelet"

	// Create a FlagSet and sets the usage
	fs := flag.NewFlagSet(exe, flag.ExitOnError)
	fs.Usage = usage

	portOptions := &options{}
	fs.UintVar(&portOptions.flight, "flight.port", 8028, "Port Arrow flight Server to listen on.")
	fs.UintVar(&portOptions.grpc, "grpc.port", 50051, "Port Grpc Server to listen on.")

	// Configure the options from the flags/config file
	opts, err := server.ConfigureOptions(fs, os.Args[1:],
		server.PrintServerAndExit,
		fs.Usage,
		server.PrintTLSHelpAndDie)
	if err != nil {
		server.PrintAndDie(fmt.Sprintf("%s: %s", exe, err))
	} else if opts.CheckConfig {
		fmt.Fprintf(os.Stderr, "%s: configuration file %s is valid\n", exe, opts.ConfigFile)
		os.Exit(0)
	}

	podName := os.Getenv("POD_NAME")
	info, nodeIP := utils.GetClusterInfoFromEnv()
	if info == nil {
		var err error
		info, err = kube.NewClusterInfo(stopper)
		if err != nil {
			panic(err)
		}
	}

	ips, local := info.GetHostsAndLocal()
	routes := make([]*url.URL, 0, len(ips))
	for i, ip := range ips {
		if i == local {
			// escape local nats server
			continue
		}

		if nurl, err := url.Parse(fmt.Sprintf("nats://%s:%d", ip, opts.Cluster.Port)); err != nil {
			server.PrintAndDie(fmt.Sprintf("unable to parse url %v", err))
		} else {
			routes = append(routes, nurl)
		}
	}
	opts.Routes = routes

	// config random seed
	rand.Seed(int64(xxhash.Sum64([]byte(podName))))

	s := nats.StartNATSServer(exe, opts)

	logger := s.Logger()
	logger.Noticef("node %s started cluster %s, peers %v", s.Name(), opts.Cluster.Name, ips)

	file := store.NewFileStore(filepath.Join(opts.StoreDir, "cw"), portOptions.flight, logger)
	admin, err := client.NewLocalAdmin(s, podName, file, info)
	if err != nil {
		server.PrintAndDie(fmt.Sprintf("Nats direct client is unable to connect to local server, error: %v", err))
	}
	defer admin.Close()

	arrowFlight, err := flight.StartFlight(nodeIP, portOptions.flight, file, info, admin, logger)
	if err != nil {
		server.PrintAndDie(fmt.Sprintf("Arrow flight server is unable to listen on port %d, error: %v", portOptions.flight, err))
	}
	defer arrowFlight.Shutdown()

	s.WaitForShutdown()
}
