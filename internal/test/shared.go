package test

import (
	"fmt"
	"github.com/chronowave/codec"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"

	flightnats "github.com/chronowave/wavelet/internal/nats/server"
)

const (
	clientPort  = 4222
	clusterPort = 6222
)

type JSClusters struct {
	Servers []*server.Server
	IPS     []string
	Local   int
}

func (jsc *JSClusters) GetHostsAndLocal() ([]string, int) {
	return jsc.IPS, jsc.Local
}

func (jsc *JSClusters) Shutdown() {
	for _, s := range jsc.Servers {
		if s.Running() {
			s.Shutdown()
			s.WaitForShutdown()
		}
	}
}

type NoOpFileHandler struct {
}

func (n *NoOpFileHandler) BaseDir() string {
	return ""
}

func (n *NoOpFileHandler) Stop() {
}

func (n *NoOpFileHandler) PrepareFlight(flight string) error {
	return nil
}

func (n *NoOpFileHandler) RemoveFlight(flight string) error {
	return nil
}

func (n *NoOpFileHandler) OnUpload(req *codec.FileTransferRequest) error {
	return nil
}

func (n *NoOpFileHandler) UploadToStore(flight string, fid uint64, data []byte, ips []string) []error {
	return nil
}

func (n *NoOpFileHandler) SaveLocal(flight string, fid uint64, data []byte) error {
	return nil
}

func (n *NoOpFileHandler) RemoveLocal(flight string, fid uint64) error {
	return nil
}

type InMemoryFlightActionHandler struct {
	TransientID     uint64
	TransientEntity []byte
	schemas         map[string]*codec.Schema
}

func NewInMemoryFlightActionHandler() *InMemoryFlightActionHandler {
	return &InMemoryFlightActionHandler{
		TransientID:     0,
		TransientEntity: []byte{},
		schemas:         map[string]*codec.Schema{},
	}
}

func (n *InMemoryFlightActionHandler) UpdateFlightSchema(flight string, _ uint64, schema *codec.Schema) (*codec.Schema, error) {
	n.schemas[flight] = schema
	return schema, nil
}

func (n *InMemoryFlightActionHandler) ListFlights() map[string]*codec.Schema {
	return nil
}

func (n *InMemoryFlightActionHandler) GetFlightSchema(flight string) (*codec.Schema, error) {
	schema, ok := n.schemas[flight]
	var err error
	if !ok {
		err = os.ErrNotExist
	}
	return schema, err
}

func (n *InMemoryFlightActionHandler) CreateFlight(flight string, schema *codec.Schema) error {
	n.schemas[flight] = schema
	return nil
}

func (n *InMemoryFlightActionHandler) DeleteFlight(flight string) error {
	return nil
}

func (n *InMemoryFlightActionHandler) PutFlightData(flight string, json []byte) (*codec.PutResult, error) {
	return &codec.PutResult{}, nil
}

func (n *InMemoryFlightActionHandler) SerializeTransientColumnEntity(flight string) (uint64, []byte) {
	return n.TransientID, n.TransientEntity
}

func (n *InMemoryFlightActionHandler) SetTransientColumnEntity(id uint64, data []byte) {
	n.TransientID = id
	n.TransientEntity = data
}

func RunBasicJetStreamServer(ith int, ip string, routes []*url.URL) *server.Server {
	opts := natsserver.DefaultTestOptions.Clone()
	opts.Host = ip
	opts.Port = clientPort
	opts.JetStream = true
	if len(routes) > 0 {
		opts.ServerName = fmt.Sprintf("s_%d", ith)
		opts.Cluster.Name = "jsc_test"
		opts.Cluster.Host = ip
		opts.Cluster.Port = clusterPort
		opts.Routes = routes
	}
	opts.StoreDir = path.Join("/tmp/nats/jetstream/", strconv.FormatInt(int64(ith), 10))
	opts.NoLog = false

	return flightnats.StartNATSServer("flightnats", opts)
}

func LoopbackIPs() []string {
	ips := make([]string, 5)
	for i := range ips {
		ips[i] = fmt.Sprintf("127.0.0.%d", i+1)
	}
	return ips
}

func RunClusterJetStreamServer(ips []string) *JSClusters {
	sb := strings.Builder{}
	for i := range ips {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf("nats://%s:%d", ips[i], clusterPort))
	}
	routes := server.RoutesFromStr(sb.String())
	servers := make([]*server.Server, len(ips))
	var wg sync.WaitGroup
	wg.Add(len(servers))
	for i := range servers {
		go func(ith int) {
			servers[ith] = RunBasicJetStreamServer(ith, ips[ith], routes)
			wg.Done()
		}(i)
	}

	wg.Wait()

	return &JSClusters{Servers: servers, IPS: ips}
}
