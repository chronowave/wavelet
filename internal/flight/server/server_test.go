package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/ipc"
	nats "github.com/nats-io/nats-server/v2/logger"
	"google.golang.org/grpc"

	"github.com/chronowave/wavelet/internal/test"
)

func TestStartFlight(t *testing.T) {
	port, err := getFreePort()
	if err != nil {
		t.Errorf("unable to get random port for testing: %v", err)
		return
	}

	logger := nats.NewStdLogger(true, true, true, true, true)
	server, err := StartFlight("localhost", uint(port), &test.NoOpFileHandler{}, &test.JSClusters{}, logger)
	if err != nil {
		t.Errorf("failed to start flight test server")
		return
	}
	defer server.Shutdown()

	client, err := flight.NewClientWithMiddleware(fmt.Sprintf("localhost:%d", port), nil, nil, grpc.WithInsecure())
	if err != nil {
		t.Errorf("create flight client error %v", err)
		return
	}

	ctx := context.Background()
	info, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{
		Type: flight.FlightDescriptor_CMD,
		Cmd: []byte(`find group-by(part($c,10),$b), avg($g)
                     where [$b /adf/adf eq(10)]
                           [$g /dkf/adf]
                           [/adf/dkf [/df between(15, 20)]
                                     [/adf eq(2.5)]
                           ]`),
	})
	if err != nil {
		t.Errorf("unable to get flight info %v", err)
		return
	}
	fmt.Sprintf("end point %v", info.Endpoint[0].Location[0].Uri)
	get, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Errorf("failed to create do get client %v", err)
		return
	}

	data, err := get.Recv()
	if err != nil {
		t.Errorf("unable to get flight data %v", err)
		return
	}

	rr, err := ipc.NewReader(bytes.NewReader(data.DataBody))
	if err != nil {
		t.Errorf("unable to read flight data %v", err)
		return
	}

	for {
		rec, err := rr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
		}
		arr := rec.Column(0)
		fmt.Printf("data type %v %v\n", arr.DataType(), arr.Len())
		uint64s := arr.(*array.Uint64)
		fmt.Printf("data is %v\n", uint64s.Uint64Values())
	}

	fmt.Printf("dafd is %v", *rr.Schema())
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
