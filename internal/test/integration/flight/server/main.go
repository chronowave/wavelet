package main

import "C"
import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/cgo"
	"testing"
	"unsafe"

	"github.com/chronowave/rust-bridge"
	flight "github.com/chronowave/wavelet/internal/flight/server"
	"github.com/chronowave/wavelet/internal/store"
	"github.com/chronowave/wavelet/internal/test"
	"github.com/chronowave/wavelet/internal/test/testdata"
	nats "github.com/nats-io/nats-server/v2/logger"
)

const TestFlightPort = 5082

// note: we need to add this call back function to every main function to satisfy go build linking
//
//export send_to_go
func send_to_go(f unsafe.Pointer, hsz C.int, header *C.uchar, bsz C.int, body *C.uchar) bool {
	h := *(*cgo.Handle)(f)
	return h.Value().(rust_bridge.Callback).Send(unsafe.Slice((*byte)(header), int(hsz)), unsafe.Slice((*byte)(body), int(bsz)))
}

func main() {
	fmt.Print("start standalone flight server")

	done := make(chan bool, 1)

	logger := nats.NewStdLogger(true, true, true, true, true)

	dir := filepath.Join(os.TempDir(), "stand_alone")

	file := store.NewFileStore(filepath.Join(dir, "cw"), TestFlightPort, logger)
	actionHandler := test.NewInMemoryFlightActionHandler()
	arrowFlight, err := flight.StartFlight("", TestFlightPort,
		file, &test.JSClusters{}, actionHandler, logger)
	if err != nil {
		logger.Errorf("Arrow flight server is unable to listen on port %d, error: %v", TestFlightPort, err)
	}
	defer arrowFlight.Shutdown()

	// prepare flight
	flightName := "flight_test"
	err = file.PrepareFlight(flightName)
	if err != nil {
		logger.Errorf("failed to prepare flight %v", flightName)
		return
	}

	t := &testing.T{}
	schema := testdata.LoadSchemaFromFile(t, "../../../testdata/schema.json")
	actionHandler.CreateFlight(flightName, schema)

	actionHandler.TransientEntity = testdata.FlatColumnEntity(t, schema, 6)
	actionHandler.TransientID = 28

	for _, fid := range []uint64{24, 26} {
		index := testdata.BuildIndexFromTemplate(t, schema, 283)

		err = file.SaveLocal(flightName, fid, index)
		if err != nil {
			logger.Errorf("failed to save index to local %v", err)
			return
		}

		// upload data
		_ = file.UploadToStore(flightName, fid, index, []string{"localhost"})
	}

	<-done
}
