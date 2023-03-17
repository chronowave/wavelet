package main

import "C"
import (
	"bytes"
	"fmt"
	"runtime/cgo"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow/ipc"

	"github.com/chronowave/rust-bridge"
	"github.com/chronowave/ssql/go"
	"github.com/chronowave/wavelet/internal/test/testdata"
	nats "github.com/nats-io/nats-server/v2/logger"
)

// note: we need to add this to every main function to satisfy go build linking
//
//export send_to_go
func send_to_go(f unsafe.Pointer, hsz C.int, header *C.uchar, bsz C.int, body *C.uchar) bool {
	h := *(*cgo.Handle)(f)
	return h.Value().(rust_bridge.Callback).Send(unsafe.Slice((*byte)(header), int(hsz)), unsafe.Slice((*byte)(body), int(bsz)))
}

func main() {
	t := &testing.T{}
	schema := testdata.LoadSchemaFromFile(t, "../../../testdata/schema.json")

	//stmt, errs := ssql.Parse(`find group-by($a), sum($c), count($c), avg($c), min($c), max($c)
	//stmt, errs := ssql.Parse(`find group-by($a), count($c), count($c)
	// [$a /field3/field3 contain("ex")]
	stmt, errs := ssql.Parse(`find $a
from flight_test
where
[$c /field5][$a /field1] limit 1`)
	if len(errs) > 0 {
		t.Errorf("SSQL err %v", errs)
		return
	}

	stmt, err := schema.ReconcileStatement(stmt)
	if err != nil {
		t.Errorf("failed to reconcile query with schema %v", err)
		return
	}

	hosts := []string{
		//"http://localhost:5082",
		"http://localhost:4082",
		"http://localhost:5082",
	}

	logger := nats.NewStdLogger(true, true, true, true, true)

	transient := testdata.FlatColumnEntity(t, schema, 0)
	//actionHandler.SetTransientColumnEntity(48, transient)

	resp, err := rust_bridge.QueryCluster("", stmt, hosts, 0, 22, transient, logger)
	if err != nil {
		fmt.Printf("failed to call query cluster %v\n", err)
		return
	}
	if len(resp) == 0 {
		return
	}

	reader, _ := ipc.NewReader(bytes.NewReader(resp))

	arrowSchema := reader.Schema()

	t.Logf("schema is %v", *arrowSchema)

	err = reader.Err()
	if err != nil {
		t.Errorf("error %v", err)
		return
	}

	for reader.Next() {
		record := reader.Record()
		doc, err := record.MarshalJSON()
		if err != nil {
			t.Errorf("failed to marshall json from arrow batch record")
			return
		}
		fmt.Printf("rec: %v", string(doc))
		t.Logf("record is %v with rows=%d, cols=%d", record, record.NumRows(), record.NumCols())
	}
}
