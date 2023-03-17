package main

import "C"
import (
	"os"
	"path/filepath"
	"runtime/cgo"
	"strconv"
	"testing"
	"time"
	"unsafe"

	nats "github.com/nats-io/nats-server/v2/logger"

	"github.com/chronowave/rust-bridge"
	"github.com/chronowave/ssql/go"
	"github.com/chronowave/wavelet/internal/test/testdata"
)

// note: we need to add this call back function to every main function to satisfy go build linking
//
//export send_to_go
func send_to_go(f unsafe.Pointer, hsz C.int, header *C.uchar, bsz C.int, body *C.uchar) bool {
	h := *(*cgo.Handle)(f)
	return h.Value().(rust_bridge.Callback).Send(unsafe.Slice((*byte)(header), int(hsz)), unsafe.Slice((*byte)(body), int(bsz)))
}

func baseDir() string {
	return filepath.Join(os.TempDir(), "test_tmp", "cw")
}

func main() {
	flightName := "flight_test"
	baseDir := baseDir()
	os.MkdirAll(baseDir, os.ModePerm)

	dbDir := filepath.Join(baseDir, flightName, "db", "0")
	os.MkdirAll(dbDir, os.ModePerm)

	t := &testing.T{}

	schema := testdata.LoadSchemaFromFile(t, "../../testdata/schema.json")
	if t.Failed() {
		return
	}

	fid := uint64(24)
	// 108 no data, 107 has data
	// 52 is correct, 53 leads to null data (wrong)
	index := testdata.BuildIndexFromTemplate(t, schema, 65536)

	qf, idx := rust_bridge.SplitIndexQuickFilter(index)

	id := strconv.FormatUint(fid, 10)

	os.MkdirAll(filepath.Join(baseDir, flightName, "db"), os.ModePerm)
	os.MkdirAll(filepath.Join(baseDir, flightName, "tmp"), os.ModePerm)
	os.MkdirAll(filepath.Join(baseDir, flightName, "local"), os.ModePerm)

	indexPath := filepath.Join(baseDir, flightName, "tmp", id)
	qfPath := filepath.Join(baseDir, flightName, "tmp", id+".qf")

	err := os.WriteFile(indexPath, idx, os.ModePerm)
	if err != nil {
		t.Errorf("file server failed to save index to %v: %v", indexPath, err)
		return
	}

	err = os.WriteFile(qfPath, qf, os.ModePerm)
	if err != nil {
		t.Errorf("file server failed to save quick filter to %v: %v", qfPath, err)
		return
	}

	targetDir := filepath.Join(baseDir, flightName, "db", "0")
	_ = os.MkdirAll(targetDir, os.ModePerm)
	if err = os.Rename(indexPath, filepath.Join(targetDir, id)); err != nil {
		t.Errorf("file server failed to rename index to %v: %v", indexPath, err)
		return
	}

	if err = os.Rename(qfPath, filepath.Join(targetDir, id+".qf")); err != nil {
		t.Errorf("file server failed to rename index to %v: %v", indexPath, err)
		return
	}

	// add to quick filter for primary replica
	if err = rust_bridge.AddQuickFilter(flightName, fid, qf); err != nil {
		t.Errorf("file server failed to add quick filter: %v", err)
		return
	}

	stmt, errs := ssql.Parse(`find $a,$c
from flight_test
where
[$a /field3/field1 gt(1)]
[$c /field4/field2 between(0.3, 90.2)]`)
	if len(errs) > 0 {
		t.Errorf("SSQL err %v", errs)
		return
	}

	stmt, err = schema.ReconcileStatement(stmt)
	if err != nil {
		t.Errorf("failed to reconcile query with schema %v", err)
		return
	}

	start := time.Now()
	logger := nats.NewStdLogger(true, true, true, true, true)
	data, err := rust_bridge.QueryLocal(baseDir, stmt, 0, nil, logger)
	if err != nil {
		t.Errorf("failed to query %v", err)
		return
	}

	t.Logf("query takes %v returns data %v", time.Since(start), len(data))
}
