package testdata

// #cgo CFLAGS: -I/usr/share/chronowave
// #cgo LDFLAGS: -static -L/usr/share/chronowave -lssd -lm
// #include <stdlib.h>
// #include <rust.h>
import "C"

import (
	"encoding/json"
	"testing"

	"github.com/chronowave/codec"
	codecTest "github.com/chronowave/codec/_test"
	"github.com/chronowave/rust-bridge"
)

type TestNestedData struct {
	Field1 uint8   `json:"field1"`
	Field2 float64 `json:"field2"`
	Field3 string  `json:"field3"`
}

type TestData struct {
	ID uint64 `json:"id"`
	TestNestedData
	Field3 TestNestedData   `json:"field3"`
	Field4 []TestNestedData `json:"field4"`
}

func LoadTemplate(t *testing.T) TestData {
	text := `{
  "id": 1,
  "field1": 8,
  "field2": 28.5,
  "field3": {
    "field1": 8,
    "field2": 28.5,
    "field3": "example"
  },
  "field4": [{
    "field1": 8,
    "field2": 28.5
  },{
    "field1": 20,
    "field2": 68.5
  }]
}`

	var data TestData
	err := json.Unmarshal([]byte(text), &data)
	if err != nil {
		t.Fatalf("failed to unmarshal test data.json: %v", err)
		return data
	}

	return data
}

func BuildIndexFromTemplate(t *testing.T, schema *codec.Schema, sz int) []byte {
	dataTemplate := LoadTemplate(t)
	if t.Failed() {
		return nil
	}

	data := make([]TestData, sz)
	for i := 0; i < len(data); i++ {
		data[i] = dataTemplate
		data[i].ID = uint64(i)
	}

	buf, err := json.Marshal(data)
	if err != nil {
		t.Errorf("failed to clone data %v", err)
		return nil
	}

	entities, err := schema.ColumnizeJson(buf)
	if err != nil {
		t.Errorf("failed to columnize json %v", err)
		return nil
	}

	index, err := rust_bridge.BuildIndexFromColumnizedEntities(entities)
	if err != nil {
		t.Errorf("failed to build index data %v", err)
		return nil
	}

	return index
}

func FlatColumnEntity(t *testing.T, schema *codec.Schema, sz int) []byte {
	if sz == 0 {
		return []byte{}
	}

	dataTemplate := LoadTemplate(t)
	if t.Failed() {
		return nil
	}

	data := make([]TestData, sz)
	for i := 0; i < len(data); i++ {
		data[i] = dataTemplate
		data[i].ID = uint64(i)
	}

	buf, err := json.Marshal(data)
	if err != nil {
		t.Errorf("failed to clone data %v", err)
		return nil
	}

	entities, err := schema.ColumnizeJson(buf)
	if err != nil {
		t.Errorf("failed to columnize json %v", err)
		return nil
	}

	return codec.FlattenColumnizedEntities(entities)
}

func LoadSchemaFromFile(t *testing.T, path string) *codec.Schema {
	data, err := codecTest.LoadJsonSchema(path)
	if err != nil {
		t.Errorf("failed to load schema file %v", err)
		return nil
	}

	var schema codec.Schema
	err = schema.FromArrowFlightStream(1, data)
	if err != nil {
		t.Errorf("failed to unmarshal schema %v", err)
		return nil
	}

	return &schema
}
