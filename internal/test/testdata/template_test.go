package testdata

import (
	"encoding/json"
	"testing"
)

func TestLoadTemplate(t *testing.T) {
	schema := LoadSchemaFromFile(t, "schema.json")
	if t.Failed() {
		return
	}

	tpl := LoadTemplate(t)
	buf, _ := json.Marshal(tpl)
	entities, err := schema.ColumnizeJson(buf)
	if err != nil {
		t.Errorf("failed to parse json %v", err)
		return
	}

	if len(entities) == 0 {
		t.Errorf("failed to parse json, returned empty conten t")
		return
	}

	for _, e := range entities {
		t.Logf("parsed %v", *e)
	}
}

// can't reference rust-bridge, since rust-bridge has cgo, Go test package doesn't support cgo
/*
func TestBuildIndexFromTemplate(t *testing.T) {
	schema := LoadSchemaFromFile(t, "schema.json")

	tpl := LoadTemplate(t)

	segments := make([]TestData, 65536)
	for i := range segments {
		segments[i] = tpl
		segments[i].ID = uint64(i + 1)
	}

	buf, _ := json.Marshal(segments)
	entities, err := schema.ColumnizeJson(buf)
	if err != nil {
		t.Errorf("failed to parse json %v", err)
		return
	}

	if len(entities) == 0 {
		t.Errorf("failed to parse json, returned empty conten t")
		return
	}

	index, err := rust_bridge.BuildIndexFromColumnizedEntities(entities)
	if err != nil {
		t.Errorf("failed to build index %v", err)
		return
	}
	t.Logf("index size is %v", len(index))
}
*/
