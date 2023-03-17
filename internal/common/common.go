package common

import (
	"github.com/chronowave/codec"
)

const (
	AppName = "chronowave"
	Replica = 2
)

type FlightActionHandler interface {
	// UpdateSchema takes flight name, previous revision, and updated schema
	// on successful, return newly updated schema
	// upon failure, return existing schema and revision with non nil error
	UpdateFlightSchema(flight string, prev uint64, schema *codec.Schema) (*codec.Schema, error)
	ListFlights() map[string]*codec.Schema
	GetFlightSchema(flight string) (*codec.Schema, error)
	CreateFlight(flight string, schema *codec.Schema) error
	DeleteFlight(flight string) error
	PutFlightData(flight string, json []byte) (*codec.PutResult, error)
	SerializeTransientColumnEntity(flight string) (uint64, []byte)
}

type ConsistentFileStore interface {
	BaseDir() string
	PrepareFlight(flight string) error
	RemoveFlight(flight string) error
	OnUpload(body []byte) error
	UploadToStore(flight string, fid uint64, data []byte, ips []string) []error
	SaveLocal(flight string, fid uint64, data []byte) error
	RemoveLocal(flight string, fid uint64) error
}

type ClusterInfo interface {
	GetHostsAndLocal() ([]string, int)
}
