package server

import (
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/chronowave/codec"
	"github.com/chronowave/wavelet/internal/common"
	"github.com/chronowave/wavelet/internal/utils"
)

func (fs *FlightService) createFlight(info []byte) ([]byte, error) {
	var req codec.FlightSchemaRequest
	err := proto.Unmarshal(info, &req)
	if err != nil {
		return nil, err
	}

	var schema codec.Schema
	err = schema.FromArrowFlightStream(0, req.Schema)
	if err != nil {
		return nil, err
	}

	return nil, fs.action.CreateFlight(req.Flight, &schema)
}

func (fs *FlightService) updateSchema(info []byte) ([]byte, error) {
	var req codec.FlightSchemaRequest
	err := proto.Unmarshal(info, &req)
	if err != nil {
		return nil, err
	}

	altered, err := flight.DeserializeSchema(req.Schema, memory.DefaultAllocator)
	if err != nil {
		fs.logger.Errorf("flight service failed to parse flight exist request %v", err)
		return nil, err
	}

	exist, err := fs.action.GetFlightSchema(req.Flight)
	if err != nil {
		return nil, err
	}

	prevRev := exist.Rev()
	updated, err := exist.AddFields(altered)
	if err != nil {
		return nil, err
	}

	updated, err = fs.action.UpdateFlightSchema(req.Flight, prevRev, updated)
	if err != nil {
		fs.logger.Errorf("flight service failed to updated %s flight exist request :%v", req.Flight, err)
		return nil, err
	}
	return updated.ToArrowFlightStream()
}

func (fs *FlightService) updateFile(data []byte) ([]byte, error) {
	var req codec.FileTransferRequest
	err := proto.Unmarshal(data, &req)
	if err != nil {
		return nil, err
	}

	peers, _ := fs.cluster.GetHostsAndLocal()
	hosts := utils.SelectConsistentHosts(req.Id, peers, common.Replica)
	errs := fs.file.UploadToStore(req.Flight, req.Id, nil, hosts)
	for i, e := range errs {
		if e != nil {
			err = e
			fs.logger.Errorf("upload %s/%d to %s failed %v", req.Flight, req.Id, hosts[i], e)
		}
	}
	if err == nil {
		if err = fs.file.RemoveLocal(req.Flight, req.Id); err != nil {
		}
	}

	return []byte{}, err
}
