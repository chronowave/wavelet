package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"strconv"

	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/nats-io/nats-server/v2/server"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/chronowave/codec"
	"github.com/chronowave/rust-bridge"
	"github.com/chronowave/ssql/go"
	"github.com/chronowave/wavelet/internal/common"
	"github.com/chronowave/wavelet/internal/utils"
)

type FlightService struct {
	port    uint
	server  flight.Server
	file    common.ConsistentFileStore
	cluster common.ClusterInfo
	action  common.FlightActionHandler
	logger  server.Logger
	flight.BaseFlightServer
}

func StartFlight(
	nodeIP string,
	port uint,
	file common.ConsistentFileStore,
	cluster common.ClusterInfo,
	action common.FlightActionHandler,
	logger server.Logger) (*FlightService, error) {
	service := &FlightService{
		port:    port,
		server:  flight.NewServerWithMiddleware(nil, grpc.MaxRecvMsgSize(512*1024*1024)),
		file:    file,
		cluster: cluster,
		action:  action,
		logger:  logger,
	}
	err := service.server.Init(net.JoinHostPort(nodeIP, strconv.FormatUint(uint64(port), 10)))
	if err != nil {
		logger.Errorf("failed to initialize flight server[%d]: %v", port, err)
		return nil, err
	}

	service.server.RegisterFlightService(service)

	go service.server.Serve()

	logger.Noticef("flight server listens on %d", port)

	return service, nil
}

func (fs *FlightService) Shutdown() {
	if fs.server != nil {
		fs.server.Shutdown()
	}
}

// Handshake between client and server. Depending on the server, the
// handshake may be required to determine the token that should be used for
// future operations. Both request and response are streams to allow multiple
// round-trips depending on auth mechanism.
func (fs *FlightService) Handshake(server flight.FlightService_HandshakeServer) error {
	defer func() {
		if r := recover(); r != nil {
			fs.logger.Errorf("Flight server Handshake panic: %v, stack: %v", r, string(debug.Stack()))
		}
	}()

	_, err := server.Recv()
	if err != nil {
		return err
	}

	return server.Send(&flight.HandshakeResponse{
		Payload: []byte("this is is good"),
	})
}

// Get a list of available streams given a particular criteria. Most flight
// services will expose one or more streams that are readily available for
// retrieval. This api allows listing the streams available for
// consumption. A user can also provide a criteria. The criteria can limit
// the subset of streams that can be listed via this interface. Each flight
// service allows its own definition of how to consume criteria.
func (fs *FlightService) ListFlights(criteria *flight.Criteria, server flight.FlightService_ListFlightsServer) error {
	defer func() {
		if r := recover(); r != nil {
			fs.logger.Errorf("Flight server ListFlights panic: %v, stack: %v", r, string(debug.Stack()))
		}
	}()

	for name, schema := range fs.action.ListFlights() {
		//data, err := codec.SerializeAsArrowSchema(schema)
		data, err := schema.ToArrowFlightStream()
		if err != nil {
			fs.logger.Errorf("failed to serialize schema for flight %s: %v", name, err)
			return err
		}

		if err = server.Send(&flight.FlightInfo{
			Schema: data,
			FlightDescriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorPATH,
				Path: []string{name},
			},
			TotalRecords: -1,
			TotalBytes:   -1,
		}); err != nil {
			fs.logger.Errorf("failed to send flight info %s in list flights: %v", name, err)
			return err
		}
	}

	return nil
}

// For a given FlightDescriptor, get information about how the flight can be
// consumed. This is a useful interface if the consumer of the interface
// already can identify the specific flight to consume. This interface can
// also allow a consumer to generate a flight stream through a specified
// descriptor. For example, a flight descriptor might be something that
// includes a SQL statement or a Pickled Python operation that will be
// executed. In those cases, the descriptor will not be previously available
// within the list of available streams provided by ListFlights but will be
// available for consumption for the duration defined by the specific flight
// service.
func (fs *FlightService) GetFlightInfo(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	defer func() {
		if r := recover(); r != nil {
			fs.logger.Errorf("Flight server GetFlightInfo panic: %v, stack: %v", r, string(debug.Stack()))
		}
	}()

	switch desc.Type {
	case flight.DescriptorCMD:
		// parsing ssql, not used
		/*
			stmt, errs := ssql.Parse(string(desc.Cmd))
			if len(errs) > 0 {
				sb := strings.Builder{}

				return nil, errors.New(sb.String())
			}
			ticket, err := proto.Marshal(stmt)
			if err != nil {
				return nil, err
			}
		*/
		return &flight.FlightInfo{
			Endpoint: []*flight.FlightEndpoint{
				{
					Ticket: &flight.Ticket{
						Ticket: desc.Cmd,
					},
					Location: []*flight.Location{
						{
							Uri: fmt.Sprintf("localhost:%d", fs.port),
						},
					},
				},
			},
		}, nil
	case flight.DescriptorPATH:
		if len(desc.Path) != 1 {
			return nil, os.ErrInvalid
		}
		schema, err := fs.action.GetFlightSchema(desc.Path[0])
		if err != nil {
			return nil, err
		}

		data, err := schema.ToArrowFlightStream()
		return &flight.FlightInfo{
			Schema: data,
		}, err

	default:
	}
	return nil, errors.New("Invalid GetFlightInfo request")
}

// For a given FlightDescriptor, get the Schema as described in Schema.fbs::Schema
// This is used when a consumer needs the Schema of flight stream. Similar to
// GetFlightInfo this interface may generate a new flight that was not previously
// available in ListFlights.
func (fs *FlightService) GetSchema(_ context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	defer func() {
		if r := recover(); r != nil {
			fs.logger.Errorf("Flight server GetSchema panic: %v, stack: %v", r, string(debug.Stack()))
		}
	}()

	var data []byte
	if desc.Type == flight.DescriptorPATH {
		flightPath, err := fs.getPathFromFlightDescriptor(desc)
		if err != nil {
			fs.logger.Errorf("invalid flight path descriptor %v", err)
			return nil, err
		}

		schema, err := fs.action.GetFlightSchema(flightPath)
		if err != nil {
			return nil, err
		}

		data, err = schema.ToArrowFlightStream()
		if err != nil {
			return nil, err
		}
	} else if desc.Type == flight.DescriptorCMD {
		stmt, err := fs.parseSSQL(desc.Cmd)
		if err != nil {
			fs.logger.Errorf("failed to parse SSQL statement %s: %v", string(desc.Cmd), err)
			return nil, err
		}
		data, err = rust_bridge.StatementToArrowSchema(stmt, fs.logger)
		if err != nil {
			fs.logger.Errorf("failed to serialize arrow schema from SSQL statement %s: %v", string(desc.Cmd), err)
			return nil, err
		}
	}

	return &flight.SchemaResult{Schema: data}, nil
}

// Retrieve a single stream associated with a particular descriptor
// associated with the referenced ticket. A flight can be composed of one or
// more streams where each stream can be retrieved using a separate opaque
// ticket that the flight service uses for managing a collection of streams.
func (fs *FlightService) DoGet(ticket *flight.Ticket, server flight.FlightService_DoGetServer) error {
	defer func() {
		if r := recover(); r != nil {
			fs.logger.Errorf("Flight server DoGet panic: %v, stack: %v", r, string(debug.Stack()))
		}
	}()

	if len(ticket.Ticket) == 0 {
		return os.ErrInvalid
	}

	stmt, err := fs.parseSSQL(ticket.Ticket)
	if err != nil {
		return err
	}

	tid, tdata := fs.action.SerializeTransientColumnEntity(stmt.From)

	var result []byte
	if stmt.Aux.LocalOnly {
		// TODO: query directly to
	} else {
		// query cluster, scatter and gather
		hosts, local := fs.cluster.GetHostsAndLocal()
		peers := utils.BuildTonicFlightAddress(hosts, fs.port)
		result, err = rust_bridge.QueryCluster(fs.file.BaseDir(), stmt, peers, local, tid, tdata, fs.logger)
	}

	if err != nil {
		fs.logger.Errorf("%v returns err: %v", string(ticket.Ticket), err)
		return err
	}

	return server.Send(&flight.FlightData{DataBody: result})
}

// Push a stream to the flight service associated with a particular
// flight stream. This allows a client of a flight service to upload a stream
// of data. Depending on the particular flight service, a client consumer
// could be allowed to upload a single stream per descriptor or an unlimited
// number. In the latter, the service might implement a 'seal' action that
// can be applied to a descriptor once all streams are uploaded.
func (fs *FlightService) DoPut(server flight.FlightService_DoPutServer) error {
	defer func() {
		if r := recover(); r != nil {
			fs.logger.Errorf("Flight server DoPut panic: %v, stack: %v", r, string(debug.Stack()))
		}
	}()

	recv, err := server.Recv()
	if err != nil {
		return err
	}

	flightPath, err := fs.getPathFromFlightDescriptor(recv.FlightDescriptor)
	if err != nil {
		fs.logger.Errorf("invalid flight path descriptor %v", err)
		return err
	} else if len(recv.DataBody) == 0 {
		fs.logger.Errorf("flight %s empty flight put data", flightPath)
		return ErrEmptyFlightData
	}

	result, err := fs.action.PutFlightData(flightPath, recv.DataBody)
	if err != nil {
		fs.logger.Errorf("failed to put data to flight %s: %v", flightPath, err)
		return err
	}

	res, err := proto.Marshal(result)
	if err != nil {
		fs.logger.Errorf("failed to marshal put result for flight %s: %v", flightPath, err)
		return err
	}

	// return encoded number of entity
	return server.Send(&flight.PutResult{AppMetadata: res})
}

// Open a bidirectional data terminal for a given descriptor. This
// allows clients to send and receive arbitrary Arrow data and
// application-specific metadata in a single logical stream. In
// contrast to DoGet/DoPut, this is more suited for clients
// offloading computation (rather than storage) to a flight service.
func (fs *FlightService) DoExchange(server flight.FlightService_DoExchangeServer) error {
	defer func() {
		if r := recover(); r != nil {
			fs.logger.Errorf("Flight server DoExchange panic: %v, stack: %v", r, string(debug.Stack()))
		}
	}()

	flightExchange, err := server.Recv()
	if err != nil {
		fs.logger.Errorf("server receives exchange data err: %v", err)
		return err
	} else if flightExchange.FlightDescriptor == nil {
		fs.logger.Errorf("server receives exchange request with empty descriptor")
		return os.ErrInvalid
	}

	desc := flightExchange.FlightDescriptor
	streaming := false
	var flightPath string
	if desc.Type == flight.DescriptorCMD {
		// command is SSQL
		stmt, err := fs.parseSSQL(desc.Cmd)
		if err != nil {
			return err
		}

		data, err := proto.Marshal(stmt)
		if err != nil {
			fs.logger.Errorf("unable to marshal statement %v", err)
			return err
		}

		flightExchange.AppMetadata = []byte{0}
		flightExchange.DataBody = data

		flightPath = stmt.From
		streaming = true
	} else if len(desc.Path) == 0 {
		fs.logger.Errorf("server receives exchange request with empty descriptor on path variable")
		return os.ErrInvalid
	} else {
		flightPath = desc.Path[0]
	}

	tid, data := fs.action.SerializeTransientColumnEntity(flightPath)
	fs.logger.Noticef("exchange for %s, transient data size %d", flightPath, len(data))

	send := func(header, body []byte) bool {
		if err = server.Send(&flight.FlightData{DataHeader: header, DataBody: body}); err != nil {
			fs.logger.Errorf("failed to send flight data with header length %d, body length %d: %v", len(header), len(body), err)
			return true
		}

		return false
	}

	flightData, err := rust_bridge.Exchange(fs.file.BaseDir(), flightExchange, tid, data, streaming, send)

	if err != nil {
		fs.logger.Errorf("server processes exchange data err: %v", err)
		return err
	}

	if len(flightData.DataBody) > 0 {
		return server.Send(flightData)
	}

	return nil
}

// flight services can support an arbitrary number of simple actions in
// addition to the possible ListFlights, GetFlightInfo, DoGet, DoPut
// operations that are potentially available. DoAction allows a flight client
// to do a specific action against a flight service. An action includes
// opaque request and response objects that are specific to the type action
// being undertaken.
func (fs *FlightService) DoAction(action *flight.Action, server flight.FlightService_DoActionServer) error {
	defer func() {
		if r := recover(); r != nil {
			fs.logger.Errorf("Flight server DoAction panic: %v, stack: %v", r, string(debug.Stack()))
		}
	}()

	var resp []byte
	var err error
	v, ok := codec.FlightServiceAction_value[action.Type]
	if !ok {
		return os.ErrInvalid
	}
	switch codec.FlightServiceAction(v) {
	case codec.FlightServiceAction_CreateFlight:
		resp, err = fs.createFlight(action.Body)
	case codec.FlightServiceAction_DeleteFlight:
		err = fs.action.DeleteFlight(string(action.Body))
	case codec.FlightServiceAction_UpdateSchema:
		resp, err = fs.updateSchema(action.Body)
	case codec.FlightServiceAction_UpdateFile:
		resp, err = fs.updateFile(action.Body)
	case codec.FlightServiceAction_UploadFile:
		err = fs.file.OnUpload(action.Body)
	}

	if err != nil {
		fs.logger.Errorf("reply action %s err: %v", action.Type, err)
		return err
	}

	fs.logger.Noticef("reply action %s with data size %d", action.Type, len(resp))

	return server.Send(&flight.Result{Body: resp})
}

// A flight service exposes all of the available action types that it has
// along with descriptions. This allows different flight consumers to
// understand the capabilities of the flight service.
func (fs *FlightService) ListActions(_ *flight.Empty, server flight.FlightService_ListActionsServer) error {
	for _, action := range codec.FlightServiceAction_name {
		if err := server.Send(&flight.ActionType{Type: action}); err != nil {
			return err
		}
	}

	return nil
}

func (fs *FlightService) getPathFromFlightDescriptor(desc *flight.FlightDescriptor) (string, error) {
	if desc != nil && desc.Type == flight.DescriptorPATH {
		if len(desc.Path) == 1 {
			return desc.Path[0], nil
		}
		return "", ErrWrongFlightDescriptorPath
	}

	return "", ErrMissingFlightDescriptor
}

func (fs *FlightService) parseSSQL(input []byte) (*ssql.Statement, error) {
	stmt, errs := ssql.Parse(string(input))
	if len(errs) > 0 {
		if msg, e := json.Marshal(errs); e == nil {
			return nil, fmt.Errorf("invalid SSQL: %s", string(msg))
		} else {
			return nil, fmt.Errorf("invalid SSQL: %v", errs)
		}
	} else if len(stmt.From) == 0 {
		fs.logger.Errorf("SSQL statement %s is missing FROM clause", string(input))
		return nil, fmt.Errorf("invalid SSQL: missing FROM clause")
	} else if schema, err := fs.action.GetFlightSchema(stmt.From); err != nil {
		fs.logger.Errorf("invalid SSQL: flight %s is not found", string(input), stmt.From)
		return nil, fmt.Errorf("invalid SSQL: flight '%s' is not found", stmt.From)
	} else {
		return schema.ReconcileStatement(stmt)
	}
}
