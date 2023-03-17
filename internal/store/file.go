package store

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/nats-io/nats-server/v2/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/chronowave/codec"
	"github.com/chronowave/rust-bridge"
	"github.com/chronowave/wavelet/internal/common"
	"github.com/chronowave/wavelet/internal/utils"
)

const (
	db    = "db"
	tmp   = "tmp"
	local = "local"
	ext   = ".qf"

	primary = "0"
)

func NewFileStore(dir string, port uint, logger server.Logger) common.ConsistentFileStore {
	logger.Noticef("create chronowave consistent file store dir: %v", dir)
	return &fileClient{dir: dir, port: port, logger: logger}
}

type fileClient struct {
	dir    string
	port   uint
	logger server.Logger
}

func (c *fileClient) BaseDir() string {
	return c.dir
}

func (c *fileClient) PrepareFlight(flight string) error {
	for _, subDir := range []string{db, tmp, local} {
		if err := os.MkdirAll(filepath.Join(c.dir, flight, subDir), os.ModePerm); err != nil && !errors.Is(err, os.ErrExist) {
			c.logger.Errorf("prepare flight %s failed to create dir %s: %v", flight, subDir, err)
			return err
		}
	}

	dir := filepath.Join(c.dir, flight, db, primary)
	if _, err := os.Stat(dir); err == nil {
		go func() {
			c.logger.Noticef("starting to load quick filters from dir %v", dir)
			loaded := 0
			err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
				if !info.IsDir() && filepath.Ext(path) == ext {
					qf, err := os.ReadFile(path)
					if err != nil {
						c.logger.Errorf("error loads quick filter %s", path)
						return err
					}
					id, err := strconv.ParseUint(strings.TrimSuffix(info.Name(), ext), 10, 64)
					if err != nil {
						c.logger.Errorf("error parse quick filter name %s", info.Name())
						return err
					}
					if err = rust_bridge.AddQuickFilter(flight, id, qf); err != nil {
						c.logger.Errorf("failed to add quick filter %s: %v", path, err)
						return err
					}
					loaded += 1
				}

				return nil
			})

			if err != nil {
				c.logger.Errorf("loading %v returns err: %v", err)
			}
			c.logger.Noticef("loaded %d quick filters from dir %v", loaded, dir)
		}()
	}

	return nil
}

func (c *fileClient) RemoveFlight(flight string) error {
	src := filepath.Join(c.dir, flight)
	return os.Rename(src, src+".delete")
}

func (c *fileClient) OnUpload(body []byte) error {
	var req codec.FileTransferRequest
	err := proto.Unmarshal(body, &req)
	if err != nil {
		c.logger.Errorf("file server received invalid file transfer request: %v", err)
		return err
	} else if len(req.Flight) == 0 {
		c.logger.Errorf("file server received invalid file transfer request %v", req)
		return os.ErrInvalid
	}

	c.logger.Noticef("file server received flight %s upload request %d with size %d as replica %d",
		req.Flight, req.Id, len(req.Data), req.Replica)

	qf, idx := rust_bridge.SplitIndexQuickFilter(req.Data)

	id := strconv.FormatUint(req.Id, 10)
	indexPath := filepath.Join(c.dir, req.Flight, tmp, id)
	qfPath := filepath.Join(c.dir, req.Flight, tmp, id+ext)

	err = os.WriteFile(indexPath, idx, os.ModePerm)
	if err != nil {
		c.logger.Errorf("file server failed to save index to %v: %v", indexPath, err)
		return err
	}

	err = os.WriteFile(qfPath, qf, os.ModePerm)
	if err != nil {
		c.logger.Errorf("file server failed to save quick filter to %v: %v", qfPath, err)
		return err
	}

	targetDir := filepath.Join(c.dir, req.Flight, db, strconv.FormatUint(uint64(req.Replica), 10))
	_ = os.MkdirAll(targetDir, os.ModePerm)
	if err = os.Rename(indexPath, filepath.Join(targetDir, id)); err != nil {
		c.logger.Errorf("file server failed to rename index to %v: %v", indexPath, err)
		return err
	}

	if err = os.Rename(qfPath, filepath.Join(targetDir, id+ext)); err != nil {
		c.logger.Errorf("file server failed to rename index to %v: %v", indexPath, err)
		return err
	}

	if req.Replica == 0 {
		// add to quick filter for primary replica
		if err = rust_bridge.AddQuickFilter(req.Flight, req.Id, qf); err != nil {
			c.logger.Errorf("file server failed to add quick filter: %v", err)
			return err
		}
		c.logger.Noticef("file server added quick filter of size %v to flight %s, block %d", len(qf), req.Flight, req.Id)
	}

	return nil
}

// Upload will upload index file to ips[], where len(ips) == number of replicas, ips[0] is first and primary
// ips[1..] are the backups
func (c *fileClient) UploadToStore(flight string, fid uint64, data []byte, hosts []string) []error {
	errs := make([]error, len(hosts))
	if len(data) == 0 {
		var err error
		if data, err = os.ReadFile(filepath.Join(c.dir, flight, local, strconv.FormatUint(fid, 10))); err != nil {
			for i := range errs {
				errs[i] = err
			}

			return errs
		}
	}

	peers := utils.BuildFlightAddress(hosts, c.port)
	c.logger.Noticef("uploading flight %s index %d with size %d to %v", flight, fid, len(data), peers)

	// hosts[0] is the primary, hosts[1..] are the backups
	var wg sync.WaitGroup
	wg.Add(len(peers))
	for i, peer := range peers {
		go func(ith int, remote string) {
			defer wg.Done()
			errs[ith] = c.upload(flight, fid, uint32(ith), data, remote)
		}(i, peer)
	}

	wg.Wait()
	c.logger.Noticef("uploaded flight %s index %d with size %d to %v: %v", flight, fid, len(data), peers, errs)
	return errs
}

func (c *fileClient) SaveLocal(flight string, fid uint64, data []byte) error {
	return os.WriteFile(filepath.Join(c.dir, flight, local, strconv.FormatUint(fid, 10)), data, os.ModePerm)
}

func (c *fileClient) RemoveLocal(flight string, fid uint64) error {
	return os.Remove(filepath.Join(c.dir, flight, local, strconv.FormatUint(fid, 10)))
}

// end internal.common.ConsistentFileStore
func (c *fileClient) upload(flightName string, id uint64, replica uint32, data []byte, addr string) error {
	var client flight.Client
	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()

	req, err := proto.Marshal(&codec.FileTransferRequest{
		Flight:  flightName,
		Id:      id,
		Replica: replica,
		Data:    data,
	})
	if err != nil {
		c.logger.Errorf("error to marshall file transfer request: %v", err)
		return err
	}

	uploadFile := flight.Action{
		Type: codec.FlightServiceAction_UploadFile.String(),
		Body: req,
	}

	var action flight.FlightService_DoActionClient

	for retries := 0; retries < 5; retries++ {
		if client != nil {
			_ = client.Close()
		}
		c.logger.Noticef("uploading flight %s index %d with size %d to %v", flightName, id, len(data), addr)
		client, err = flight.NewClientWithMiddleware(addr, nil, nil,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			c.logger.Errorf("uploading flight %s index %d with size %d to %v: %v", flightName, id, len(data), addr, err)
		} else if action, err = client.DoAction(context.Background(), &uploadFile); err != nil {
			c.logger.Errorf("uploading flight %s index %d with size %d to %v: %v", flightName, id, len(data), addr, err)
		} else if _, err = action.Recv(); err != nil {
			c.logger.Errorf("uploading flight %s index %d with size %d to %v: %v", flightName, id, len(data), addr, err)
		} else {
			c.logger.Noticef("uploaded flight %s index %d with size %d to %v: %v", flightName, id, len(data), addr, err)
			break
		}
		time.Sleep(time.Millisecond * 500)
	}

	return err
}
