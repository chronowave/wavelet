package terminal

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/chronowave/codec"
	"github.com/chronowave/wavelet/internal/common"
	"github.com/chronowave/wavelet/internal/nats/client/utils"
	"github.com/chronowave/wavelet/internal/nats/def"
)

type transient struct {
	id       uint64
	entities []*codec.ColumnedEntity
}

type Terminal struct {
	flight      string
	schema      *codec.Schema
	eventStream string
	jobStream   string
	dataStream  string

	nc     *nats.Conn
	jsc    nats.JetStreamContext
	kvs    nats.KeyValue
	logger server.Logger

	// transient pointer
	tp     unsafe.Pointer
	lock   sync.Mutex
	eraser func()
}

type config struct {
	template  string
	retention nats.RetentionPolicy
}

var streams = []config{
	{
		template:  def.JobStreamTemplate,
		retention: nats.WorkQueuePolicy,
	},
	{
		template: def.DataStreamTemplate,
	},
	{
		template: def.EventStreamTemplate,
	},
}

const (
	stateSnapshot = "StateSnapshot"
	schemaKey     = "Schema"
)

func Create(flight string, schema *codec.Schema, s *server.Server, jsc nats.JetStreamContext) error {
	replicas := 1
	if s.JetStreamIsClustered() {
		replicas = 3
	}
	logger := s.Logger()

	// reset schema
	if schema == nil {
		return os.ErrInvalid
	}

	data, err := schema.ToArrowFlightStream()
	if err != nil {
		logger.Errorf("node %s failed to parse flight %s schema file: %v", s.Name(), flight, err)
		return err
	}

	for _, conf := range streams {
		subj := fmt.Sprintf(conf.template, flight)
		utils.CreateStream(s, jsc, &nats.StreamConfig{
			Name:      subj,
			Subjects:  []string{subj},
			Retention: conf.retention,
			Replicas:  replicas,
		}, logger)
	}

	conf := &nats.KeyValueConfig{
		Bucket:   fmt.Sprintf(def.KeyValueTemplate, flight),
		History:  1,
		Replicas: replicas,
	}

	kvs, err := utils.CreateOrGetKeyValueStore(jsc, conf, logger)
	if err != nil {
		logger.Errorf("node %s failed to create kv store err: %v", s.Name(), err)
		return err
	}

	_, err = kvs.Create(schemaKey, data)
	if err != nil {
		logger.Errorf("node %s failed to save flight %s schema: %v", s.Name(), flight, err)
		return err
	}

	// state machine starts from 1
	state := StateMatchine{
		LastJobID:    1,
		LastMsgSeq:   1,
		LastEventSeq: 1,
		LastUpdate:   time.Now().Add(-24 * time.Hour),
		Jobs:         map[uint64]Job{},
	}

	data, err = json.Marshal(state)
	if err != nil {
		logger.Errorf("node %s failed to marshall StateMachine %v, err: %v", s.Name(), state, err)
		return err
	}

	data = append([]byte{byte(SnapshotEvent)}, data...)

	var ack *nats.PubAck
	for err = os.ErrInvalid; err != nil; {
		if ack, err = jsc.Publish(fmt.Sprintf(def.EventStreamTemplate, flight), data); err == nil {
			break
		}

		logger.Errorf("node %s retry in 1 sec, init state machine in event stream err: %v", s.Name(), err)
		time.Sleep(time.Second)
	}

	data = make([]byte, 8)
	binary.LittleEndian.PutUint64(data, ack.Sequence)

	for err = os.ErrInvalid; err != nil; {
		_, err = kvs.Create(stateSnapshot, data)
		if err == nil {
			break
		}
		logger.Errorf("node %s retry in 1 sec, init state machine snapshot in kv err: %v", s.Name(), err)
		time.Sleep(time.Second)
	}

	logger.Noticef("node %s has created flight %s", s.Name(), flight)
	return nil
}

// start terminal on local node
func Start(
	flight string,
	s *server.Server,
	nodeIP string,
	file common.ConsistentFileStore,
	cluster common.ClusterInfo,
) (*Terminal, error) {
	// prepare file store, ie directory as $storage/cw/flight
	if err := file.PrepareFlight(flight); err != nil {
		return nil, err
	}

	// connect to local NATS server
	nc, jsc, err := utils.ConnectToLocalServer(s)
	if err != nil {
		return nil, err
	}

	t := &Terminal{
		flight:      flight,
		eventStream: fmt.Sprintf(def.EventStreamTemplate, flight),
		jobStream:   fmt.Sprintf(def.JobStreamTemplate, flight),
		dataStream:  fmt.Sprintf(def.DataStreamTemplate, flight),

		nc:     nc,
		jsc:    jsc,
		logger: s.Logger(),
	}

	var (
		ctx, cancel  = context.WithCancel(context.Background())
		watcher      nats.KeyWatcher
		jobLeaderSub *nats.Subscription
	)

	go func() {
		// wait for kvs and streams are ready
		utils.WaitForStream(s, t.jsc, t.dataStream, s.Logger())
		utils.WaitForStream(s, t.jsc, t.eventStream, s.Logger())
		utils.WaitForStream(s, t.jsc, t.jobStream, s.Logger())
		t.kvs = utils.WaitForKeyValue(s, jsc, fmt.Sprintf(def.KeyValueTemplate, flight), s.Logger())

		var err error
		watcher, err = t.kvs.Watch(schemaKey)
		if err != nil {
			t.logger.Errorf("node %s failed to start schema key watcher for flight %s: %v", s.Name(), flight, err)
			return
		}
		//t.keyWatcher = watcher

		var (
			monitorCtx    context.Context
			monitorCancel context.CancelFunc
			lastSchemaVer uint64
		)

		updateSchemaEntry := func(entry nats.KeyValueEntry) {
			if entry == nil {
				t.logger.Warnf("node %s received nil schema event for flight %s", s.Name(), flight)
				return
			}
			if entry.Operation() != nats.KeyValuePut {
				t.logger.Warnf("node %s ignored schema event op %v for flight %s", s.Name(), entry.Operation(), flight)
				return
			}
			data := entry.Value()
			if len(data) == 0 {
				t.logger.Warnf("node %s received empty schema event for flight %s", s.Name(), flight)
				return
			}

			rev := entry.Revision()
			if rev > lastSchemaVer {
				t.logger.Noticef("node %s update schema ver to %d for flight %s from %v", s.Name(), rev, flight, lastSchemaVer)
				var schema codec.Schema
				if err := schema.FromArrowFlightStream(rev, data); err != nil {
					t.logger.Warnf("node %s error to unmarshal schema event for flight %s: %v, data: %v", s.Name(), flight, err, data)
					return
				}
				t.schema = &schema
				lastSchemaVer = rev
			} else {
				t.logger.Noticef("node %s skip schema ver %d update for flight %s, existing version %v", s.Name(), rev, flight, lastSchemaVer)
			}
		}

		for loading := true; loading; {
			if lastSchemaEntry, err := t.kvs.Get(schemaKey); err != nil {
				t.logger.Errorf("node %s will retry to load schema for flight %s: %v", s.Name(), flight, err)
				time.Sleep(100 * time.Millisecond)
			} else {
				updateSchemaEntry(lastSchemaEntry)
				loading = false
			}
		}

		go func() {
			t.logger.Noticef("node %s starts watching schema for flight %s", s.Name(), flight)
			defer func() {
				t.logger.Noticef("node %s stops watching schema for flight %s", s.Name(), flight)
			}()
			for evt := range watcher.Updates() {
				updateSchemaEntry(evt)
			}
		}()

		// setup job stream leader elected onFlightJobStreamLeaderElected
		onFlightJobStreamLeaderElected := func(isJobStreamLeader bool) {
			t.lock.Lock()
			defer t.lock.Unlock()

			if isJobStreamLeader {
				if monitorCtx == nil {
					monitorCtx, monitorCancel = context.WithCancel(ctx)

					t.logger.Noticef("node %s becomes job stream leader for flight %s", s.Name(), flight)

					// stream leader only
					state, err := t.monitorStateMachine(monitorCtx, s)
					if err != nil {
						return
					}
					go t.monitorJobStatus(monitorCtx, s, state)
				} else {
					t.logger.Noticef("node %s was job stream leader for flight %s", s.Name(), flight)
				}
			} else {
				if monitorCancel != nil {
					t.logger.Noticef("node %s released from job stream leader for flight %s", s.Name(), flight)
					monitorCancel()
					monitorCtx, monitorCancel = nil, nil
				} else {
					t.logger.Noticef("node %s is not job stream leader for flight %s", s.Name(), flight)
				}
			}
		}

		jobLeaderSub, err = t.nc.Subscribe(server.JSAdvisoryStreamLeaderElectedPre+"."+t.jobStream+".>", func(msg *nats.Msg) {
			var jslea server.JSStreamLeaderElectedAdvisory
			if err := json.Unmarshal(msg.Data, &jslea); err != nil {
				t.logger.Errorf("node %s unmarshal JSStreamLeaderElectedAdvisory for flight unexpected error: %v", s.Name(), flight, err)
				return
			}
			onFlightJobStreamLeaderElected(strings.Compare(s.Name(), jslea.Leader) == 0)
		})
		if err != nil {
			t.logger.Errorf("node %s failed sub job stream leader elected advisory event for flight %s", s.Name(), flight)
			return
		}

		t.waitOnJobQueue(ctx, s, nodeIP, file, cluster)

		onFlightJobStreamLeaderElected(s.GlobalAccount().JetStreamIsStreamLeader(t.jobStream))
	}()

	t.eraser = func() {
		cancel()

		if watcher != nil {
			if err := watcher.Stop(); err != nil {
				t.logger.Errorf("failed to stop schema key watcher for flight %s: %v", t.flight, err)
			}
		}

		if jobLeaderSub != nil {
			if err := jobLeaderSub.Unsubscribe(); err != nil {
				t.logger.Errorf("failed to unsub schema key watcher for flight %s: %v", t.flight, err)
			}
		}

		if nc != nil {
			nc.Close()
		}

		if err = file.RemoveFlight(flight); err != nil {
			t.logger.Errorf("error to remove flight %s from file store: %v", t.flight, err)
		}

		// TODO: remove flight quick filter

		t.logger.Noticef("flight %s is stopped", t.flight)
	}

	return t, nil
}

func (t *Terminal) UpdateSchema(schema *codec.Schema, prev uint64) error {
	data, err := schema.ToArrowFlightStream()
	if err != nil {
		return err
	}

	// note: update to KVS, kvs schema key watcher will load and assign schema to terminal
	_, err = t.kvs.Update(schemaKey, data, prev)
	return err
}

func (t *Terminal) GetSchema() *codec.Schema {
	return t.schema
}

func (t *Terminal) ParseJson(body []byte) ([][]byte, error) {
	entities, err := t.schema.ColumnizeJson(body)
	if err != nil {
		t.logger.Errorf("err to columnize json %v, doc: %v", err, string(body))
		return nil, err
	}

	data := make([][]byte, len(entities))
	for i, e := range entities {
		data[i], err = codec.SerializeColumnedEntity(e)
		if err != nil {
			t.logger.Errorf("failed to serialize %dth parsed entities: %v", i, err)
			return nil, err
		}
	}

	return data, nil
}

func (t *Terminal) SerializeTransientColumnEntity() (uint64, []byte) {
	tp := atomic.LoadPointer(&t.tp)
	if tp == nil {
		t.logger.Noticef("node doesn't have transient data for flight %s", t.flight)
		return 0, []byte{}
	}

	tmp := (*transient)(tp)
	data := codec.FlattenColumnizedEntities(tmp.entities)
	return tmp.id, data
}

func (t *Terminal) Delete() {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.logger.Noticef("removing flight %s", t.flight)
	if t.eraser != nil {
		t.eraser()
	}
}
