package terminal

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"runtime/debug"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/chronowave/codec"
	"github.com/chronowave/rust-bridge"
	"github.com/chronowave/wavelet/internal/common"
	"github.com/chronowave/wavelet/internal/utils"
)

// NOTE: 2 bytes hard limit per block
const numOfDocs = ^uint16(0)

func (t *Terminal) processJobAction(
	ctx context.Context,
	s *server.Server,
	nodeIP string,
	file common.ConsistentFileStore,
	cluster common.ClusterInfo,
	sub *nats.Subscription,
	action *Job,
) {
	isDone := false

	defer func() {
		// clear transient working block
		atomic.StorePointer(&t.tp, nil)

		if r := recover(); r != nil {
			t.logger.Errorf("node %s err process data  %v", r)
			debug.PrintStack()
		}

		// sub == nil is ok
		_ = sub.Unsubscribe()

		if isDone {
			t.logger.Noticef("node %s job %v is done", s.Name(), *action)
		} else {
			t.logger.Noticef("node %s job %v contains error, will need restart", s.Name(), *action)
		}
	}()

	var (
		consumer *nats.ConsumerInfo
		err      error
	)

	for err = os.ErrInvalid; err != nil; {
		consumer, err = sub.ConsumerInfo()
		if err == nil {
			break
		}

		t.logger.Errorf("node %s get data stream consumer info errr %v", s.Name(), err)
		time.Sleep(500 * time.Millisecond)
	}

	t.logger.Noticef("node %s starts job %v consumer %s", s.Name(), *action, consumer.Name)

	updateJobStatus := func(event *Job) {
		action.Consumer = consumer.Name
		action.NodeIP = nodeIP
		event.Time = time.Now().Unix()
		update, err := json.Marshal(event)
		if err != nil {
			t.logger.Errorf("node %s skips job %v, due to marshal err: %v", s.Name(), *action, err)
			return
		}

		for err = os.ErrInvalid; err != nil; {
			_, err = t.jsc.Publish(t.eventStream, append([]byte{byte(JobEvent)}, update...))
			if err == nil {
				t.logger.Noticef("node %s posted event %v", s.Name(), *event)
				break
			}
			t.logger.Errorf("node %s will retry to publish job status %v due to: %v", s.Name(), *event, err)
			time.Sleep(100 * time.Millisecond)
		}
	}

	// set target time out
	var entities []*codec.ColumnedEntity
	switch action.Status {
	case Idle:
		action.Status = Active
		fallthrough
	case Active:
		updateJobStatus(action)

		seq := action.StartSeq
		var working *codec.ColumnedEntity
		for j := 0; j < int(numOfDocs); {
			msg, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				// note: shouldn't j++/seq++ in case no data
				if errors.Is(err, ctx.Err()) {
					return
				} else if !errors.Is(err, nats.ErrTimeout) {
					t.logger.Errorf("node %s consumer %s pulls from data stream error: %v", s.Name(), consumer.Name, err)
					time.Sleep(500 * time.Millisecond)
				}
				continue
			}

			if err = msg.Term(); err != nil {
				// ignore error, will delete anyway from this ephemeral subscriber anyway
				t.logger.Warnf("ack message %v err %v", *msg, err)
			}

			meta, err := msg.Metadata()
			if err != nil {
				t.logger.Warnf("node %s consumer %s works job %d skips message %v, meta data got err %v", s.Name(), consumer.Name, action.Id, *msg, err)
				continue
			} else if seq != meta.Sequence.Stream {
				t.logger.Warnf("node %s consumer %s works job %d skips message seq[%d] != msg[%v]", s.Name(), consumer.Name, action.Id, seq, meta.Sequence)
				continue
			} else if working, err = codec.DeserializeColumnedEntity(msg.Data); err != nil {
				t.logger.Warnf("node %s consumer %s works job %d skips message msg[%v] due to %v, data is %v",
					s.Name(), consumer.Name, action.Id, meta.Sequence, err, msg.Data)
				seq++
				continue
			}
			j++
			seq++
			//t.logger.Debugf("node %s consumer %s works job %d receives data sequence[%v], next is %d", s.Name(), consumer.Name, action.Id, meta.Sequence, seq)

			entities = append(entities, working)
			tmp := transient{id: action.Id, entities: entities}
			atomic.StorePointer(&t.tp, unsafe.Pointer(&tmp))
		}

		// kick start next job rotation
		if err := t.rotate(s, action.Id+1, seq); err != nil {
			t.logger.Errorf("node %s job rotation is stalled")
		}
	case Build:
		updateJobStatus(action)

		// catchup job, end must be greater than start, no need to post next job to job queue
		if action.StartSeq >= action.EndSeq {
			t.logger.Errorf("node %s skipped catchup job=%v invalid range", s.Name(), *action)
			return
		}

		var working *codec.ColumnedEntity
		for seq := action.StartSeq; seq < action.EndSeq; {
			msg, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				// note: shouldn't j++/seq++ in case no data
				if errors.Is(err, ctx.Err()) {
					return
				} else if !errors.Is(err, nats.ErrTimeout) {
					t.logger.Errorf("node %s pull job %v from data stream error %v", s.Name(), *action, err)
					time.Sleep(500 * time.Millisecond)
				}
				continue
			}

			if err = msg.Term(); err != nil {
				// ignore error, will delete anyway from this ephemeral subscriber anyway
				t.logger.Warnf("ack message %v err %v", *msg, err)
			}

			meta, err := msg.Metadata()
			if err != nil {
				t.logger.Warnf("skip message %v, meta data got err %v", *msg, err)
				continue
			} else if seq != meta.Sequence.Stream {
				t.logger.Warnf("skip message seq[%d] != meta.Sequence[%v]", seq, meta.Sequence)
				continue
			} else if working, err = codec.DeserializeColumnedEntity(msg.Data); err != nil {
				t.logger.Warnf("node %s consumer %s works job %d skips message msg[%v] due to %v, data is %v",
					s.Name(), consumer.Name, action.Id, meta.Sequence, err, msg.Data)
				seq++
				continue
			}
			seq++
			t.logger.Noticef("node %s received message sequence[%v], next is %d", s.Name(), meta.Sequence, seq)

			entities = append(entities, working)
			tmp := transient{id: action.Id, entities: entities}
			atomic.StorePointer(&t.tp, unsafe.Pointer(&tmp))
		}
	}

	data, err := rust_bridge.BuildIndexFromColumnizedEntities(entities)
	if err != nil {
		t.logger.Errorf("node %s consumer %s works job %d failed to build index from column entities %v", s.Name(), consumer.Name, action.Id, err)
		return
	}

	peers, _ := cluster.GetHostsAndLocal()
	hosts := utils.SelectConsistentHosts(action.Id, peers, 2)
	t.logger.Noticef("node %s uploads job %v data to hosts %v", s.Name(), *action, hosts)

	errs := file.UploadToStore(t.flight, action.Id, data, hosts)

	isDone = true
	for i, e := range errs {
		if e != nil {
			isDone = false
			t.logger.Errorf("node %s uploads job %v data to host %v failed: %v", s.Name(), *action, hosts[i], e)
		}
	}

	if isDone {
		// stream leader has timer to check and purge data
		action.Status = Done
		updateJobStatus(action)
		isDone = err == nil
	} else {
		if err = file.SaveLocal(t.flight, action.Id, data); err == nil {
			action.Status = Upload
			updateJobStatus(action)
		} else {
			t.logger.Errorf("node %s failed to save data for job %v: %v", action, err)
		}
	}
}
