package terminal

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"runtime/debug"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/chronowave/wavelet/internal/common"
)

type JobStatus byte

const (
	_ JobStatus = iota
	Idle
	Active
	Build
	Upload
	Done
)

type Job struct {
	Id       uint64
	Consumer string
	NodeIP   string
	Status   JobStatus
	Time     int64
	StartSeq uint64
	EndSeq   uint64
}

const jobQueue = "jobQueue"

func (t *Terminal) waitOnJobQueue(ctx context.Context, s *server.Server, nodeIP string, file common.ConsistentFileStore, cluster common.ClusterInfo) {
retry:
	jobSub, err := t.jsc.PullSubscribe(t.jobStream, jobQueue, nats.BindStream(t.jobStream))
	if err != nil {
		t.logger.Errorf("node %s failed to create queue subscriber on job stream err: %v", s.Name(), err)
		time.Sleep(1 * time.Second)
		goto retry
	}

	go func() {
		// put back on job queue once it is done
		defer func() {
			if r := recover(); r != nil {
				t.logger.Errorf("node %s fetch job queue err: %v", s.Name(), r)
				debug.PrintStack()
			}

			jobSub.Unsubscribe()

			t.logger.Errorf("node %s fetch job queue finished unexpectedly", s.Name())
		}()

		t.logger.Noticef("node %s is waiting on job queue", s.Name())

		for {
			// Job queue is WorkQueuePolicy
			msgs, err := jobSub.Fetch(1, nats.Context(ctx))
			if err != nil {
				if errors.Is(err, ctx.Err()) {
					return
				} else if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				t.logger.Errorf("node %s fetch job queue stream received error: %v", s.Name(), err)
				break
			}

			for _, msg := range msgs {
				if msg == nil {
					t.logger.Errorf("node %s fetch job queue stream received empty message", s.Name())
					continue
				}

				if err = msg.AckSync(); err != nil {
					t.logger.Errorf("node %s ack msg %v failed with err: %v", s.Name(), *msg, err)
					continue
				}

				var action Job
				if err = json.Unmarshal(msg.Data, &action); err != nil {
					t.logger.Errorf("node %s unmarshall job command raw message [%v] error: %v", s.Name(), string(msg.Data), err)
					return
				}

				// sub to data stream ephemeral from start seq, it should be before ack msg, TODO: delete startSequence
				dataSub, err := t.jsc.SubscribeSync(t.dataStream, nats.BindStream(t.dataStream), nats.StartSequence(action.StartSeq))
				if err != nil {
					t.logger.Errorf("node %s is unable to subscribe to data stream: %v", err)
					return
				}
				// note: processJobAction will call dataSub.Unsub

				// process job should re-sub to job stream after done of collecting data from data stream
				t.processJobAction(ctx, s, nodeIP, file, cluster, dataSub, &action)
			}
		}

		t.waitOnJobQueue(ctx, s, nodeIP, file, cluster)
	}()
}

func (t *Terminal) rotate(s *server.Server, nextId, seq uint64) error {
	t.logger.Noticef("node %s posted rotate event id=%d, seq=%d", s.Name(), nextId, seq)
	data, err := json.Marshal(Job{
		Id:       nextId,
		Status:   Idle,
		Time:     time.Now().Unix(),
		StartSeq: seq,
	})
	if err != nil {
		t.logger.Errorf("node %s marshall job err: %v", s.Name(), err)
		return err
	}

	for err = os.ErrInvalid; err != nil; {
		_, err = t.jsc.Publish(t.eventStream, append([]byte{byte(JobEvent)}, data...))
		if err == nil {
			break
		}

		t.logger.Errorf("node %s publish rotate job data to event stream err: %v", s.Name(), err)
		time.Sleep(500 * time.Millisecond)
	}

	for err = os.ErrInvalid; err != nil; {
		_, err = t.jsc.Publish(t.jobStream, data)
		if err == nil {
			break
		}

		t.logger.Errorf("node %s publish rotate job data to job stream err: %v", s.Name(), err)
		time.Sleep(500 * time.Millisecond)
	}

	return err
}
