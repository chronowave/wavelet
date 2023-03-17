package terminal

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"runtime/debug"
	"sort"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type EventDataMarker uint8

const (
	_ EventDataMarker = iota
	SnapshotEvent
	JobEvent
)

const snapshotSize = 8

type StateMatchine struct {
	LastJobID    uint64
	LastMsgSeq   uint64
	LastEventSeq uint64
	LastUpdate   time.Time
	Jobs         map[uint64]Job
}

func (t *Terminal) monitorJobStatus(ctx context.Context, s *server.Server, state **StateMatchine) {
	t.logger.Noticef("node %s start checking job status", s.Name())
	ticker := time.NewTicker(10 * time.Second)
	defer func() {
		t.logger.Noticef("node %s stop checking job status", s.Name())
		ticker.Stop()
	}()

	check := func(state **StateMatchine) {
		defer func() {
			if r := recover(); r != nil {
				t.logger.Errorf("node %s checking job status contains err %v", s.Name(), r)
				debug.PrintStack()
			}
		}()

		var consumers []*nats.ConsumerInfo
		for c := range t.jsc.Consumers(t.dataStream, nats.Context(ctx)) {
			consumers = append(consumers, c)
		}
		t.logger.Noticef("node %s reported %d consumers on data stream", s.Name(), len(consumers))

		// make a copy of state
		t.lock.Lock()
		lastState := **state
		t.lock.Unlock()

		// note: data consumers should have at least one consumer at any time
		if len(consumers) == 0 {
			t.logger.Noticef("node %s is elected leader in a cluster without consumers on data stream", s.Name())

			var (
				jobQueueInfo *nats.StreamInfo
				err          error
			)

			for jobQueueInfo == nil {
				if jobQueueInfo, err = t.jsc.StreamInfo(t.jobStream, nats.Context(ctx)); err != nil {
					t.logger.Warnf("node %s will retry to  get job stream info, err: %v", s.Name(), err)
					time.Sleep(1 * time.Second)
				}
			}

			if jobQueueInfo.State.Msgs == 0 && time.Since(lastState.LastUpdate) > 10*time.Second {
				t.logger.Noticef("node %s will bootstrap job queue, last state updated at %v", s.Name(), lastState.LastUpdate)

				if err = t.rotate(s, lastState.LastJobID, lastState.LastMsgSeq); err != nil {
					t.logger.Noticef("node %s restart job queue %v failed: %v", s.Name(), lastState, err)
				} else {
					t.logger.Noticef("node %s restarted job queue %v", s.Name(), lastState)
				}
			}
		} else {
			t.logger.Noticef("node %s checked job queue is running", s.Name())
		}

		jobIDs := make([]uint64, 0, len(lastState.Jobs))
		for k := range lastState.Jobs {
			jobIDs = append(jobIDs, k)
		}

		sort.Slice(jobIDs, func(i, j int) bool { return jobIDs[i] < jobIDs[j] })

		t.logger.Noticef("node %s is checking jobs %v", s.Name(), jobIDs)

		for _, jid := range jobIDs {
			job := lastState.Jobs[jid]
			if job.Status == Idle || job.Status == Active || job.Status == Build {
				t.logger.Noticef("node %s stops at idle/active/build job %v", s.Name(), job)
				// TODO: check timestamp and make sure the consumer is still running
			}
		}
	}

	check(state)
	for {
		select {
		case <-ticker.C:
			check(state)
		case <-ctx.Done():
			return
		}
	}
}

func (t *Terminal) monitorStateMachine(ctx context.Context, s *server.Server) (**StateMatchine, error) {
	var (
		state           *StateMatchine
		eventStreamInfo *nats.StreamInfo
		err             error
		processData     func(inner **StateMatchine, data []byte, seq uint64, cnt int8) (int8, bool)
		prevMsgs        = make([]*nats.Msg, snapshotSize*2)
	)

	for err = os.ErrInvalid; err != nil; {
		if eventStreamInfo, err = t.jsc.StreamInfo(t.eventStream, nats.Context(ctx)); err == nil {
			break
		}
		t.logger.Errorf("node %s will retry to get event stream info: %v", s.Name(), err)
		time.Sleep(time.Second)
	}

	processData = func(inner **StateMatchine, data []byte, seq uint64, cnt int8) (int8, bool) {
		canSnap := false
		switch EventDataMarker(data[0]) {
		case SnapshotEvent:
			var snapshot StateMatchine
			if err = json.Unmarshal(data[1:], &snapshot); err != nil {
				t.logger.Errorf("node %s unmarshall state machine from event stream err: %v", s.Name(), err)
				return cnt, false
			}
			t.logger.Noticef("node %s %dth event message is state snapshot, catchup last %d: %v", s.Name(), seq, seq-snapshot.LastEventSeq, snapshot)

			// no need to be atomic
			*inner = &snapshot
			for m := snapshot.LastEventSeq; m < seq; m++ {
				idx := m % uint64(len(prevMsgs))
				if prevMsgs[idx] != nil {
					processData(inner, prevMsgs[idx].Data, m, -1)
				}
			}
		case JobEvent:
			canSnap = true
			var job Job
			if err = json.Unmarshal(data[1:], &job); err != nil {
				t.logger.Errorf("node %s unmarshall job data first byte %d error: %v", s.Name(), data[0], err)
				return cnt, false
			}
			// ignore if state is nil, the system will start tracking from a snapshot state
			if *inner != nil {
				if cnt >= 0 {
					t.lock.Lock()
					defer t.lock.Unlock()
				}

				if job.Status == Done {
					delete((*inner).Jobs, job.Id)
				} else {
					(*inner).Jobs[job.Id] = job
					if job.Status == Idle {
						(*inner).LastJobID = job.Id
						(*inner).LastMsgSeq = job.StartSeq

						// update prev job status
						prevID := job.Id - 1
						if prevJob, ok := (*inner).Jobs[prevID]; ok {
							t.logger.Noticef("node %s Idle Job %d, rotate prev job %d from %v to Build, end seq at %d",
								s.Name(), job.Id, prevID, prevJob.Status, job.StartSeq)
							if prevJob.Status == Active && prevJob.EndSeq == 0 {
								prevJob.Status = Build
								prevJob.EndSeq = job.StartSeq
							}
						}
					}
				}

				// snapshot state after every 6 event messages
				cnt = (cnt + 1) % snapshotSize
			} else {
				t.logger.Errorf("node %s replays event data at startup, skips %d event message, no snapshot state yet", s.Name(), seq)
			}
		default:
			t.logger.Errorf("node %s unknown event data marker byte %d error: %v", s.Name(), data[0], err)
		}

		return cnt, canSnap
	}

	handle := func(inner **StateMatchine, msg *nats.Msg, seq uint64, cnt int8) (int8, []byte) {
		msg.Term()
		meta, err := msg.Metadata()
		if err != nil {
			t.logger.Errorf("node %s skips %dth event message due to metadata() err: %v", s.Name(), seq, err)
			return cnt, nil
		} else if meta.Sequence.Stream != seq {
			t.logger.Errorf("node %s skips %dth event message due to expected seq %d mismatch", s.Name(), seq, meta.Sequence.Stream)
			return cnt, nil
		}

		data := msg.Data
		if len(data) == 0 {
			t.logger.Warnf("node %s skips %dth event message due to msg data is empty", s.Name(), seq)
			return cnt, nil
		}

		t.logger.Noticef("node %s processes %dth event msg with type %d", s.Name(), seq, data[0])

		canSnap := false
		cnt, canSnap = processData(inner, data, seq, cnt)

		if canSnap {
			prevMsgs[seq%uint64(len(prevMsgs))] = msg
			if cnt == 0 {
				(*inner).LastEventSeq = seq
				t.logger.Noticef("node %s snapshot state %v", s.Name(), *inner)
				if data, err = json.Marshal(*inner); err == nil {
					return cnt, data
				} else {
					t.logger.Errorf("node %s marshall state machine err: %v", s.Name(), err)
				}
			}
		} else {
			prevMsgs[seq%uint64(len(prevMsgs))] = nil
		}

		return cnt, nil
	}

	var (
		startSeq uint64
		entry    nats.KeyValueEntry
	)
	for err = os.ErrInvalid; err != nil; {
		if entry, err = t.kvs.Get(stateSnapshot); err == nil {
			startSeq = binary.LittleEndian.Uint64(entry.Value())
			break
		}
		time.Sleep(time.Second)
	}

	// get state
	pullOpts := []nats.SubOpt{
		nats.StartSequence(startSeq),
		nats.BindStream(t.eventStream),
		nats.Context(ctx),
	}

	var sub *nats.Subscription
	for err = os.ErrInvalid; err != nil; {
		// ephemeral event stream subscriber
		sub, err = t.jsc.SubscribeSync(t.eventStream, pullOpts...)
		if err == nil {
			break
		}
		t.logger.Errorf("node %s create event stream ephemral subscriber err: %v", s.Name(), err)
		time.Sleep(time.Second)
	}

	t.logger.Noticef("node %s pulls msg [%d, %d] from event stream", s.Name(), startSeq, eventStreamInfo.State.LastSeq)
	for seq := startSeq; seq <= eventStreamInfo.State.LastSeq; seq++ {
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			if err == ctx.Err() {
				t.logger.Errorf("node %s exits init event loop with err: %v", s.Name(), err)
				return nil, err
			} else if err != context.DeadlineExceeded {
				// note: nats uses internal TTL
				t.logger.Warnf("node %s init event loop fetch data err: %v", s.Name(), err)
			}

			seq--
			continue
		}

		handle(&state, msg, seq, -1)
	}

	go func(lastSeq uint64) {
		defer func() {
			if r := recover(); r != nil {
				t.logger.Errorf("error %v", r)
				debug.PrintStack()
			}
			t.logger.Warnf("node %s event loop exits", s.Name())

			sub.Unsubscribe()
		}()

		var (
			rollingCount = int8(1)
			data         []byte
		)

		for lastSeq++; true; lastSeq++ {
			msg, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				if err == ctx.Err() {
					t.logger.Errorf("node %s exits event loop err: %v", s.Name(), err)
					break
				} else if err != context.DeadlineExceeded {
					// note: nats uses internal TTL
					t.logger.Warnf("node %s event loop fetch data err: %v", s.Name(), err)
				}
				lastSeq--
				continue
			}

			if rollingCount, data = handle(&state, msg, lastSeq, rollingCount); len(data) > 0 {
				// post snapshot state machine
				if _, err := t.jsc.Publish(t.eventStream, append([]byte{byte(SnapshotEvent)}, data...), nats.Context(ctx)); err != nil {
					t.logger.Errorf("node %s publishs snapshot state machine err: %v", s.Name(), err)
				} else {
					t.logger.Noticef("node %s saves snapshot state machine last event seq = %v to kv store", s.Name(), state.LastEventSeq)
					data = make([]byte, 8)
					binary.LittleEndian.PutUint64(data, state.LastEventSeq)
					if _, err = t.kvs.Put(stateSnapshot, data); err != nil {
						t.logger.Errorf("node %s put snapshot state machine kv ack %d err: %v", s.Name(), state.LastEventSeq, err)
					}
				}
			}
		}
	}(eventStreamInfo.State.LastSeq)

	t.logger.Noticef("node %s starts up snapshot state is %v", s.Name(), *state)

	return &state, nil
}
