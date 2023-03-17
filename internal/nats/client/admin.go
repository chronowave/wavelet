package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/chronowave/codec"
	"github.com/chronowave/wavelet/internal/common"
	"github.com/chronowave/wavelet/internal/nats/client/terminal"
	"github.com/chronowave/wavelet/internal/nats/client/utils"
	"github.com/chronowave/wavelet/internal/nats/def"
)

const (
	terminalStream = "terminal_stream"
)

type Admin struct {
	s             *server.Server
	nc            *nats.Conn
	jsc           nats.JetStreamContext
	nodeIP        string
	logger        server.Logger
	wasLeader     bool
	lock          sync.Mutex
	done          chan struct{}
	file          common.ConsistentFileStore
	cluster       common.ClusterInfo
	subscriptions []*nats.Subscription
	terminals     map[string]*terminal.Terminal
}

func NewLocalAdmin(
	s *server.Server,
	nodeIP string,
	file common.ConsistentFileStore,
	cluster common.ClusterInfo,
) (*Admin, error) {
	admin := &Admin{
		s:         s,
		nodeIP:    nodeIP,
		logger:    s.Logger(),
		wasLeader: false,
		file:      file,
		cluster:   cluster,
		terminals: map[string]*terminal.Terminal{},
	}

	var err error
	admin.nc, admin.jsc, err = utils.ConnectToLocalServer(s)
	if err != nil {
		return nil, err
	}

	//
	topic := server.JSAdvisoryStreamLeaderElectedPre + "." + terminalStream + ".>"
	sub, err := admin.nc.Subscribe(topic, func(msg *nats.Msg) {
		var jslea server.JSStreamLeaderElectedAdvisory
		if err := json.Unmarshal(msg.Data, &jslea); err != nil {
			admin.logger.Errorf("node %s unmarshal JSStreamLeaderElectedAdvisory unexpected error: %v", s.Name(), err)
			return
		}
		admin.logger.Noticef("node %s received JSStreamLeaderElectedAdvisory leader is %s", s.Name(), jslea.Leader)
		admin.onChannelStreamLeaderElected(strings.Compare(s.Name(), jslea.Leader) == 0)
	})
	if err != nil {
		admin.logger.Errorf("node %s failed to sub terminal stream leader elected: %v", s.Name(), err)
		return nil, err
	}
	admin.subscriptions = append(admin.subscriptions, sub)
	admin.logger.Noticef("node %s started subscription on topic: %s", s.Name(), topic)

	for _, prefix := range []string{server.JSAdvisoryConsumerCreatedPre, server.JSAdvisoryConsumerDeletedPre} {
		topic = prefix + "." + terminalStream + ".>"
		sub, err = admin.nc.Subscribe(topic, admin.onChannelStreamConsumerMsg)
		if err != nil {
			admin.logger.Errorf("node %s failed to sub terminal stream consumer %s: %v", s.Name(), prefix, err)
			return nil, err
		}

		admin.logger.Noticef("node %s started subscription on topic: %s", s.Name(), topic)
		admin.subscriptions = append(admin.subscriptions, sub)
	}

	// meta leader
	if s.JetStreamIsLeader() {
		admin.bootstrapAsMetaLeader(s)
	} else {
		admin.bootstrapAsMetaFollower(s)
	}

	return admin, nil
}

// start internal.common.FlightActionHandler

// on successful, return newly updated schema
// upon failure, return existing schema and revision with non nil error
func (a *Admin) UpdateFlightSchema(flight string, prev uint64, schema *codec.Schema) (*codec.Schema, error) {
	term, ok := a.terminals[flight]
	if !ok {
		a.logger.Errorf("node %s admin client doesn't have flight %s", a.s.Name(), flight)
		return nil, os.ErrInvalid
	}

	err := term.UpdateSchema(schema, prev)
	if err != nil {
		a.logger.Errorf("node %s admin client failed to update flight %s schema: %v", a.s.Name(), flight, err)
	}
	return schema, err
}

func (a *Admin) ListFlights() map[string]*codec.Schema {
	flights := make(map[string]*codec.Schema, len(a.terminals))
	for k, v := range a.terminals {
		flights[k] = v.GetSchema()
	}

	return flights
}

func (a *Admin) GetFlightSchema(flight string) (*codec.Schema, error) {
	term, ok := a.terminals[flight]
	if !ok {
		a.logger.Errorf("node %s admin client doesn't have flight %s, flights are %v", a.s.Name(), flight, a.terminals)
		return nil, os.ErrNotExist
	}

	return term.GetSchema(), nil
}

func (a *Admin) CreateFlight(flight string, schema *codec.Schema) error {
	waitForFlightToBeReady := func() {
		for {
			_, err := a.GetFlightSchema(flight)
			if err == nil {
				break
			}
			a.logger.Noticef("node %s wait for flight %s to be ready: %v", a.s.Name(), flight, err)
			time.Sleep(time.Second)
		}
		a.logger.Noticef("node %s flight %s is ready", a.s.Name(), flight)
	}

	// making sure there is no existing consumer
	for {
		_, err := a.jsc.ConsumerInfo(terminalStream, flight, nats.MaxWait(time.Second))
		if err == nil {
			a.logger.Noticef("node %s skips setup existing flight %s", a.s.Name(), flight)
			waitForFlightToBeReady()
			return nil
		} else if err != nats.ErrTimeout {
			break
		}
	}

	config := &nats.ConsumerConfig{
		Durable:   flight,
		AckPolicy: nats.AckExplicitPolicy,
	}

	// creating terminal on local admin node
	if err := terminal.Create(flight, schema, a.s, a.jsc); err != nil {
		a.logger.Noticef("node %s creates flight %s terminal err: %v", a.s.Name(), flight, err)
		return err
	}

	// TODO: start flight locally

	for {
		_, err := a.jsc.AddConsumer(terminalStream, config)
		if err == nil {
			break
		}
		a.logger.Errorf("node %s will retry to add durable consumer to terminal for flight %s due to %v",
			a.s.Name(), flight, err)
		_ = a.jsc.DeleteConsumer(terminalStream, flight)
		time.Sleep(time.Second)
	}

	waitForFlightToBeReady()
	return nil
}

func (a *Admin) DeleteFlight(flight string) error {
	var err error
	for retries := 0; retries < 3; retries++ {
		err := a.jsc.DeleteConsumer(terminalStream, flight)
		if err == nil || errors.Is(err, nats.ErrConsumerNotFound) {
			return nil
		}
		a.logger.Errorf("will retry to delete flight %s from terminal stream consumers: %v", flight, err)
		time.Sleep(500 * time.Millisecond)
	}

	return err
}

func (a *Admin) PutFlightData(flight string, json []byte) (*codec.PutResult, error) {
	term, ok := a.terminals[flight]
	if !ok {
		a.logger.Errorf("node %s doesn't have flight %s", a.s.Name(), flight)
		return nil, os.ErrExist
	}

	data, err := term.ParseJson(json)
	if err != nil {
		return nil, err
	}

	futures, errs := make([]nats.PubAckFuture, len(data)), make([]string, len(data))

	subj := fmt.Sprintf(def.DataStreamTemplate, flight)
	for i := range data {
		if futures[i], err = a.jsc.PublishAsync(subj, data[i]); err != nil {
			errs[i] = err.Error()
		}
	}

	<-a.jsc.PublishAsyncComplete()

	result := codec.PutResult{
		Seq:   make([]uint64, len(data)),
		Error: errs,
	}

	for i := range data {
		if len(errs[i]) == 0 {
			select {
			case ack := <-futures[i].Ok():
				result.Seq[i] = ack.Sequence
			case err = <-futures[i].Err():
				result.Error[i] = err.Error()
				a.logger.Errorf("get ack for %dth data: %v", i, err)
			}
		} else {
			a.logger.Errorf("failed to publish ack for %dth data: %v", i, errs[i])
		}
	}

	return &result, nil
}

func (a *Admin) SerializeTransientColumnEntity(flight string) (uint64, []byte) {
	term, ok := a.terminals[flight]
	if !ok {
		a.logger.Warnf("node %s doesn't have flight %s, skip building transient index", a.s.Name(), flight)
		return 0, []byte{}
	}

	return term.SerializeTransientColumnEntity()
}

// end internal.common.FlightActionHandler

func (a *Admin) Close() {
	for _, s := range a.subscriptions {
		_ = s.Unsubscribe()
	}

	if a.nc != nil {
		a.nc.Close()
	}
}

func (a *Admin) bootstrapAsMetaLeader(s *server.Server) {
	a.logger.Noticef("node %s bootstraps as meta leader", s.Name())

	replicas := 1
	if s.JetStreamIsClustered() {
		replicas = 3
	}

	// create terminal stream, as queue sub
	utils.CreateStream(s, a.jsc, &nats.StreamConfig{
		Name:     terminalStream,
		Subjects: []string{terminalStream},
		Replicas: replicas,
	}, a.logger)

	a.onChannelStreamLeaderElected(s.GlobalAccount().JetStreamIsStreamLeader(terminalStream))
}

func (a *Admin) bootstrapAsMetaFollower(s *server.Server) {
	a.logger.Noticef("node %s bootstraps as meta followers", s.Name())
	utils.WaitForStream(s, a.jsc, terminalStream, a.logger)
	a.onChannelStreamLeaderElected(s.GlobalAccount().JetStreamIsStreamLeader(terminalStream))
}

func (a *Admin) onChannelStreamLeaderElected(isLeader bool) {
	a.lock.Lock()
	defer a.lock.Unlock()

	if isLeader {
		if a.wasLeader {
			a.logger.Noticef("node %s skips leader bootstrap, was leader", a.s.Name())
			return
		}

		// start
		if a.done != nil {
			close(a.done)
		}
		a.done = make(chan struct{})
		go a.startAsChannelStreamLeader()
	} else if a.wasLeader {
		// stop
		if a.done != nil {
			close(a.done)
		}
	}

	a.wasLeader = isLeader

	for {
		numOfConsumers := 0

		// list for consumers
		for name := range a.jsc.ConsumerNames(terminalStream) {
			numOfConsumers++
			if _, ok := a.terminals[name]; ok {
				a.logger.Noticef("node %s terminal %s is running", a.s.Name(), name)
			} else if flight, err := terminal.Start(name, a.s, a.nodeIP, a.file, a.cluster); err == nil {
				a.terminals[name] = flight
				a.logger.Noticef("node %s started flight %s", a.s.Name(), name)
			} else {
				a.logger.Errorf("node %s failed to start flight %s: %v", a.s.Name(), name, err)
			}
		}

		if info, err := a.jsc.StreamInfo(terminalStream); err != nil {
			a.logger.Warnf("node %s failed to get terminal stream info: %v", a.s.Name(), err)
			continue
		} else if info.State.Consumers > numOfConsumers {
			a.logger.Warnf("node %s stream consumers %d is greater than listed consumers %d", a.s.Name(), info.State.Consumers, numOfConsumers)
			continue
		}
		break
	}

	a.logger.Noticef("node %s is done with bootstraping", a.s.Name())
}

func (a *Admin) onChannelStreamConsumerMsg(msg *nats.Msg) {
	var jscaa server.JSConsumerActionAdvisory
	if err := json.Unmarshal(msg.Data, &jscaa); err != nil {
		a.logger.Errorf("node %s unmarshal JSStreamLeaderElectedAdvisory unexpected error: %v", a.s.Name(), err)
		return
	}
	a.lock.Lock()
	defer a.lock.Unlock()

	switch jscaa.Action {
	case server.CreateEvent:
		if _, ok := a.terminals[jscaa.Consumer]; ok {
			a.logger.Noticef("node %s terminal had already been started", a.s.Name(), jscaa.Consumer)
		} else if flight, err := terminal.Start(jscaa.Consumer, a.s, a.nodeIP, a.file, a.cluster); err == nil {
			a.terminals[jscaa.Consumer] = flight
			a.logger.Noticef("node %s terminal has started flight %s", a.s.Name(), jscaa.Consumer)
		} else {
			a.logger.Errorf("node %s failed to start flight %s: %v", a.s.Name(), jscaa.Consumer, err)
		}
	case server.DeleteEvent:
		if flight, ok := a.terminals[jscaa.Consumer]; ok {
			flight.Delete()
			a.logger.Noticef("node %s terminal has stopped flight %s", a.s.Name(), jscaa.Consumer)
		} else {
			a.logger.Warnf("node %s flight %s had been stopped", a.s.Name(), jscaa.Consumer)
		}

		delete(a.terminals, jscaa.Consumer)
	}
}

func (a *Admin) startAsChannelStreamLeader() {
	defer func() {
		a.logger.Noticef("node %s stops as terminal stream leader", a.s.Name())
	}()

	a.logger.Noticef("node %s starts as terminal stream leader", a.s.Name())

	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ticker.C:
			// TODO: check
		case <-a.done:
			return
		}
	}
}
