package terminal

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/chronowave/wavelet/internal/nats/client"
	"github.com/chronowave/wavelet/internal/nats/client/utils"
	"github.com/chronowave/wavelet/internal/store"
	"github.com/chronowave/wavelet/internal/test"
)

func TestStart(t *testing.T) {
	ips := test.LoopbackIPs()
	jsc := test.RunClusterJetStreamServer(ips)
	defer jsc.Shutdown()

	admins := make([]*client.Admin, len(jsc.Servers))
	var wg sync.WaitGroup
	wg.Add(len(jsc.Servers))
	for i, ss := range jsc.Servers {
		go func(ith int, s *server.Server) {
			path := filepath.Join(s.JetStreamConfig().StoreDir, "cw")
			fileStore, err := store.NewFileStore(path, s.ClusterAddr().IP.String(), 7222, s.Logger())
			if err != nil {
				fmt.Printf("error %v", err)
				runtime.Goexit()
			}
			clnt, err := client.NewLocalAdmin(s, ips[ith], fileStore, jsc)
			if err != nil {
				t.Errorf("create client with err %v", err)
				return
			}
			//defer clnt.Close()

			admins[ith] = clnt
			wg.Done()
		}(i, ss)
	}

	wg.Wait()

	conn, err := nats.Connect(jsc.Servers[0].ClientURL())
	if err != nil {
		t.Errorf("error is %v", err)
		return
	}

	conn.Subscribe(server.JSAdvisoryStreamUpdatedPre+".>", func(msg *nats.Msg) {
		var jsaa server.JSStreamActionAdvisory
		json.Unmarshal(msg.Data, &jsaa)
		t.Logf("stream updated %v", jsaa)
	})
	conn.Subscribe(server.JSAdvisoryStreamRestoreCreatePre+".>", func(msg *nats.Msg) {
		var jsaa server.JSStreamActionAdvisory
		json.Unmarshal(msg.Data, &jsaa)
		t.Logf("stream restore created %v", jsaa)
	})
	conn.Subscribe(server.JSAdvisoryStreamRestoreCompletePre+".>", func(msg *nats.Msg) {
		var jsaa server.JSStreamActionAdvisory
		json.Unmarshal(msg.Data, &jsaa)
		t.Logf("stream restore completed %v", jsaa)
	})
	conn.Subscribe(server.JSAdvisoryStreamCreatedPre+".>", func(msg *nats.Msg) {
		var jsaa server.JSStreamActionAdvisory
		json.Unmarshal(msg.Data, &jsaa)
		t.Logf("stream created %v", jsaa)
	})

	conn.Subscribe(server.JSAdvisoryStreamLeaderElectedPre+".>", func(msg *nats.Msg) {
		var jsaa server.JSStreamActionAdvisory
		json.Unmarshal(msg.Data, &jsaa)
		t.Logf("stream elected %v", jsaa)
	})

	js, _ := conn.JetStream()

	_ = nats.ConsumerInfo{}

	// create job stream, as queue sub
	utils.CreateStream(jsc.Servers[0], js, &nats.StreamConfig{
		Name:      "test_",
		Subjects:  []string{"dd"},
		Retention: nats.WorkQueuePolicy,
		Replicas:  2,
	}, jsc.Servers[0].Logger())

	consumer, err := js.AddConsumer("test_", &nats.ConsumerConfig{
		Durable:   "abc",
		AckPolicy: nats.AckExplicitPolicy,
	})
	if err != nil {
		t.Errorf("failed to create consumer %v, %v", err, consumer)
	}
	time.Sleep(21 * time.Second)
}
