package client

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/chronowave/codec"
	"github.com/chronowave/wavelet/internal/store"
	"github.com/chronowave/wavelet/internal/test"
)

func TestNewAdminClient(t *testing.T) {
	ips := test.LoopbackIPs()
	jsc := test.RunClusterJetStreamServer(ips)
	defer jsc.Shutdown()

	clients := createAdminClients(t, jsc, ips)
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	raw := `{"$revision": 1, "$unicodes":{"field1":"IQ==","field2":"Ig==","field3":"Iw==","field4":"JA=="},"$markers":{"!":12,"\"":20,"#":3,"#!":12,"#\"":20,"$":3,"$!":12,"$\"":20},"$nextcode":36,"$nextfield":9,"fields":[{"id":0,"unicode":0,"marker":12,"deprecate":false,"name":"field1","type":"UINT8","nullable":false,"children":null},{"id":0,"unicode":0,"marker":20,"deprecate":false,"name":"field2","type":"FLT32","nullable":false,"children":null},{"id":0,"unicode":0,"marker":3,"deprecate":false,"name":"field3","type":"SOO","nullable":false,"children":[{"id":0,"unicode":0,"marker":12,"deprecate":false,"name":"field1","type":"UINT8","nullable":false,"children":null},{"id":0,"unicode":0,"marker":20,"deprecate":false,"name":"field2","type":"FLT32","nullable":false,"children":null}]},{"id":0,"unicode":0,"marker":3,"deprecate":false,"name":"field4","type":"SOO","nullable":false,"children":[{"id":0,"unicode":0,"marker":12,"deprecate":false,"name":"field1","type":"UINT8","nullable":false,"children":null},{"id":0,"unicode":0,"marker":20,"deprecate":false,"name":"field2","type":"FLT32","nullable":false,"children":null}]}]}`
	schema := codec.Schema{}
	err := schema.UnmarshalJSON([]byte(raw))
	if err != nil {
		t.Errorf("error %v", err)
		return
	}

	/*
		if err := clients[0].CreateFlight("testing1", &schema); err != nil {
			t.Errorf("error to create consumer %v", err)
			return
		}
		:w

	*/

	time.Sleep(60 * time.Second)
}

func createAdminClients(t *testing.T, jsc *test.JSClusters, ips []string) []*Admin {
	clients := make([]*Admin, len(jsc.Servers))
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
			clnt, err := NewLocalAdmin(s, ips[ith], fileStore, jsc)
			if err != nil {
				t.Errorf("create client with err %v", err)
				return
			}
			//defer clnt.Close()

			clients[ith] = clnt
			wg.Done()
		}(i, ss)
	}

	wg.Wait()

	return clients
}
