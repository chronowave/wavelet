package utils

import (
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const RetrySecond = 1

func ConnectToLocalServer(s *server.Server) (*nats.Conn, nats.JetStreamContext, error) {
	logger := s.Logger()
	conn, err := nats.Connect(s.ClientURL())
	if err != nil {
		logger.Errorf("node %s failed to connect to local NATS server: %v", s.Name(), err)
		return nil, nil, err
	}

	var jsc nats.JetStreamContext
	for jsc == nil {
		if jsc, err = conn.JetStream(nats.MaxWait(RetrySecond * time.Second)); err != nil {
			logger.Warnf("node %s retry in %d seconds, JetStream cluster is not ready: %v", s.Name(), RetrySecond, err)
		}
	}

	return conn, jsc, err
}

func CreateOrGetKeyValueStore(jsc nats.JetStreamContext, conf *nats.KeyValueConfig, logger server.Logger) (nats.KeyValue, error) {
	var (
		err error
		kv  nats.KeyValue
	)

	for {
		if kv, err = jsc.KeyValue(conf.Bucket); err == nil && kv != nil {
			logger.Noticef("bind to an existing Key Value store %s", conf.Bucket)
			break
		}
		logger.Warnf("there is no existing KeyValue store %s, err: %v", conf.Bucket, err)
		if kv, err = jsc.CreateKeyValue(conf); err == nil && kv != nil {
			logger.Noticef("create a new Key Value store %s", conf.Bucket)
			break
		}

		logger.Warnf("retry in %d seconds, KeyValue store %s creation has err: %v", RetrySecond, conf.Bucket, err)
		time.Sleep(RetrySecond * time.Second)
	}

	return kv, err
}

func CreateStream(s *server.Server, jsc nats.JetStreamContext, conf *nats.StreamConfig, logger server.Logger) bool {
	created := false
	for {
		info, err := jsc.StreamInfo(conf.Name)
		if err == nil {
			logger.Noticef("node %s stream %s already exists: %v", s.Name(), conf.Name, *info)
			break
		} else if err == nats.ErrStreamNotFound {
			logger.Noticef("node %s get stream %s with err: %v, try to create stream", s.Name(), conf.Name, err)

			info, err = jsc.AddStream(conf)
			if err == nil {
				logger.Noticef("node %s created stream %v", s.Name(), *info)
				created = true
				break
			}
			logger.Errorf("node %s create stream %s with error: %v", s.Name(), conf.Name, err)
			if err = jsc.DeleteStream(conf.Name); err != nil {
				logger.Errorf("node %s delete stream %s with error: %v", s.Name(), conf.Name, err)
			}
		} else {
			logger.Noticef("node %s will retry to get info for stream %s due to err: %v", s.Name(), conf.Name, err)
		}

		time.Sleep(RetrySecond * time.Second)
	}

	return created
}

func WaitForStream(s *server.Server, jsc nats.JetStreamContext, streamName string, logger server.Logger) *nats.StreamInfo {
	for {
		info, err := jsc.StreamInfo(streamName)
		if err == nil {
			return info
		}

		logger.Warnf("node %s retry in %d seconds, failed to get %s stream info: %v", s.Name(), RetrySecond, streamName, err)
		time.Sleep(RetrySecond * time.Second)
	}
}

func WaitForKeyValue(s *server.Server, jsc nats.JetStreamContext, bucketName string, logger server.Logger) nats.KeyValue {
	for {
		kv, err := jsc.KeyValue(bucketName)
		if err == nil {
			return kv
		}

		logger.Warnf("node %s retry in %d seconds, bind KeyValue store bucket %s has err: %v", s.Name(), RetrySecond, bucketName, err)
		time.Sleep(RetrySecond * time.Second)
	}
}
