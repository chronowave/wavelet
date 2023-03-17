package utils

import (
	"net"
	"strconv"
	"strings"

	"github.com/dgryski/go-jump"
)

func SelectConsistentHosts(key uint64, hosts []string, replicas int) []string {
	ith := int(jump.Hash(key, len(hosts)))
	if replicas > len(hosts) {
		replicas = len(hosts)
	}

	ips := make([]string, replicas)

	for i := range ips {
		ips[i] = hosts[(ith+i)%len(hosts)]
	}

	return ips
}

func BuildTonicFlightAddress(hosts []string, port uint) []string {
	return build("http://", hosts, port)
}

func BuildFlightAddress(hosts []string, port uint) []string {
	return build("", hosts, port)
}

func build(schema string, hosts []string, port uint) []string {
	peers := make([]string, len(hosts))
	for i, h := range hosts {
		if strings.Index(h, ":") > 0 {
			peers[i] = schema + h
		} else {
			peers[i] = schema + net.JoinHostPort(h, strconv.FormatUint(uint64(port), 10))
		}
	}

	return peers
}
