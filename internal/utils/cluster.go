package utils

import (
	"os"
	"strings"

	"github.com/chronowave/wavelet/internal/common"
)

type envCluster struct {
	urls  []string
	local int
}

// start internal ClusterInfo

func (e *envCluster) GetHostsAndLocal() ([]string, int) {
	return e.urls, e.local
}

// end internal ClusterInfo

func GetClusterInfoFromEnv() (common.ClusterInfo, string) {
	str := os.Getenv("CLUSTER_IPS")
	if len(str) == 0 {
		return nil, ""
	}

	var urls []string
	nodeIP := ""
	for i, ip := range strings.Split(str, ",") {
		if i == 0 {
			nodeIP = strings.Split(ip, ":")[0]
		}
		urls = append(urls, ip)
	}

	if len(urls) == 0 {
		return nil, ""
	}

	return &envCluster{urls, 0}, nodeIP
}
