package kube

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/chronowave/wavelet/internal/common"
)

type clusterInfo struct {
	ips   []string
	local int
	lock  sync.Mutex
}

func NewClusterInfo(stopper chan struct{}) (common.ClusterInfo, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		kubeconfigPath := os.Getenv("KUBECONFIG")
		if kubeconfigPath == "" {
			kubeconfigPath = os.Getenv("HOME") + "/.kube/config"
			klog.Infof("no KUBECONFIG env, use %s", kubeconfigPath)
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			klog.Errorf("parse kube config %s err: %v", kubeconfigPath, err)
			return nil, err
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Errorf("create kubernetes clientset err: %v", err)
		return nil, err
	}

	ns := podNamespace()
	statefulSet := clientset.AppsV1().StatefulSets(ns)

	ctx := context.Background()

	ss, err := statefulSet.Get(ctx, common.AppName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("get kubernetes statefulset err: %v", err)
		return nil, err
	}

	replicas := int32(1)
	if ss.Spec.Replicas != nil {
		replicas = *ss.Spec.Replicas
	}

	// TODO: make here safe
	info := clusterInfo{}
	info.ips, err = getClusterNodesPodName(ctx, clientset, ns, replicas)
	if err != nil {
		klog.Errorf("get kubernetes cluster nodes pod name err: %v", err)
		return nil, err
	}

	podName := os.Getenv("POD_NAME")
	for i, ip := range info.ips {
		if strings.HasPrefix(ip, podName) {
			info.local = i
		}
	}

	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0, informers.WithNamespace(ns))
	statefulsetInformer := factory.Apps().V1().StatefulSets().Informer()

	statefulsetInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, newObj interface{}) {
			if _, ok := newObj.(*appsv1.StatefulSet); ok {
				if ss.Spec.Replicas != nil {
					replicas = *ss.Spec.Replicas
				}
				if ips, err := getClusterNodesPodName(context.Background(), clientset, ns, replicas); err == nil {
					info.updateNodesIP(ips)
				}
			}
		},
	})

	go statefulsetInformer.Run(stopper)

	if !cache.WaitForCacheSync(stopper, statefulsetInformer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
	}

	return &info, err
}

func (c *clusterInfo) GetHostsAndLocal() ([]string, int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	clone := make([]string, len(c.ips))
	copy(clone, c.ips)

	return clone, c.local
}

func (c *clusterInfo) updateNodesIP(ips []string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.ips = ips
}

func getClusterNodesPodName(ctx context.Context, clientset *kubernetes.Clientset, ns string, replicas int32) ([]string, error) {
	ips := make([]string, replicas)
	for notready := true; notready; {
		notready = false
		list, err := clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=%s", common.AppName),
		})

		if err != nil {
			klog.Errorf("list pods err: %v", err)
			return nil, err
		}

		if int(replicas) != len(list.Items) {
			notready = true
			continue
		}

		for _, pod := range list.Items {
			if len(pod.Status.PodIP) > 0 {
				names := strings.Split(pod.Name, "-")
				if len(names) <= 1 {
					klog.Infof("stateful set pod name pattern doesn't match name-cnt, %s", pod.Name)
					notready = true
				} else if ith, err := strconv.ParseInt(names[len(names)-1], 10, 32); err != nil {
					klog.Infof("stateful set pod name pattern doesn't match name-cnt, %s", pod.Name)
					notready = true
				} else {
					ips[ith] = pod.Name
					if len(pod.Spec.Subdomain) > 0 {
						ips[ith] += "." + pod.Spec.Subdomain
					}
				}
			} else {
				notready = true
			}
		}
		klog.Infof("list pods returns partial data %v", ips)
	}
	klog.Infof("pods ips: %v", ips)

	return ips, nil
}

func podNamespace() string {
	if pns := os.Getenv("POD_NAMESPACE"); len(pns) != 0 {
		return pns
	}
	return "default"
}
