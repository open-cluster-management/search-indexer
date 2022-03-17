// Copyright Contributors to the Open Cluster Management project

package clustersync

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	clusterv1beta1 "github.com/stolostron/multicloud-operators-foundation/pkg/apis/internal.open-cluster-management.io/v1beta1"
	"github.com/stolostron/search-indexer/pkg/config"
	"github.com/stolostron/search-indexer/pkg/database"
	"github.com/stolostron/search-indexer/pkg/model"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	klog "k8s.io/klog/v2"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
)

var dynamicClient dynamic.Interface
var dao database.DAO

const managedClusterGVR = "managedclusters.v1.cluster.open-cluster-management.io"
const managedClusterInfoGVR = "managedclusterinfos.v1beta1.internal.open-cluster-management.io"

func ElectLeaderAndStart(ctx context.Context) {
	client := config.Cfg.KubeClient
	lockName := "search-indexer.open-cluster-management.io"
	podName := config.Cfg.PodName
	podNamespace := config.Cfg.PodNamespace

	lock := getNewLock(client, lockName, podName, podNamespace)
	runLeaderElection(ctx, lock)
}

func syncClusters(ctx context.Context) {
	klog.Info("Attempting to sync clusters.")
	WatchClusters(ctx)
	<-ctx.Done()
	klog.Info("Exit syncClusters.")
}

// Watches ManagedCluster objects and updates the database with a Cluster node.
func WatchClusters(ctx context.Context) {
	klog.Info("Begin ClusterWatch routine")

	dynamicClient = config.GetDynamicClient()
	dynamicFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient,
		time.Duration(config.Cfg.RediscoverRateMS))

	// Create GVR for ManagedCluster and ManagedClusterInfo
	managedClusterGvr, _ := schema.ParseResourceArg(managedClusterGVR)
	managedClusterInfoGvr, _ := schema.ParseResourceArg(managedClusterInfoGVR)

	//Create Informers for ManagedCluster and ManagedClusterInfo
	managedClusterInformer := dynamicFactory.ForResource(*managedClusterGvr).Informer()
	managedClusterInfoInformer := dynamicFactory.ForResource(*managedClusterInfoGvr).Informer()

	// Create handlers for events
	handlers := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			klog.V(4).Info("clusterWatch: AddFunc for ", obj.(*unstructured.Unstructured).GetKind())
			processClusterUpsert(obj)
		},
		UpdateFunc: func(prev interface{}, next interface{}) {
			klog.V(4).Info("clusterWatch: UpdateFunc for", next.(*unstructured.Unstructured).GetKind())
			processClusterUpsert(next)
		},
		// DeleteFunc: func(obj interface{}) {
		// 	klog.Info("clusterWatch: DeleteFunc")
		// 	processClusterDelete(obj)
		// },
	}

	// Add Handlers to both Informers
	managedClusterInformer.AddEventHandler(handlers)
	managedClusterInfoInformer.AddEventHandler(handlers)

	// Periodically check if the ManagedCluster/ManagedClusterInfo resource exists
	go stopAndStartInformer(ctx, "cluster.open-cluster-management.io/v1", managedClusterInformer)
	go stopAndStartInformer(ctx, "internal.open-cluster-management.io/v1beta1", managedClusterInfoInformer)
}

// Stop and Start informer according to Rediscover Rate
func stopAndStartInformer(ctx context.Context, groupVersion string, informer cache.SharedIndexInformer) {
	var stopper chan struct{}
	informerRunning := false

	for {
		select {
		case <-ctx.Done():
			klog.Info("Exit informers for clusterwatch.")
			stopper <- struct{}{}
			return
		default:
			_, err := config.Cfg.KubeClient.ServerResourcesForGroupVersion(groupVersion)
			// we fail to fetch for some reason other than not found
			if err != nil && !isClusterMissing(err) {
				klog.Errorf("Cannot fetch resource list for %s, error message: %s ", groupVersion, err)
			} else {
				if informerRunning && isClusterMissing(err) {
					klog.Infof("Stopping cluster informer routine because %s resource not found.", groupVersion)
					stopper <- struct{}{}
					informerRunning = false
				} else if !informerRunning && !isClusterMissing(err) {
					klog.Infof("Starting cluster informer routine for cluster watch for %s resource", groupVersion)
					stopper = make(chan struct{})
					informerRunning = true
					go informer.Run(stopper)
				}
			}
			time.Sleep(time.Duration(config.Cfg.RediscoverRateMS) * time.Millisecond)
		}
	}
}

var mux sync.Mutex

func processClusterUpsert(obj interface{}) {
	// Lock so only one goroutine at a time can access add a cluster.
	// Helps to eliminate duplicate entries.
	mux.Lock()
	defer mux.Unlock()
	j, err := json.Marshal(obj.(*unstructured.Unstructured))
	if err != nil {
		klog.Warning("Error unmarshalling object from Informer in processClusterUpsert.")
	}

	// We update by name, and the name *should be* the same for a given cluster in either object
	// Objects from a given cluster collide and update rather than duplicate insert
	// Unmarshall either ManagedCluster or ManagedClusterInfo
	// check which object we are using

	var resource model.Resource
	switch obj.(*unstructured.Unstructured).GetKind() {
	case "ManagedCluster":
		managedCluster := clusterv1.ManagedCluster{}
		err = json.Unmarshal(j, &managedCluster)
		if err != nil {
			klog.Warning("Failed to Unmarshal MangedCluster", err)
		}
		resource = transformManagedCluster(&managedCluster)
	case "ManagedClusterInfo":
		managedClusterInfo := clusterv1beta1.ManagedClusterInfo{}
		err = json.Unmarshal(j, &managedClusterInfo)
		if err != nil {
			klog.Warning("Failed to Unmarshal ManagedclusterInfo", err)
		}
		resource = transformManagedClusterInfo(&managedClusterInfo)
	default:
		klog.Warning("ClusterWatch received unknown kind.", obj.(*unstructured.Unstructured).GetKind())
		return
	}

	if (database.DAO{} == dao) {
		dao = database.NewDAO(nil)
	}
	// Upsert (attempt update, attempt insert on failure)
	dao.UpsertCluster(resource)

	// If a cluster is offline we remove the resources from that cluster, but leave the cluster resource object.
	/*if resource.Properties["status"] == "offline" {
		klog.Infof("Cluster %s is offline, removing cluster resources from datastore.", cluster.GetName())
		delClusterResources(cluster)
	}*/

}

func isClusterMissing(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "could not find the requested resource")
}
func addAdditionalProperties(props map[string]interface{}) map[string]interface{} {
	clusterUid := string("cluster__" + props["name"].(string))
	_, ok := database.ExistingClustersMap[clusterUid]
	if ok {
		existingProps, _ := database.ExistingClustersMap[clusterUid].(map[string]interface{})
		for key, val := range existingProps {
			_, present := props[key]
			if !present {
				props[key] = val
			}
		}
	}
	return props
}

// Transform ManagedClusterInfo object into db.Resource suitable for insert into redis
func transformManagedClusterInfo(managedClusterInfo *clusterv1beta1.ManagedClusterInfo) model.Resource {
	// https://github.com/stolostron/multicloud-operators-foundation/
	//    blob/main/pkg/apis/internal.open-cluster-management.io/v1beta1/clusterinfo_types.go

	props := make(map[string]interface{})

	// Get properties from ManagedClusterInfo
	props["consoleURL"] = managedClusterInfo.Status.ConsoleURL
	props["nodes"] = int64(len(managedClusterInfo.Status.NodeList))
	props["kind"] = "Cluster"
	props["name"] = managedClusterInfo.GetName()
	props["_clusterNamespace"] = managedClusterInfo.GetNamespace() // Needed for rbac mapping.
	props["apigroup"] = "internal.open-cluster-management.io"      // Maps rbac to ManagedClusterInfo
	props = addAdditionalProperties(props)
	// Create the resource
	resource := model.Resource{
		Kind:           "Cluster",
		UID:            string("cluster__" + managedClusterInfo.GetName()),
		Properties:     props,
		ResourceString: "managedclusterinfos", // Maps rbac to ManagedClusterInfo.
	}
	return resource
}

// Transform ManagedCluster object into db.Resource suitable for insert into DB
func transformManagedCluster(managedCluster *clusterv1.ManagedCluster) model.Resource {
	// https://github.com/stolostron/api/blob/main/cluster/v1/types.go#L78
	// We use ManagedCluster as the primary source of information
	// Properties duplicated between this and ManagedClusterInfo are taken from ManagedCluster

	props := make(map[string]interface{})
	if managedCluster.GetLabels() != nil {
		// Unmarshaling labels to map[string]interface{}
		var labelMap map[string]interface{}
		clusterLabels, _ := json.Marshal(managedCluster.GetLabels())
		err := json.Unmarshal(clusterLabels, &labelMap)
		if err == nil {
			props["label"] = labelMap
		}
	}

	props["kind"] = "Cluster"
	props["name"] = managedCluster.GetName()                  // must match ManagedClusterInfo
	props["_clusterNamespace"] = managedCluster.GetName()     // maps to the namespace of ManagedClusterInfo
	props["apigroup"] = "internal.open-cluster-management.io" // maps rbac to ManagedClusterInfo
	props["created"] = managedCluster.GetCreationTimestamp().UTC().Format(time.RFC3339)

	cpuCapacity := managedCluster.Status.Capacity["cpu"]
	props["cpu"], _ = cpuCapacity.AsInt64()
	memCapacity := managedCluster.Status.Capacity["memory"]
	props["memory"] = memCapacity.String()
	props["kubernetesVersion"] = managedCluster.Status.Version.Kubernetes

	for _, condition := range managedCluster.Status.Conditions {
		props[condition.Type] = string(condition.Status)
	}
	props = addAdditionalProperties(props)
	resource := model.Resource{
		Kind:           "Cluster",
		UID:            string("cluster__" + managedCluster.GetName()),
		Properties:     props,
		ResourceString: "managedclusterinfos", // Maps rbac to ManagedClusterInfo
	}
	return resource
}
