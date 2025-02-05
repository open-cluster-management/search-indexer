// Copyright Contributors to the Open Cluster Management project

package database

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/stolostron/search-indexer/pkg/metrics"
	"github.com/stolostron/search-indexer/pkg/model"
	"k8s.io/klog/v2"
)

func (dao *DAO) SyncData(ctx context.Context, event model.SyncEvent,
	clusterName string, syncResponse *model.SyncResponse, body []byte) error {
	err := json.NewDecoder(bytes.NewReader(body)).Decode(&event)
	if err != nil {
		klog.Errorf("Error decoding request body from cluster [%s]. Error: %+v\n", clusterName, err)
		return err
	}
	resourceTotal := len(event.AddResources) + len(event.UpdateResources) + len(event.DeleteResources)
	metrics.RequestSize.Observe(float64(resourceTotal))

	defer metrics.SlowLog(fmt.Sprintf("Slow Sync from cluster %s.", clusterName), 0)()
	batch := NewBatchWithRetry(ctx, dao, syncResponse)
	var queueErr error

	// ADD RESOURCES
	// In case of conflict update only if data has changed
	for _, resource := range event.AddResources {
		data, _ := json.Marshal(resource.Properties)
		queueErr = batch.Queue(batchItem{
			action: "addResource",
			query: `INSERT into search.resources as r values($1,$2,$3) ON CONFLICT (uid) 
			DO UPDATE SET data=$3 WHERE r.uid=$1 and r.data IS DISTINCT FROM $3`,
			uid:  resource.UID,
			args: []interface{}{resource.UID, clusterName, string(data)},
		})
	}

	// UPDATE RESOURCES
	// The collector enforces that a resource isn't added and updated in the same sync event.
	// The uid and cluster fields will never get updated for a resource.
	for _, resource := range event.UpdateResources {
		data, _ := json.Marshal(resource.Properties)
		queueErr = batch.Queue(batchItem{
			action: "updateResource",
			query:  "UPDATE search.resources SET data=$2 WHERE uid=$1",
			uid:    resource.UID,
			args:   []interface{}{resource.UID, string(data)},
		})
	}

	// DELETE RESOURCES and all edges pointing to the resource.
	if len(event.DeleteResources) > 0 {
		params := make([]string, len(event.DeleteResources))
		uids := make([]interface{}, len(event.DeleteResources))
		for i, resource := range event.DeleteResources {
			params[i] = fmt.Sprintf("$%d", i+1)
			uids[i] = resource.UID
		}
		paramStr := strings.Join(params, ",")

		// TODO: Need better safety for delete errors.
		// The current retry logic won't work well if there's an error here.
		err := batch.Queue(batchItem{
			action: "deleteResource",
			query:  fmt.Sprintf("DELETE from search.resources WHERE uid IN (%s)", paramStr),
			uid:    fmt.Sprintf("%s", uids),
			args:   uids,
		})
		queueErr = batch.Queue(batchItem{
			action: "deleteResource",
			query:  fmt.Sprintf("DELETE from search.edges WHERE sourceId IN (%s) OR destId IN (%s)", paramStr, paramStr),
			uid:    fmt.Sprintf("%s", uids),
			args:   uids,
		})
		if err != nil {
			queueErr = err
		}
	}

	// ADD EDGES
	// Nothing to update in case of conflict as resource kind cannot change
	for _, edge := range event.AddEdges {
		queueErr = batch.Queue(batchItem{
			action: "addEdge",
			query:  "INSERT into search.edges values($1,$2,$3,$4,$5,$6) ON CONFLICT (sourceid, destid, edgetype) DO NOTHING",
			uid:    edge.SourceUID,
			args:   []interface{}{edge.SourceUID, edge.SourceKind, edge.DestUID, edge.DestKind, edge.EdgeType, clusterName}})
	}

	// UPDATE EDGES
	// Edges are never updated. The collector only sends ADD and DELETE eveents for edges.

	// DELETE EDGES
	for _, edge := range event.DeleteEdges {
		queueErr = batch.Queue(batchItem{
			action: "deleteEdge",
			query:  "DELETE from search.edges WHERE sourceId=$1 AND destId=$2 AND edgeType=$3",
			uid:    edge.SourceUID,
			args:   []interface{}{edge.SourceUID, edge.DestUID, edge.EdgeType}})
	}

	// Flush remaining items in the batch.
	batch.flush()

	// Wait for all batches to complete.
	batch.wg.Wait()
	if queueErr != nil {
		klog.V(1).Infof("Completed sync of cluster %12s with errors.", clusterName)
		return queueErr
	}

	// The response fields below are redundant, these are more interesting for resync.
	syncResponse.TotalAdded = len(event.AddResources) - len(syncResponse.AddErrors)
	syncResponse.TotalUpdated = len(event.UpdateResources) - len(syncResponse.UpdateErrors)
	syncResponse.TotalDeleted = len(event.DeleteResources) - len(syncResponse.DeleteErrors)
	syncResponse.TotalEdgesAdded = len(event.AddEdges) - len(syncResponse.AddEdgeErrors)
	syncResponse.TotalEdgesDeleted = len(event.DeleteEdges) - len(syncResponse.DeleteEdgeErrors)

	klog.V(1).Infof("Completed sync of cluster %12s", clusterName)
	return batch.connError
}
