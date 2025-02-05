// Copyright Contributors to the Open Cluster Management project

package database

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/stolostron/search-indexer/pkg/metrics"
	"github.com/stolostron/search-indexer/pkg/model"
	"k8s.io/klog/v2"
)

// Reset data for the cluster to the incoming state.
func (dao *DAO) ResyncData(ctx context.Context, event model.SyncEvent,
	clusterName string, syncResponse *model.SyncResponse) error {

	defer metrics.SlowLog(fmt.Sprintf("Slow resync from %12s. RequestId: %d", clusterName, event.RequestId), 0)()
	klog.Infof(
		"Starting resync from %12s. This is normal, but it could be a problem if it happens often.", clusterName)

	// Reset resources
	err := dao.resetResources(ctx, event.AddResources, clusterName, syncResponse)
	if err != nil {
		klog.Warningf("Error resyncing resources for cluster %12s. Error: %+v", clusterName, err)
		return err
	}

	// Reset edges
	err = dao.resetEdges(ctx, event.AddEdges, clusterName, syncResponse)
	if err != nil {
		klog.Warningf("Error resyncing edges for cluster %12s. Error: %+v", clusterName, err)
		return err
	}

	klog.V(1).Infof("Completed resync of cluster %12s.\t RequestId: %d", clusterName, event.RequestId)
	return nil
}

// Reset Resources.
// 1. Upsert each incoming resource. Keep the UID.
// 2. Delete existing UIDs that don't match the incoming UIDs.
func (dao *DAO) resetResources(ctx context.Context, resources []model.Resource, clusterName string,
	syncResponse *model.SyncResponse) error {

	batch := NewBatchWithRetry(ctx, dao, syncResponse)

	incomingUIDs := make([]interface{}, 0)

	// UPSERT resources in the database.
	for _, resource := range resources {
		uid := resource.UID
		data, _ := json.Marshal(resource.Properties)
		query, params, err := useGoqu(
			"INSERT into search.resources values($1,$2,$3) ON CONFLICT (uid) DO UPDATE SET data=$3 WHERE data!=$3",
			[]interface{}{uid, clusterName, string(data)})
		if err == nil {
			queueErr := batch.Queue(batchItem{
				action: "addResource",
				query:  query,
				uid:    uid,
				args:   params,
			})
			if queueErr != nil {
				klog.Warningf("Error queuing resources to add. Error: %+v", queueErr)
				return queueErr
			}
			syncResponse.TotalAdded++
		}
		incomingUIDs = append(incomingUIDs, uid)
	}
	batch.flush() // TODO: Remove, this is to debug timing.
	batch.wg.Wait()
	klog.Info("Done with UPSERT resources for ", clusterName)

	// DELETE resources that no longer exist.
	// FIXME: This query is takig too long.
	query, params, err := useGoqu(
		"DELETE from search.resources WHERE cluster=$1 AND uid NOT IN ($2)",
		[]interface{}{clusterName, incomingUIDs})
	if err == nil {
		queueErr := batch.Queue(batchItem{
			action: "deleteResource",
			query:  query,
			uid:    fmt.Sprintf("%s", incomingUIDs),
			args:   params,
		})
		if queueErr != nil {
			klog.Warningf("Error queuing resources for deletion. Error: %+v", queueErr)
		}
	}

	batch.flush() // TODO: Remove, this is to debug timing.
	batch.wg.Wait()
	klog.Info("Done with DELETE resources for ", clusterName)

	// DELETE edges pointing to resources that no longer exist.
	query, _, err = useGoqu(
		"DELETE from search.edges WHERE cluster=$1 AND sourceid NOT IN ($2) OR destid NOT IN ($2)",
		[]interface{}{clusterName, incomingUIDs})
	if err == nil {
		queueErr := batch.Queue(batchItem{
			action: "deleteEdge",
			query:  query,
			uid:    fmt.Sprintf("%s", incomingUIDs),
			args:   params,
		})
		if queueErr != nil {
			klog.Warningf("Error queuing edges for deletion. Error: %+v", queueErr)
		}
	}
	batch.flush()
	batch.wg.Wait()
	klog.Info("Done deleting edges to resources that don't exist for ", clusterName)

	// TODO: These metrics are now harder to generate.
	// Will need to check how these are used in the collector.
	//
	// syncResponse.TotalAdded = len(incomingUIDs)
	// syncResponse.TotalDeleted = len(resourcesToDelete)
	// syncResponse.TotalUpdated = len(resourcesToUpdate)
	// metrics.LogStepDuration(&timer, clusterName,
	// 	fmt.Sprintf("Reset resources stats: UNCHANGED [%d] INSERT [%d] UPDATE [%d] DELETE [%d]",
	// 		len(resources)-len(incomingResMap)-len(resourcesToUpdate),
	// 		syncResponse.TotalAdded, syncResponse.TotalUpdated, syncResponse.TotalDeleted))

	return batch.connError
}

// TODO: Update this function.
// Reset Edges
//  1. Get existing edges for the cluster. Excluding intercluster edges.
//  2. For each incoming edge, INSERT if it doesn't exist.
//  3. Delete any existing edges that aren't in the incoming sync event.
func (dao *DAO) resetEdges(ctx context.Context, edges []model.Edge, clusterName string,
	syncResponse *model.SyncResponse) error {
	timer := time.Now()

	batch := NewBatchWithRetry(ctx, dao, syncResponse)

	var queueErr error
	existingEdgesMap := make(map[string]model.Edge)

	// Get all existing edges for the cluster.
	query, params, err := useGoqu(
		"SELECT sourceid, edgetype, destid FROM search.edges WHERE edgetype!='interCluster' AND cluster=$1",
		[]interface{}{clusterName})
	if err == nil {
		edgeRow, err := dao.pool.Query(ctx, query, params...)
		if err != nil {
			klog.Warningf("Error getting existing edges during resync of cluster %12s. Error: %+v", clusterName, err)
		}

		for edgeRow.Next() {
			edge := model.Edge{}
			err := edgeRow.Scan(&edge.SourceUID, &edge.EdgeType, &edge.DestUID)
			if err != nil {
				klog.Warningf("Error scanning edge row. Error: %+v", err)
				continue
			}
			existingEdgesMap[edge.SourceUID+edge.EdgeType+edge.DestUID] = edge
		}
		edgeRow.Close()
	}
	metrics.LogStepDuration(&timer, clusterName, "Resync QUERY existing edges")

	// Now compare existing edges with the new edges.
	for _, edge := range edges {
		// If the edge already exists, do nothing.
		if _, ok := existingEdgesMap[edge.SourceUID+edge.EdgeType+edge.DestUID]; ok {
			delete(existingEdgesMap, edge.SourceUID+edge.EdgeType+edge.DestUID)
			continue
		}
		// If the edge doesn't exist, add it.
		query, params, err := useGoqu(
			"INSERT into search.edges values($1,$2,$3,$4,$5,$6) ON CONFLICT (sourceid, destid, edgetype) DO NOTHING",
			[]interface{}{edge.SourceUID, edge.SourceKind, edge.DestUID, edge.DestKind, edge.EdgeType, clusterName})
		if err == nil {
			queueErr = batch.Queue(batchItem{
				action: "addEdge",
				query:  query,
				uid:    edge.SourceUID,
				args:   params,
			})
			if queueErr != nil {
				klog.Warningf("Error queuing edges. Error: %+v", queueErr)
				return queueErr
			}
			syncResponse.TotalEdgesAdded++
		}
	}

	// Delete existing edges that are not in the new sync event.
	for _, edge := range existingEdgesMap {
		query, params, err := useGoqu(
			"DELETE from search.edges WHERE sourceid=$1 AND destid=$2 AND edgetype=$3",
			[]interface{}{edge.SourceUID, edge.DestUID, edge.EdgeType})
		if err == nil {
			queueErr = batch.Queue(batchItem{
				action: "deleteEdge",
				query:  query,
				uid:    edge.SourceUID,
				args:   params,
			})
			if queueErr != nil {
				klog.Warningf("Error queuing edges. Error: %+v", queueErr)
				return queueErr
			}
			syncResponse.TotalEdgesDeleted++
		}
	}

	batch.flush()
	batch.wg.Wait()
	metrics.LogStepDuration(&timer, clusterName, fmt.Sprintf("Reset edges stats: INSERT [%d] DELETE [%d]",
		syncResponse.TotalEdgesAdded, syncResponse.TotalEdgesDeleted))
	return batch.connError
}
