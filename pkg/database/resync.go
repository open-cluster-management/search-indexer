// Copyright Contributors to the Open Cluster Management project

package database

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/stolostron/search-indexer/pkg/metrics"
	"github.com/stolostron/search-indexer/pkg/model"
	"k8s.io/klog/v2"
)

// Reset data for the cluster to the incoming state.
func (dao *DAO) ResyncData(ctx context.Context, event model.SyncEvent,
	clusterName string, syncResponse *model.SyncResponse, body []byte) error {

	defer metrics.SlowLog(fmt.Sprintf("Slow resync from %12s. RequestId: %d", clusterName, event.RequestId), 0)()
	klog.Infof(
		"Starting resync from %12s. This is normal, but it could be a problem if it happens often.", clusterName)

	// Reset resources
	err := dao.resetResources(ctx, clusterName, syncResponse, body)
	if err != nil {
		klog.Warningf("Error resyncing resources for cluster %12s. Error: %+v", clusterName, err)
		return err
	}

	// Reset edges
	err = dao.resetEdges(ctx, clusterName, syncResponse, body)
	if err != nil {
		klog.Warningf("Error resyncing edges for cluster %12s. Error: %+v", clusterName, err)
		return err
	}

	klog.V(1).Infof("Completed resync of cluster %12s.\t RequestId: %d", clusterName, event.RequestId)
	return nil
}

func decodeAddResources(body []byte, key string, clusterName string, batch batchWithRetry, syncResponse *model.SyncResponse) (int, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	count := 0

	// read opening {
	if _, err := dec.Token(); err != nil {
		return count, fmt.Errorf("error decoding SyncEvent token: %v", err)
	}
	for {
		// read tokens until we get to addResources
		field, err := dec.Token()
		if err == io.EOF {
			break
		}
		if field == key {
			// read opening [
			if _, err = dec.Token(); err != nil {
				return count, fmt.Errorf("error reading resource opening token: %v", err)
			}
			for dec.More() {
				var resource model.Resource
				if err = dec.Decode(&resource); err != nil {
					return count, fmt.Errorf("error decoding resource from request: %v", err)
				}
				// insert
				props, _ := json.Marshal(resource.Properties)
				query, params, err := useGoqu(
					"INSERT into search.resources values($1,$2,$3) ON CONFLICT (uid) DO NOTHING",
					[]interface{}{resource.UID, clusterName, string(props)})
				if err == nil {
					queueErr := batch.Queue(batchItem{
						action: "addResource",
						query:  query,
						uid:    resource.UID,
						args:   params,
					})
					if queueErr != nil {
						klog.Warningf("Error queuing resources to add. Error: %+v", queueErr)
						// TODO: return queueErr
					}
					syncResponse.TotalAdded++
				}
				count++
			}
			return count, nil
		}
	}

	return count, nil
}

func decodeAddEdges(body []byte, key string) ([]model.Edge, error) {
	dest := make([]model.Edge, 0)
	dec := json.NewDecoder(bytes.NewReader(body))

	// read opening {
	if _, err := dec.Token(); err != nil {
		return dest, fmt.Errorf("error decoding SyncEvent token: %v", err)
	}
	for {
		// read tokens until we ge to addEdges
		field, err := dec.Token()
		if err == io.EOF {
			break
		}
		if field == key {
			// read opening [
			if _, err := dec.Token(); err != nil {
				fmt.Println("err here: ", err.Error())
			}
			for dec.More() {
				var edge model.Edge
				if err = dec.Decode(&edge); err != nil {
					return dest, fmt.Errorf("error decoding edge from request: %v", err)
				}
				dest = append(dest, edge)
			}
			return dest, nil
		}
	}
	return dest, nil
}

// Reset Resources.
//  1. Delete existing resources for cluster
//  2. Delete existing edges for cluster
//  3. Iterate over request []byte and insert resource into database
func (dao *DAO) resetResources(ctx context.Context, clusterName string,
	syncResponse *model.SyncResponse, body []byte) error {
	timer := time.Now()

	batch := NewBatchWithRetry(ctx, dao, syncResponse)

	// Delete existing resources (UID and data) for the cluster.
	query, params, err := useGoqu(
		"DELETE FROM search.resources WHERE cluster=$1 AND uid!='cluster__$1'",
		[]interface{}{clusterName})
	if err == nil {
		_, err = dao.pool.Query(ctx, query, params...)
		if err != nil {
			klog.Warningf("Error deleting existing resources for cluster %12s. Error: %+v", clusterName, err)
			return err
		}
	}

	// Delete existing edges for the cluster
	query, params, err = useGoqu(
		"DELETE FROM search.edges WHERE cluster=$1 AND edgetype!='intercluster'",
		[]interface{}{clusterName})
	if err == nil {
		_, err = dao.pool.Query(ctx, query, params...)
		if err != nil {
			klog.Warningf("Error deleting existing edges for cluster %12s. Error: %+v", clusterName, err)
			return err
		}
	}
	metrics.LogStepDuration(&timer, clusterName, "QUERY existing resources.")

	// Insert new resources into the cluster
	countResources, err := decodeAddResources(body, "addResources", clusterName, batch, syncResponse)
	if err != nil {
		return err
	}
	metrics.RequestSize.Observe(float64(countResources))

	batch.flush()
	batch.wg.Wait()
	syncResponse.TotalAdded = countResources
	// TODO: ensure we have all stats
	//syncResponse.TotalDeleted = len(resourcesToDelete)
	//syncResponse.TotalUpdated = len(resourcesToUpdate)
	//metrics.LogStepDuration(&timer, clusterName,
	//	fmt.Sprintf("Reset resources stats: UNCHANGED [%d] INSERT [%d] UPDATE [%d] DELETE [%d]",
	//		countResources-len(incomingResMap)-len(resourcesToUpdate),
	//		syncResponse.TotalAdded, syncResponse.TotalUpdated, syncResponse.TotalDeleted))

	return batch.connError
}

// Reset Edges
//  1. Get existing edges for the cluster. Excluding intercluster edges.
//  2. For each incoming edge, INSERT if it doesn't exist.
//  3. Delete any existing edges that aren't in the incoming sync event.
func (dao *DAO) resetEdges(ctx context.Context, clusterName string,
	syncResponse *model.SyncResponse, body []byte) error {
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

	edges, err := decodeAddEdges(body, "addEdges")
	if err != nil {
		klog.Errorf("Error decoding edges: %v", err)
		return err
	}
	for _, edge := range edges {
		// add edges
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

	batch.flush()
	batch.wg.Wait()
	metrics.LogStepDuration(&timer, clusterName, fmt.Sprintf("Reset edges stats: INSERT [%d] DELETE [%d]",
		syncResponse.TotalEdgesAdded, syncResponse.TotalEdgesDeleted))
	return batch.connError
}
