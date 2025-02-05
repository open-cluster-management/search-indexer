// Copyright Contributors to the Open Cluster Management project

package server

import (
	"encoding/json"
	"fmt"
	"github.com/stolostron/search-indexer/pkg/database"
	"io"
	"net/http"
	"time"

	"github.com/stolostron/search-indexer/pkg/metrics"

	"github.com/gorilla/mux"
	"github.com/stolostron/search-indexer/pkg/config"
	"github.com/stolostron/search-indexer/pkg/model"
	"k8s.io/klog/v2"
)

func (s *ServerConfig) SyncResources(w http.ResponseWriter, r *http.Request) {
	database.PrintMem("SyncResources start")
	start := time.Now()
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	clusterName := params["id"]

	// Decode SyncEvent from request body.
	syncEvent, err := decodeSyncEvent(r.Body, clusterName)
	if err != nil {
		klog.Error(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	resourceTotal := len(syncEvent.AddResources) + len(syncEvent.UpdateResources) + len(syncEvent.DeleteResources)
	metrics.RequestSize.Observe(float64(resourceTotal))

	// Initialize SyncResponse object.
	syncResponse := &model.SyncResponse{
		Version:          config.COMPONENT_VERSION,
		RequestId:        syncEvent.RequestId,
		AddErrors:        make([]model.SyncError, 0),
		UpdateErrors:     make([]model.SyncError, 0),
		DeleteErrors:     make([]model.SyncError, 0),
		AddEdgeErrors:    make([]model.SyncError, 0),
		DeleteEdgeErrors: make([]model.SyncError, 0),
	}

	// The collector sends 2 types of requests:
	// 1. ReSync [ClearAll=true]  - It has the complete current state. It must overwrite any previous state.
	// 2. Sync   [ClearAll=false] - This is the delta changes from the previous state.
	if syncEvent.ClearAll {
		err = s.Dao.ResyncData(r.Context(), syncEvent, clusterName, syncResponse)
	} else {
		err = s.Dao.SyncData(r.Context(), syncEvent, clusterName, syncResponse)
	}
	if err != nil {
		klog.Warningf("Responding with error to request from %12s. RequestId: %s  Error: %s",
			clusterName, syncEvent.RequestId, err)
		http.Error(w, "Server error while processing the request.", http.StatusInternalServerError)
		return
	}

	// Get the total cluster resources for validation by the collector.
	totalResources, totalEdges, validateErr := s.Dao.ClusterTotals(r.Context(), clusterName)
	if validateErr != nil {
		klog.Warningf("Responding with error to request from %12s. RequestId: %s  Error: %s",
			clusterName, syncEvent.RequestId, validateErr)
		http.Error(w, "Server error while processing the request.", http.StatusInternalServerError)
		return
	}
	syncResponse.TotalResources = totalResources
	syncResponse.TotalEdges = totalEdges

	// Send Response
	w.WriteHeader(http.StatusOK)
	encodeError := json.NewEncoder(w).Encode(syncResponse)
	if encodeError != nil {
		klog.Error("Error responding to SyncEvent:", encodeError, syncResponse)
		w.WriteHeader(http.StatusInternalServerError)
	}

	// Log request.
	klog.V(5).Infof("Request from [%12s] took [%v] clearAll [%t] addTotal [%d]",
		clusterName, time.Since(start), syncEvent.ClearAll, len(syncEvent.AddResources))
	// klog.V(5).Infof("Response for [%s]: %+v", clusterName, syncResponse)
	database.PrintMem("SyncResources end")
}

func decodeSyncEvent(body io.ReadCloser, clusterName string) (model.SyncEvent, error) {
	database.PrintMem("decodeSyncEvent start")
	var syncEvent model.SyncEvent
	dec := json.NewDecoder(body)

	// consume opening array token
	if _, err := dec.Token(); err != nil {
		return syncEvent, fmt.Errorf("error decoding SyncEvent token from cluster \"%s\": %v", clusterName, err)
	}
	for dec.More() {
		field, err := dec.Token()
		if err != nil {
			return syncEvent, fmt.Errorf("error decoding field name from SyncEvent request body from cluster \"%s\": %v", clusterName, err)
		}

		switch field {
		case "clearAll":
			if err := dec.Decode(&syncEvent.ClearAll); err != nil {
				return syncEvent, fmt.Errorf("failed to decode \"%s\" as clearAll: %v", field, err)
			}
		case "requestId":
			if err := dec.Decode(&syncEvent.RequestId); err != nil {
				return syncEvent, fmt.Errorf("failed to decode \"%s\" as requestId: %v", field, err)
			}
		case "addResources":
			if err := decodeArray(dec, &syncEvent.AddResources, clusterName, field); err != nil {
				return syncEvent, err
			}
		case "updateResources":
			if err := decodeArray(dec, &syncEvent.UpdateResources, clusterName, field); err != nil {
				return syncEvent, err
			}
		case "deleteResources":
			if err := decodeArray(dec, &syncEvent.DeleteResources, clusterName, field); err != nil {
				return syncEvent, err
			}
		case "addEdges":
			if err := decodeArray(dec, &syncEvent.AddEdges, clusterName, field); err != nil {
				return syncEvent, err
			}
		case "deleteEdges":
			if err := decodeArray(dec, &syncEvent.DeleteEdges, clusterName, field); err != nil {
				return syncEvent, err
			}
		}
	}
	database.PrintMem("decodeSyncEvent end")

	return syncEvent, nil
}

func decodeArray(dec *json.Decoder, target interface{}, clusterName string, field interface{}) error {
	// consume opening token
	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("error reading start of array for \"%s\" from cluster \"%s\": %v", field, clusterName, err)
	}
	switch slice := target.(type) {
	case *[]model.Resource:
		for dec.More() {
			var resource model.Resource
			if err := dec.Decode(&resource); err != nil {
				return fmt.Errorf("failed to decode \"%s\" as resource from cluster \"%s\": %v", field, clusterName, err)
			}
			*slice = append(*slice, resource)
		}
	case *[]model.DeleteResourceEvent:
		for dec.More() {
			var deleteResource model.DeleteResourceEvent
			if err := dec.Decode(&deleteResource); err != nil {
				return fmt.Errorf("failed to decode \"%s\" as delete resource from cluster \"%s\": %v", field, clusterName, err)
			}
			*slice = append(*slice, deleteResource)
		}
	case *[]model.Edge:
		for dec.More() {
			var edge model.Edge
			if err := dec.Decode(&edge); err != nil {
				return fmt.Errorf("failed to decode \"%s\" as edge from cluster \"%s\": %v", field, clusterName, err)
			}
			*slice = append(*slice, edge)
		}
	default:
		return fmt.Errorf("unsupported field array type \"%s\" to decode for SyncEvent from cluster \"%s\"", field, clusterName)
	}
	// consume closing token
	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("error reading end of array for \"%s\" from cluster \"%s\": %v", field, clusterName, err)
	}

	return nil
}
