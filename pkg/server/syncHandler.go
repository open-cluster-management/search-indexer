// Copyright Contributors to the Open Cluster Management project

package server

import (
	"bytes"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/stolostron/search-indexer/pkg/config"
	"github.com/stolostron/search-indexer/pkg/model"
	"io"
	"k8s.io/klog/v2"
	"net/http"
	"time"
)

func decodeKey(body *[]byte, key string, dest interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(*body))
	for {
		t, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			klog.Errorf("Error decoding token from request body: %s", err)
		}
		if k, ok := t.(string); ok && k == key {
			if decoder.More() {
				if err = decoder.Decode(dest); err != nil {
					return err
				}
				break
			}
		}
	}

	return nil
}

func (s *ServerConfig) SyncResources(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	clusterName := params["id"]

	// Copy request body and get ClearAll to determine processing procedure.
	var syncEvent model.SyncEvent
	body, err := io.ReadAll(r.Body)
	if err != nil {
		klog.Errorf("Error copying request body: %s", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err = decodeKey(&body, "clearAll", &syncEvent.ClearAll); err != nil {
		klog.Errorf("Error decoding clearAll from request body: %s", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err = decodeKey(&body, "requestId", &syncEvent.RequestId); err != nil {
		klog.Errorf("Error decoding requestId from request body: %s", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

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
		err = s.Dao.ResyncData(r.Context(), syncEvent, clusterName, syncResponse, body)
	} else {
		err = s.Dao.SyncData(r.Context(), syncEvent, clusterName, syncResponse, body)
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
}
