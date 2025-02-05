// Copyright Contributors to the Open Cluster Management project
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/stolostron/search-indexer/pkg/config"
	"github.com/stolostron/search-indexer/pkg/model"
	"github.com/stolostron/search-indexer/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func Test_syncRequest(t *testing.T) {
	// Read mock request body.
	body, readErr := os.Open("./mocks/simple.json")
	if readErr != nil {
		t.Fatal(readErr)
	}
	responseRecorder := httptest.NewRecorder()

	request := httptest.NewRequest(http.MethodPost, "/aggregator/clusters/test-cluster/sync", body)
	router := mux.NewRouter()

	// Create server with mock database.
	server, mockPool := buildMockServer(t)

	br := &testutils.MockBatchResults{
		MockRows: testutils.MockRows{
			MockData: []map[string]interface{}{{"count": 5}, {"count": 3}},
		},
	}
	mockPool.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(br).Times(2)

	router.HandleFunc("/aggregator/clusters/{id}/sync", server.SyncResources)
	router.ServeHTTP(responseRecorder, request)

	// Validation
	expected := model.SyncResponse{Version: config.COMPONENT_VERSION, TotalAdded: 2, TotalResources: 5, TotalEdges: 3}

	if responseRecorder.Code != http.StatusOK {
		t.Errorf("Want status '%d', got '%d'", http.StatusOK, responseRecorder.Code)
	}

	var decodedResp model.SyncResponse
	err := json.NewDecoder(responseRecorder.Body).Decode(&decodedResp)
	if err != nil {
		t.Error("Unable to decode respoonse body.")
	}

	if fmt.Sprintf("%+v", decodedResp) != fmt.Sprintf("%+v", expected) {
		t.Errorf("Incorrect response body.\n expected '%+v'\n received '%+v'", expected, decodedResp)
	}
}

func Test_syncRequest_withError(t *testing.T) {
	// Read mock request body.
	body, readErr := os.Open("./mocks/simple.json")
	if readErr != nil {
		t.Fatal(readErr)
	}
	responseRecorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/aggregator/clusters/test-cluster/sync", body)
	router := mux.NewRouter()

	// Create server with mock database.
	server, mockPool := buildMockServer(t)

	br := &testutils.MockBatchResults{
		MockRows: testutils.MockRows{
			MockData: []map[string]interface{}{{"count": 5}, {"count": 3}},
		},
		MockErrorOnClose: errors.New("unexpected EOF"),
	}
	mockPool.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(br).Times(2)

	router.HandleFunc("/aggregator/clusters/{id}/sync", server.SyncResources)
	router.ServeHTTP(responseRecorder, request)

	// Validate
	assert.Equal(t, http.StatusInternalServerError, responseRecorder.Code)
	bodyString, _ := responseRecorder.Body.ReadString(byte(0))
	assert.Equal(t, "Server error while processing the request.\n", bodyString)
}

func Test_syncRequest_withErrorQueryingTotalResources(t *testing.T) {
	// Read mock request body.
	body, readErr := os.Open("./mocks/simple.json")
	if readErr != nil {
		t.Fatal(readErr)
	}
	responseRecorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/aggregator/clusters/test-cluster/sync", body)
	router := mux.NewRouter()

	// Create server with mock database.
	server, mockPool := buildMockServer(t)

	br := &testutils.MockBatchResults{
		MockRows: testutils.MockRows{
			MockData: []map[string]interface{}{{"count": 10}, {"count": 4}},
		},
		MockErrorOnClose: errors.New("unexpected EOF"),
	}
	mockPool.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(br).Times(2)

	router.HandleFunc("/aggregator/clusters/{id}/sync", server.SyncResources)
	router.ServeHTTP(responseRecorder, request)

	// Validate
	assert.Equal(t, http.StatusInternalServerError, responseRecorder.Code)
	bodyString, _ := responseRecorder.Body.ReadString(byte(0))
	assert.Equal(t, "Server error while processing the request.\n", bodyString)
}

func Test_resyncRequest(t *testing.T) {
	// Read mock request body.
	body, readErr := os.Open("./mocks/clearAll.json")
	if readErr != nil {
		t.Fatal(readErr)
	}
	responseRecorder := httptest.NewRecorder()

	request := httptest.NewRequest(http.MethodPost, "/aggregator/clusters/test-cluster/sync", body)
	router := mux.NewRouter()

	// Create server with mock database.
	server, mockPool := buildMockServer(t)
	testutils.MockDatabaseState(mockPool) // Mock Postgres state and SELECT queries.

	br := &testutils.MockBatchResults{
		MockRows: testutils.MockRows{
			MockData: []map[string]interface{}{{"count": 10}, {"count": 4}},
		},
	}

	mockPool.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Times(2)
	mockPool.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(br).Times(5)

	router.HandleFunc("/aggregator/clusters/{id}/sync", server.SyncResources)
	router.ServeHTTP(responseRecorder, request)

	if responseRecorder.Code != http.StatusOK {
		t.Errorf("Want status '%d', got '%d'", http.StatusOK, responseRecorder.Code)
	}

	var decodedResp model.SyncResponse
	err := json.NewDecoder(responseRecorder.Body).Decode(&decodedResp)
	if err != nil {
		t.Error("Unable to decode response body.")
	}

	expected := model.SyncResponse{Version: config.COMPONENT_VERSION, TotalAdded: 2, TotalDeleted: 1, TotalResources: 10, TotalEdgesDeleted: 1, TotalEdges: 4}
	if fmt.Sprintf("%+v", decodedResp) != fmt.Sprintf("%+v", expected) {
		t.Errorf("Incorrect response body.\n expected '%+v'\n received '%+v'", expected, decodedResp)
	}
}

func Test_resyncRequest_withErrorDeletingResources(t *testing.T) {
	// Read mock request body.
	body, readErr := os.Open("./mocks/clearAll.json")
	if readErr != nil {
		t.Fatal(readErr)
	}
	responseRecorder := httptest.NewRecorder()

	request := httptest.NewRequest(http.MethodPost, "/aggregator/clusters/test-cluster/sync", body)
	router := mux.NewRouter()

	// Create server with mock database.
	server, mockPool := buildMockServer(t)
	testutils.MockDatabaseState(mockPool) // Mock Postgres state and SELECT queries.

	mockPool.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("unexpected EOF"))

	br := &testutils.MockBatchResults{
		MockRows: testutils.MockRows{
			MockData: []map[string]interface{}{{"count": 10}},
		},
		MockErrorOnClose: errors.New("unexpected EOF"),
	}
	mockPool.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(br).Times(3)

	router.HandleFunc("/aggregator/clusters/{id}/sync", server.SyncResources)
	router.ServeHTTP(responseRecorder, request)

	// Validate
	assert.Equal(t, http.StatusInternalServerError, responseRecorder.Code)
	bodyString, _ := responseRecorder.Body.ReadString(byte(0))
	assert.Equal(t, "Server error while processing the request.\n", bodyString)
}

func Test_resyncRequest_withErrorDeletingEdges(t *testing.T) {
	// Read mock request body.
	body, readErr := os.Open("./mocks/clearAll.json")
	if readErr != nil {
		t.Fatal(readErr)
	}
	responseRecorder := httptest.NewRecorder()

	request := httptest.NewRequest(http.MethodPost, "/aggregator/clusters/test-cluster/sync", body)
	router := mux.NewRouter()

	// Create server with mock database.
	server, mockPool := buildMockServer(t)
	testutils.MockDatabaseState(mockPool) // Mock Postgres state and SELECT queries.
	mockPool.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any())
	mockPool.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("unexpected EOF"))

	br := &testutils.MockBatchResults{
		MockRows: testutils.MockRows{
			MockData: []map[string]interface{}{{"count": 10}},
		},
		MockErrorOnClose: errors.New("unexpected EOF"),
	}
	mockPool.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(br).Times(3)

	router.HandleFunc("/aggregator/clusters/{id}/sync", server.SyncResources)
	router.ServeHTTP(responseRecorder, request)

	// Validate
	assert.Equal(t, http.StatusInternalServerError, responseRecorder.Code)
	bodyString, _ := responseRecorder.Body.ReadString(byte(0))
	assert.Equal(t, "Server error while processing the request.\n", bodyString)
}

func Test_incorrectRequestBody(t *testing.T) {
	body := strings.NewReader("This is an incorrect request body.")

	responseRecorder := httptest.NewRecorder()

	request := httptest.NewRequest(http.MethodPost, "/aggregator/clusters/test-cluster/sync", body)
	router := mux.NewRouter()

	server, _ := buildMockServer(t)

	router.HandleFunc("/aggregator/clusters/{id}/sync", server.SyncResources)
	router.ServeHTTP(responseRecorder, request)

	if responseRecorder.Code != http.StatusBadRequest {
		t.Errorf("Want status '%d', got '%d'", http.StatusBadRequest, responseRecorder.Code)
	}
}

func Test_syncEventDecode(t *testing.T) {
	// Given: a mock syncEvent request body with fields clearAll, requestId, addResources, updateResources, addEdges, deleteEdges
	body, readErr := os.Open("./mocks/decode-sync-event.json")
	if readErr != nil {
		t.Fatal(readErr)
	}
	defer body.Close()
	mockBody := io.NopCloser(body)

	// When: we pass the full syncEvent request to our decode func
	syncEvent, err := decodeSyncEvent(mockBody, "clusterName")

	// Then: The entire syncEvent request is decoded into syncEvent without err
	assert.Nil(t, err)
	assert.Equal(t, 2, len(syncEvent.AddResources))
	assert.Equal(t, 2, len(syncEvent.UpdateResources))
	assert.Equal(t, 2, len(syncEvent.DeleteResources))
	assert.Equal(t, 2, len(syncEvent.AddEdges))
	assert.Equal(t, 2, len(syncEvent.DeleteEdges))
	assert.Equal(t, true, syncEvent.ClearAll)
	assert.Equal(t, 12345, syncEvent.RequestId)
}

func Test_syncEventIncorrectClearAll(t *testing.T) {
	// Given: a mock syncEvent request body with incorrect clearAll field value
	body := strings.NewReader(`{"clearAll": "TRUE"}`)
	mockBody := io.NopCloser(body)

	// When: we pass the syncEvent request to our decode func
	_, err := decodeSyncEvent(mockBody, "clusterName")

	// Then: it fails to decode incorrect clearAll value
	assert.Error(t, err, "failed to decode clearAll and clearAll")
}
