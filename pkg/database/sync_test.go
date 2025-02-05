// Copyright Contributors to the Open Cluster Management project
package database

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stolostron/search-indexer/pkg/model"
	"github.com/stolostron/search-indexer/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func Test_SyncData(t *testing.T) {
	// Prepare a mock DAO instance
	dao, mockPool := buildMockDAO(t)
	dao.batchSize = 1

	// Mock PosgreSQL calls
	br := &testutils.MockBatchResults{}
	mockPool.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(br).Times(7)

	// Prepare Request data
	data, _ := os.Open("./mocks/simple.json")
	dataBytes, _ := io.ReadAll(data)
	var syncEvent model.SyncEvent
	json.NewDecoder(data).Decode(&syncEvent) //nolint: errcheck

	// Execute test
	response := &model.SyncResponse{}
	err := dao.SyncData(context.Background(), syncEvent, "test-cluster", response, dataBytes)

	// Assert
	assert.Nil(t, err)
	AssertEqual(t, response.TotalAdded, 2, "Incorrect number of resources added.")
	AssertEqual(t, response.TotalUpdated, 1, "Incorrect number of resources updated.")
	AssertEqual(t, response.TotalDeleted, 1, "Incorrect number of resources deleted.")
	AssertEqual(t, response.TotalEdgesAdded, 1, "Incorrect number of edges added.")
	AssertEqual(t, response.TotalEdgesDeleted, 1, "Incorrect number of edges deleted.")
}

// Test for the error path.
func Test_Sync_With_Exec_Errors(t *testing.T) {
	// Prepare a mock DAO instance
	dao, mockPool := buildMockDAO(t)
	dao.batchSize = 1

	// Mock PosgreSQL calls
	br := &testutils.MockBatchResults{
		MockErrorOnExec: errors.New("mocking error on exec"),
	}
	mockPool.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(br).Times(7)

	// Prepare Request data
	data, _ := os.Open("./mocks/simple.json")
	dataBytes, _ := io.ReadAll(data)
	var syncEvent model.SyncEvent
	json.NewDecoder(data).Decode(&syncEvent) //nolint: errcheck

	// Supress console output to prevent log messages from polluting test output.
	defer testutils.SupressConsoleOutput()()

	// Execute test
	response := &model.SyncResponse{}
	err := dao.SyncData(context.Background(), syncEvent, "test-cluster", response, dataBytes)

	// Assert
	assert.Nil(t, err)
	AssertEqual(t, len(response.AddErrors), 2, "Incorrect number of AddErrors.")
	AssertEqual(t, len(response.UpdateErrors), 1, "Incorrect number of UpdateErrors.")
	AssertEqual(t, len(response.DeleteErrors), 2, "Incorrect number of DeleteErrors.")
	AssertEqual(t, len(response.AddEdgeErrors), 1, "Incorrect number of AddEdgeErrors.")
	AssertEqual(t, len(response.DeleteEdgeErrors), 1, "Incorrect number of DeleteEdgeErrors.")
}

func Test_Sync_With_OnClose_Errors(t *testing.T) {
	// Prepare a mock DAO instance
	dao, mockPool := buildMockDAO(t)
	dao.batchSize = 1

	// Mock PosgreSQL calls
	br := &testutils.MockBatchResults{
		MockErrorOnClose: errors.New("unexpected EOF"),
	}
	mockPool.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(br).Times(7)

	// Prepare Request data
	data, _ := os.Open("./mocks/simple.json")
	dataBytes, _ := io.ReadAll(data)
	var syncEvent model.SyncEvent
	json.NewDecoder(data).Decode(&syncEvent) //nolint: errcheck

	// Supress console output to prevent log messages from polluting test output.
	defer testutils.SupressConsoleOutput()()

	// Execute test
	response := &model.SyncResponse{}
	err := dao.SyncData(context.Background(), syncEvent, "test-cluster", response, dataBytes)

	// Assert
	assert.NotNil(t, err)
}
