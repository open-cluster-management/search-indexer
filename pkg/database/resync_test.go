// Copyright Contributors to the Open Cluster Management project

package database

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stolostron/search-indexer/pkg/model"
	"github.com/stolostron/search-indexer/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func Test_ResyncData(t *testing.T) {
	// Prepare a mock DAO instance.
	dao, mockPool := buildMockDAO(t)

	testutils.MockDatabaseState(mockPool) // Mock Postgres state and SELECT queries.

	br := &testutils.MockBatchResults{}
	mockPool.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(br).Times(2)

	// Prepare Request data.
	data, _ := os.Open("./mocks/simple.json")
	dataBytes, _ := io.ReadAll(data)
	var syncEvent model.SyncEvent

	// Supress console output to prevent log messages from polluting test output.
	defer testutils.SupressConsoleOutput()()

	// Execute function test.
	response := &model.SyncResponse{}
	err := dao.ResyncData(context.Background(), syncEvent, "test-cluster", response, dataBytes)

	assert.Nil(t, err)
}

func Test_ResyncData_errors(t *testing.T) {
	// Prepare a mock DAO instance.
	dao, mockPool := buildMockDAO(t)
	// Mock Postgres state and SELECT queries.
	testutils.MockDatabaseState(mockPool)

	// Mock error on INSERT.
	br := &testutils.MockBatchResults{MockErrorOnClose: errors.New("unexpected EOF")}
	mockPool.EXPECT().SendBatch(gomock.Any(), gomock.Any()).Return(br).Times(2)

	// Prepare Request data.
	data, _ := os.Open("./mocks/simple.json")
	dataBytes, _ := io.ReadAll(data)
	var syncEvent model.SyncEvent

	// Supress console output to prevent log messages from polluting test output.
	defer testutils.SupressConsoleOutput()()

	// Execute function test.
	response := &model.SyncResponse{}
	err := dao.ResyncData(context.Background(), syncEvent, "test-cluster", response, dataBytes)

	assert.NotNil(t, err)
}
