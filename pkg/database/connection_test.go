// Copyright Contributors to the Open Cluster Management project

package database

import (
	"testing"

	"github.com/golang/mock/gomock"
)

func Test_initializeTables(t *testing.T) {
	// Prepare a mock DAO instance
	dao, mockPool := buildMockDAO(t)
	mockPool.EXPECT().Exec(gomock.Any(), gomock.Eq("CREATE SCHEMA IF NOT EXISTS search")).Return(nil, nil)
	mockPool.EXPECT().Exec(gomock.Any(), gomock.Eq("DROP TABLE search.resources")).Return(nil, nil)
	mockPool.EXPECT().Exec(gomock.Any(), gomock.Eq("DROP TABLE search.edges")).Return(nil, nil)
	mockPool.EXPECT().Exec(gomock.Any(), gomock.Eq("CREATE TABLE IF NOT EXISTS search.edges (sourceId TEXT, sourceKind TEXT,destId TEXT,destKind TEXT,edgeType TEXT,cluster TEXT)")).Return(nil, nil)
	mockPool.EXPECT().Exec(gomock.Any(), gomock.Eq("CREATE TABLE IF NOT EXISTS search.resources (uid TEXT PRIMARY KEY, cluster TEXT, data JSONB)")).Return(nil, nil)

	// Execute function test.
	dao.InitializeTables()

}
