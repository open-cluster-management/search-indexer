// Copyright Contributors to the Open Cluster Management project

package database

import (
	"context"
	"fmt"

	"github.com/driftprogramming/pgxpoolmock"
	pgxpool "github.com/jackc/pgx/v4/pgxpool"
	"github.com/stolostron/search-indexer/pkg/config"
	"k8s.io/klog/v2"
)

// Database Access Object. Use a DAO instance so we can replace the pool object in the unit tests.
type DAO struct {
	pool      pgxpoolmock.PgxPool
	batchSize int
}

var poolSingleton pgxpoolmock.PgxPool

// Creates new DAO instance.
func NewDAO(p pgxpoolmock.PgxPool) DAO {
	// Crete DAO with default values.
	dao := DAO{
		batchSize: 500,
	}
	if p != nil {
		klog.Error("Using provided database connection. This path should only get executed during unit tests.")
		dao.pool = p
		return dao
	}

	if poolSingleton == nil {
		poolSingleton = initializePool()
	}
	dao.pool = poolSingleton
	return dao
}

func initializePool() pgxpoolmock.PgxPool {
	cfg := config.Cfg

	database_url := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", cfg.DBUser, cfg.DBPass, cfg.DBHost, cfg.DBPort, cfg.DBName)
	klog.Info("Connecting to PostgreSQL at: ", fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", cfg.DBUser, "*****", cfg.DBHost, cfg.DBPort, cfg.DBName))

	config, configErr := pgxpool.ParseConfig(database_url)
	if configErr != nil {
		klog.Fatal("Error parsing database connection configuration. ", configErr)
	}

	conn, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		klog.Error("Unable to connect to database: %+v\n", err)
		// TODO: We need to retry the connection until successful.
	}

	return conn
}

func (dao *DAO) InitializeTables() {
	// FIXME: REMOVE THIS WORKAROUND! Dropping tables to simplify development, we can't keep this for production.
	klog.Warning("FIXME: REMOVE THIS WORKAROUND! I'm dropping tables to simplify development, we can't keep this for production.")
	dao.pool.Exec(context.Background(), "CREATE SCHEMA IF NOT EXISTS search")                                                                                            //nolint: errcheck
	dao.pool.Exec(context.Background(), "DROP TABLE search.resources")                                                                                                   //nolint: errcheck
	dao.pool.Exec(context.Background(), "DROP TABLE search.edges")                                                                                                       //nolint: errcheck
	dao.pool.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS search.resources (uid TEXT PRIMARY KEY, cluster TEXT, data JSONB)")                                  //nolint: errcheck
	dao.pool.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS search.edges (sourceId TEXT, sourceKind TEXT,destId TEXT,destKind TEXT,edgeType TEXT,cluster TEXT)") //nolint: errcheck
}
