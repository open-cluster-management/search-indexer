// Copyright Contributors to the Open Cluster Management project

package database

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/driftprogramming/pgxpoolmock"
	"github.com/jackc/pgx/v4"
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

	dbConnString := fmt.Sprint(
		"host=", cfg.DBHost,
		" port=", cfg.DBPort,
		" user=", cfg.DBUser,
		" password=", cfg.DBPass,
		" dbname=", cfg.DBName,
		" sslmode=require", // https://www.postgresql.org/docs/current/libpq-connect.html
	)

	// Remove password from connection log.
	redactedDbConn := strings.ReplaceAll(dbConnString, cfg.DBPass, "[REDACTED]")
	klog.Infof("Connecting to PostgreSQL using: %s", redactedDbConn)

	config, configErr := pgxpool.ParseConfig(dbConnString)
	if configErr != nil {
		klog.Fatal("Error parsing database connection configuration. ", configErr)
	}

	retry := 0
	var conn *pgxpool.Pool
	var err error
	for {
		conn, err = pgxpool.ConnectConfig(context.TODO(), config)
		if err != nil {
			// Max wait time is 30 sec
			waitMS := int(math.Min(float64(retry*500), float64(cfg.MaxBackoffMS/10)))
			timeToSleep := time.Duration(waitMS) * time.Millisecond
			retry++
			klog.Errorf("Unable to connect to database: %+v. Will retry in %s\n", err, timeToSleep)
			time.Sleep(timeToSleep)
		} else {
			klog.Info("Successfully connected to database!")
			break
		}
	}

	return conn
}

func (dao *DAO) InitializeTables(ctx context.Context) {
	if config.Cfg.DevelopmentMode {
		klog.Warning("Dropping search schema for development only. We must not see this message in production.")
		_, err := dao.pool.Exec(ctx, "DROP SCHEMA IF EXISTS search CASCADE")
		checkError(err, "Error dropping schema search.")
	}

	_, err := dao.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS search")
	checkError(err, "Error creating schema.")
	_, err = dao.pool.Exec(ctx,
		"CREATE TABLE IF NOT EXISTS search.resources (uid TEXT PRIMARY KEY, cluster TEXT, data JSONB)")
	checkError(err, "Error creating table search.resources.")
	_, err = dao.pool.Exec(ctx,
		"CREATE TABLE IF NOT EXISTS search.edges (sourceId TEXT, sourceKind TEXT,destId TEXT,destKind TEXT,edgeType TEXT,cluster TEXT, PRIMARY KEY(sourceId, destId, edgeType))")
	checkError(err, "Error creating table search.edges.")

	// Jsonb indexing data keys:
	_, err = dao.pool.Exec(ctx,
		"CREATE INDEX IF NOT EXISTS data_kind_idx ON search.resources USING GIN ((data -> 'kind'))")
	checkError(err, "Error creating index on search.resources data key kind.")

	_, err = dao.pool.Exec(ctx,
		"CREATE INDEX IF NOT EXISTS data_namespace_idx ON search.resources USING GIN ((data -> 'namespace'))")
	checkError(err, "Error creating index on search.resources data key namespace.")

	_, err = dao.pool.Exec(ctx,
		"CREATE INDEX IF NOT EXISTS data_name_idx ON search.resources USING GIN ((data ->  'name'))")
	checkError(err, "Error creating index on search.resources data key name.")

	_, err = dao.pool.Exec(ctx,
		"CREATE INDEX IF NOT EXISTS data_cluster_idx ON search.resources USING btree (cluster)")
	checkError(err, "Error creating index on search.resources cluster.")

	_, err = dao.pool.Exec(ctx,
		"CREATE INDEX IF NOT EXISTS data_composite_idx ON search.resources USING GIN ((data -> '_hubClusterResource'::text), (data -> 'namespace'::text), (data -> 'apigroup'::text), (data -> 'kind_plural'::text))")
	checkError(err, "Error creating index on search.resources data composite.")

	_, err = dao.pool.Exec(ctx,
		"CREATE INDEX IF NOT EXISTS data_hubCluster_idx ON search.resources USING GIN ((data ->  '_hubClusterResource')) WHERE data ? '_hubClusterResource'")
	checkError(err, "Error creating index on search.resources data key _hubClusterResource.")
}

func checkError(err error, logMessage string) {
	if err != nil {
		klog.Error(logMessage, " ", err)
	}
}

func checkErrorAndRollback(err error, logMessage string, tx pgx.Tx, ctx context.Context) {
	checkError(err, logMessage)
	if err := tx.Rollback(ctx); err != nil {
		checkError(err, "Encountered error while rolling back cluster delete transaction command")
	}
}
