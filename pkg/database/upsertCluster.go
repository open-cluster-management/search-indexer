package database

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stolostron/search-indexer/pkg/config"
	"github.com/stolostron/search-indexer/pkg/model"
	"k8s.io/klog/v2"
)

func (dao *DAO) DeleteCluster(ctx context.Context, clusterName string) {
	clusterUID := string("cluster__" + clusterName)
	retry := 0
	cfg := config.Cfg

	// Retry cluster deletion till it succeeds
	for {
		// If a statement within a transaction fails, the transaction can get aborted and rest of the statements can get skipped.
		// So if any statements fail, we retry the entire transaction
		err := dao.DeleteClusterTxn(ctx, clusterName)
		if err != nil {
			klog.Errorf("Unable to process cluster delete transaction for cluster:%s. Error: %+v\n", clusterName, err)
			waitMS := int(math.Min(float64(retry*500), float64(cfg.MaxBackoffMS)))
			retry++
			klog.Infof("Retry cluster delete transaction in %d milliseconds\n", waitMS)
			time.Sleep(time.Duration(waitMS) * time.Millisecond)
		} else {
			klog.Info("Successfully deleted cluster %s, related resources and edges from database!", clusterName)
			break
		}
	}

	// Delete cluster from existing clusters cache
	DeleteClustersCache(clusterUID)
}

func (dao *DAO) DeleteClusterTxn(ctx context.Context, clusterName string) error {
	clusterUID := string("cluster__" + clusterName)

	tx, txErr := dao.pool.BeginTx(ctx, pgx.TxOptions{})
	if txErr != nil {
		klog.Error("Error while beginning transaction block for deleting cluster ", clusterName)
		return txErr
	} else {
		// Delete resources for cluster from resources table from DB
		if _, err := tx.Exec(ctx, "DELETE FROM search.resources WHERE cluster=$1", clusterName); err != nil {
			checkErrorAndRollback(err, fmt.Sprintf("Error deleting resources from search.resources for clusterName %s.", clusterName), tx, ctx)
			return err
		}

		// Delete edges for cluster from DB
		if _, err := tx.Exec(ctx, "DELETE FROM search.edges WHERE cluster=$1", clusterName); err != nil {
			checkErrorAndRollback(err, fmt.Sprintf("Error deleting resources from search.edges for clusterName %s.", clusterName), tx, ctx)
			return err
		}

		// Delete cluster node from DB
		if _, err := tx.Exec(ctx, "DELETE FROM search.resources WHERE uid=$1", clusterUID); err != nil {
			checkErrorAndRollback(err, fmt.Sprintf("Error deleting cluster %s from search.resources.", clusterName), tx, ctx)
			tx.Commit(ctx)
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			checkErrorAndRollback(err, fmt.Sprintf("Error commiting delete cluster transaction for cluster: %s.", clusterName), tx, ctx)
			return err
		}
	}
	return nil
}

func (dao *DAO) UpsertCluster(resource model.Resource) {
	data, _ := json.Marshal(resource.Properties)
	clusterName := resource.Properties["name"].(string)
	query := "INSERT INTO search.resources as r (uid, cluster, data) values($1,'',$2) ON CONFLICT (uid) DO UPDATE SET data=$2 WHERE r.uid=$1"
	args := []interface{}{resource.UID, string(data)}

	// Insert cluster node if cluster does not exist in the DB
	if !dao.clusterInDB(resource.UID) || !dao.clusterPropsUpToDate(resource.UID, resource) {
		_, err := dao.pool.Exec(context.TODO(), query, args...)
		if err != nil {
			klog.Warningf("Error inserting/updating cluster with query %s, %s: %s ", query, clusterName, err.Error())
		} else {
			UpdateClustersCache(resource.UID, resource.Properties)
		}
	} else {
		klog.V(4).Infof("Cluster %s already exists in DB and properties are up to date.", clusterName)
		return
	}

}

func (dao *DAO) clusterInDB(clusterUID string) bool {
	_, ok := ReadClustersCache(clusterUID)
	if !ok {
		klog.V(3).Infof("Cluster [%s] is not in existingClustersCache. Updating cache with latest state from database.",
			clusterUID)
		query := "SELECT uid, data from search.resources where uid=$1"
		rows, err := dao.pool.Query(context.TODO(), query, clusterUID)
		if err != nil {
			klog.Errorf("Error while fetching cluster %s from database: %s", clusterUID, err.Error())
		}
		if rows != nil {
			for rows.Next() {
				var uid string
				var data interface{}
				err := rows.Scan(&uid, &data)
				if err != nil {
					klog.Errorf("Error %s retrieving rows for query:%s", err.Error(), query)
				} else {
					UpdateClustersCache(uid, data)
				}
			}
		}
		_, ok = ReadClustersCache(clusterUID)
	}
	return ok
}

func (dao *DAO) clusterPropsUpToDate(clusterUID string, resource model.Resource) bool {
	currProps := resource.Properties
	data, clusterPresent := ReadClustersCache(clusterUID)
	if clusterPresent {
		existingProps, ok := data.(map[string]interface{})
		if ok && len(existingProps) == len(currProps) {
			for key, currVal := range currProps {
				existingVal, ok := existingProps[key]

				if !ok || !reflect.DeepEqual(currVal, existingVal) {
					klog.V(4).Infof("cluster property values doesn't match for key:%s, existing value:%s, new value:%s \n",
						key, existingVal, currVal)
					return false
				}
			}
			return true
		} else {
			klog.V(3).Infof("For cluster %s, properties needs to be updated.", clusterUID)
			klog.V(5).Info("existingProps: ", existingProps)
			klog.V(5).Info("currProps: ", currProps)
			return false
		}
	} else {
		klog.V(3).Infof("Cluster [%s] is not in existingClustersCache.", clusterUID)
		return false
	}
}
