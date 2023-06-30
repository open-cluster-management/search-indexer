// Copyright Contributors to the Open Cluster Management project

package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	pgx "github.com/jackc/pgx/v4"
	"github.com/stolostron/search-indexer/pkg/model"
	"k8s.io/klog/v2"
)

// This is a wrapper for pgx.Batch. It add the following.
//  - The Queue() function checks the size of the queued items and automatically triggers the batch processing.
//  - Retry after a batch operation fails. It sends smaller batches to isolate the query producing the error.
//  - Report queries that resulted in errors.

type batchItem struct {
	query  string
	args   []interface{}
	action string // Used to report errors.
	uid    string // Used to report errors.
}

type batchWithRetry struct {
	connError       error
	ctx             context.Context
	dao             *DAO
	edgeInsertQ     [][]interface{} // Used to group edge INSERTs into a single query.
	items           []batchItem
	resourceInsertQ [][]interface{} // Used to group resource INSERTs into a single query.
	syncResponse    *model.SyncResponse
	wg              *sync.WaitGroup
}

func NewBatchWithRetry(ctx context.Context, dao *DAO, syncResponse *model.SyncResponse) batchWithRetry {
	batch := batchWithRetry{
		ctx:             ctx,
		dao:             dao,
		edgeInsertQ:     make([][]interface{}, 0),
		items:           make([]batchItem, 0),
		resourceInsertQ: make([][]interface{}, 0),
		syncResponse:    syncResponse,
		wg:              &sync.WaitGroup{},
	}
	return batch
}

// Adds a query to the queue and check if there's enough items to process the batch.
func (b *batchWithRetry) Queue(item batchItem) error {
	if b.connError != nil { // Can't queue more items after DB connection error.
		return b.connError
	}
	if item.action == "addResource" {
		b.resourceInsertQ = append(b.resourceInsertQ, item.args)
		if len(b.resourceInsertQ) >= b.dao.batchSize {
			values := make([]string, 0)

			for _, item := range b.resourceInsertQ {
				values = append(values, fmt.Sprintf("('%s','%s','%s')", item...))
			}

			// Add the bulk INSERT to the batch.
			b.items = append(b.items, batchItem{
				query:  fmt.Sprintf("INSERT INTO search.resources VALUES %s;", strings.Join(values, ", ")),
				args:   make([]interface{}, 0),
				action: "bulkResources",
				uid:    "",
			})

			// Reset the bulk INSERT queue.
			b.resourceInsertQ = make([][]interface{}, 0)
		}
	} else if item.action == "addEdge" {
		b.edgeInsertQ = append(b.edgeInsertQ, item.args)
		if len(b.edgeInsertQ) >= b.dao.batchSize {
			values := make([]string, 0)

			for _, item := range b.edgeInsertQ {
				values = append(values, fmt.Sprintf("('%s','%s','%s','%s','%s','%s')", item...))
			}

			// Add the bulk INSERT to the batch.
			b.items = append(b.items, batchItem{
				query:  fmt.Sprintf("INSERT INTO search.edges VALUES %s;", strings.Join(values, ",")),
				args:   make([]interface{}, 0),
				action: "bulkInsertEdges",
				uid:    "",
			})

			// Reset the bulk INSERT queue.
			b.edgeInsertQ = make([][]interface{}, 0)
		}
	} else {
		b.items = append(b.items, item)
	}

	if len(b.items) >= b.dao.batchSize {
		items := b.items               // Create a snapshot of the items to process.
		b.items = make([]batchItem, 0) // Reset the queue.
		b.wg.Add(1)
		go b.sendBatch(items) // nolint: errcheck
	}
	return nil
}

// Sends a batch to the database. If the batch results in an error, we divide
// the batch into smaller batches and retry until we isolate the erroring query.
func (b *batchWithRetry) sendBatch(items []batchItem) error {
	defer b.wg.Done()
	start := time.Now()
	defer func() {
		klog.V(3).Infof("Processed batch with %d statements in %+v", len(items), time.Since(start))
	}()

	batch := &pgx.Batch{}
	for _, item := range items {
		batch.Queue(item.query, item.args...)
	}
	br := b.dao.pool.SendBatch(b.ctx, batch)
	_, execErr := br.Exec()

	closeErr := br.Close()
	if closeErr != nil {
		if strings.Contains(closeErr.Error(), "unexpected EOF") || strings.Contains(closeErr.Error(), "failed to connect") {
			b.connError = closeErr
			klog.Error("Send batch failed because database is unavailable. Won't retry.")
			return errors.New("Failed to connect to database.")
		}
		klog.Error("Error closing batch result. ", closeErr)
		return closeErr
	}

	// Process errors.
	// pgx.Batch is processed as a transaction, so in case of an error, the entire batch will fail.
	if execErr != nil && len(items) == 1 {

		errorItem := items[0]
		klog.Errorf("ERROR processing batchItem. %+v", errorItem)

		var errorArray *[]model.SyncError
		switch errorItem.action {
		case "addResource":
			errorArray = &b.syncResponse.AddErrors
		case "updateResource":
			errorArray = &b.syncResponse.UpdateErrors
		case "deleteResource":
			errorArray = &b.syncResponse.DeleteErrors
		case "addEdge":
			errorArray = &b.syncResponse.AddEdgeErrors
		case "deleteEdge":
			errorArray = &b.syncResponse.DeleteEdgeErrors
		default:
			klog.Error("Unable to process sync error with type: ", errorItem.action)
		}
		*errorArray = append(*errorArray,
			model.SyncError{ResourceUID: errorItem.uid, Message: "Resource generated an error while updating the database."})

		return nil // We have processed the error, so don't return an error here to stop the recursion.

	} else if execErr != nil {
		// Error in send batch, resend queries using smaller batches.
		// Use a binary search recursively until we find the error.

		b.wg.Add(2)
		err1 := b.sendBatch(items[:len(items)/2])
		err2 := b.sendBatch(items[len(items)/2:])

		// Returns error only if we fail processing either retry.
		if err1 != nil && err2 != nil {
			return nil
		}
	}
	return execErr
}

// Process all queued items.
func (b *batchWithRetry) flush() {
	if len(b.resourceInsertQ) > 0 {
		values := make([]string, 0)

		for _, item := range b.resourceInsertQ {
			values = append(values, fmt.Sprintf("('%s', '%s', '%s')", item...))
		}

		// Add the resources INSERT to the batch.
		b.items = append(b.items, batchItem{
			query:  fmt.Sprintf("INSERT INTO search.resources VALUES %s;", strings.Join(values, ", ")),
			args:   make([]interface{}, 0),
			action: "bulkResources",
			uid:    "",
		})

		// Reset the resources INSERT queue.
		b.resourceInsertQ = make([][]interface{}, 0)
	}

	if len(b.edgeInsertQ) > 0 {
		values := make([]string, 0)

		for _, item := range b.edgeInsertQ {
			values = append(values, fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s')", item...))
		}

		// Add the edges INSERT to the batch.
		b.items = append(b.items, batchItem{
			query:  fmt.Sprintf("INSERT INTO search.edges VALUES %s;", strings.Join(values, ",")),
			args:   make([]interface{}, 0),
			action: "bulkInsertEdges",
			uid:    "",
		})

		// Reset the edges INSERT queue.
		b.edgeInsertQ = make([][]interface{}, 0)
	}

	if len(b.items) > 0 {
		items := b.items               // Create a snapshot of the items to process.
		b.items = make([]batchItem, 0) // Reset the queue.
		b.wg.Add(1)
		go b.sendBatch(items) // nolint: errcheck
	}
}
