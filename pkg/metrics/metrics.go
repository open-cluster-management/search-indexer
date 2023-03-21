// Copyright Contributors to the Open Cluster Management project

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	PromRegistry = prometheus.NewRegistry()

	SyncRequestDuration = promauto.With(PromRegistry).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "search_indexer_request_duration",
		Help:    "Time (seconds) the search indexer takes to process a sync request from managed clusters.",
		Buckets: []float64{.25, .5, 1, 1.5, 2, 2.5, 5, 10},
	}, []string{"code"})

	SyncRequestSize = promauto.With(PromRegistry).NewHistogramVec(prometheus.HistogramOpts{
		Name: "search_indexer_request_size",
		Help: "Number of changes processed (add,update,delete) in a sync request from managed cluster.",
		// }, []string{"code", "managed_cluster_name"})
	}, []string{"code"})

	SyncRequestCount = promauto.With(PromRegistry).NewCounterVec(prometheus.CounterOpts{
		Name: "search_indexer_request_count",
		Help: "The total number of incoming sync requests to the search indexer from managed cluster.",
	}, []string{"managed_cluster_name"})
)
