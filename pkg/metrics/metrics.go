// Copyright Contributors to the Open Cluster Management project

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	PromRegistry = prometheus.NewRegistry()

	RequestCount = promauto.With(PromRegistry).NewCounterVec(prometheus.CounterOpts{
		Name: "search_indexer_request_count",
		Help: "Total requests received by the search indexer (from managed clusters).",
	}, []string{"managed_cluster_name"})

	RequestDuration = promauto.With(PromRegistry).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "search_indexer_request_duration",
		Help:    "Time (seconds) the search indexer takes to process a request (from managed cluster).",
		Buckets: []float64{.25, .5, 1, 1.5, 2, 3, 5, 10},
	}, []string{"code"})

	RequestsInFlight = promauto.With(PromRegistry).NewGauge(prometheus.GaugeOpts{
		Name: "search_indexer_requests_in_flight",
		Help: "Total requests the search indexer is processing at a given time.",
	})

	RequestSize = promauto.With(PromRegistry).NewHistogram(prometheus.HistogramOpts{
		Name:    "search_indexer_request_size",
		Help:    "Total changes (add, update, delete) in the search indexer request (from managed cluster).",
		Buckets: []float64{50, 100, 200, 500, 5000, 10000, 25000, 50000, 100000, 200000},
	})

	ResyncCount = promauto.With(PromRegistry).NewCounterVec(prometheus.CounterOpts{
		Name: "search_indexer_cluster_resync_request_count",
		Help: "Total cluster resync requests received by the search indexer (from managed clusters).",
	}, []string{"cluster_resync_request"})

	// FUTURE: The summary metric could combine RequestCount and RequestDuration into a single metric.
	// RequestSummary = promauto.With(PromRegistry).NewSummaryVec(prometheus.SummaryOpts{
	// 	Name: "search_indexer_requests_summary",
	// 	Help: "Summarize (count and duration) of requests from managed clusters.",
	// }, []string{"managed_cluster_name"})
)
