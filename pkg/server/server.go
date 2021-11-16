// Copyright Contributors to the Open Cluster Management project

package server

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/open-cluster-management/search-indexer/pkg/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"
)

func StartAndListen() {
	router := mux.NewRouter()
	router.HandleFunc("/liveness", LivenessProbe).Methods("GET")
	router.HandleFunc("/readiness", ReadinessProbe).Methods("GET")
	router.HandleFunc("/aggregator/clusters/{id}/sync", SyncResources).Methods("POST")

	// Export metrics
	router.Path("/metrics").Handler(promhttp.Handler())

	// Configure TLS
	cfg := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
	}
	srv := &http.Server{
		Addr:              config.Cfg.ServerAddress,
		Handler:           router,
		TLSConfig:         cfg,
		ReadHeaderTimeout: time.Duration(config.Cfg.HTTPTimeout) * time.Millisecond,
		ReadTimeout:       time.Duration(config.Cfg.HTTPTimeout) * time.Millisecond,
		WriteTimeout:      time.Duration(config.Cfg.HTTPTimeout) * time.Millisecond,
		TLSNextProto:      make(map[string]func(*http.Server, *tls.Conn, http.Handler)),
	}

	klog.Info("Server listening on: ", srv.Addr)
	klog.Fatal(srv.ListenAndServeTLS("./sslcert/tls.crt", "./sslcert/tls.key"),
		" Use ./setup.sh to generate certificates for local development.")
}
