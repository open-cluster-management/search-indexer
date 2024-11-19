// Copyright Contributors to the Open Cluster Management project

package config

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strconv"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const COMPONENT_VERSION = "2.13.0"

var DEVELOPMENT_MODE = false // Do not change this. See config_development.go to enable.
var Cfg = new()

// Struct to hold our configuratioin
type Config struct {
	DBBatchSize         int // Batch size used to write to DB. Default: 2500
	DBHost              string
	DBMinConns          int32 // Overrides pgxpool.Config{ MinConns } Default: 0
	DBMaxConns          int32 // Overrides pgxpool.Config{ MaxConns } Default: 20
	DBMaxConnIdleTime   int   // Overrides pgxpool.Config{ MaxConnIdleTime } Default: 30 min
	DBMaxConnLifeTime   int   // Overrides pgxpool.Config{ MaxConnLifetime } Default: 60 min
	DBMaxConnLifeJitter int   // Overrides pgxpool.Config{ MaxConnLifetimeJitter } Default: 2 min
	DBName              string
	DBPass              string
	DBPort              int
	DBUser              string
	DevelopmentMode     bool
	HTTPTimeout         int // Timeout for http server connections. Default: 5 min
	KubeClient          *kubernetes.Clientset
	KubeConfigPath      string
	MaxBackoffMS        int // Maximum backoff in ms to wait after db connection error
	PodName             string
	PodNamespace        string
	ResyncPeriodMS      int    // Time in MS for the clusters informer. Default: 15 min.
	RediscoverRateMS    int    // Time in MS we should check on cluster resource type
	RequestLimit        int    // Max number of concurrent requests. Used to prevent from overloading the database
	ServerAddress       string // Web server address
	SlowLog             int    // Log operations slower than the specified time in ms. Default: 1 sec
	Version             string
}

// Reads config from environment.
func new() *Config {
	conf := &Config{
		DBBatchSize:         getEnvAsInt("DB_BATCH_SIZE", 2500),
		DBHost:              getEnv("DB_HOST", "localhost"),
		DBMaxConns:          getEnvAsInt32("DB_MAX_CONNS", int32(10)),        // Overrides pgxpool default (4)
		DBMaxConnIdleTime:   getEnvAsInt("DB_MAX_CONN_IDLE_TIME", 60*1000),   // 1 min - Overrides pgxpool default (30)
		DBMaxConnLifeJitter: getEnvAsInt("DB_MAX_CONN_LIFE_JITTER", 10*1000), // 10 sec - Overrides pgxpool default
		DBMaxConnLifeTime:   getEnvAsInt("DB_MAX_CONN_LIFE_TIME", 60*1000),   // 1 min - Overrides pgxpool default (60)
		DBMinConns:          getEnvAsInt32("DB_MIN_CONNS", int32(2)),         // Overrides pgxpool default (0)
		DBName:              getEnv("DB_NAME", ""),
		DBPass:              getEnv("DB_PASS", ""),
		DBPort:              getEnvAsInt("DB_PORT", 5432),
		DBUser:              getEnv("DB_USER", ""),
		DevelopmentMode:     DEVELOPMENT_MODE,                       // Don't read ENV. See config_development.go to enable.
		HTTPTimeout:         getEnvAsInt("HTTP_TIMEOUT", 5*60*1000), // 5 min
		KubeConfigPath:      getKubeConfigPath(),
		// Use 5 min for delete cluster activities and 30 seconds for db reconnect retry
		MaxBackoffMS:     getEnvAsInt("MAX_BACKOFF_MS", 5*60*1000), // 5 min
		PodName:          getEnv("POD_NAME", "local-dev"),
		PodNamespace:     getEnv("POD_NAMESPACE", "open-cluster-management"),
		RediscoverRateMS: getEnvAsInt("REDISCOVER_RATE_MS", 5*60*1000), // 5 min
		ResyncPeriodMS:   getEnvAsInt("RESYNC_PERIOD_MS", 15*60*1000),  // 15 min - cluster resync period
		RequestLimit:     getEnvAsInt("REQUEST_LIMIT", 25),             // Set to 25 to prevent memory issues.
		ServerAddress:    getEnv("AGGREGATOR_ADDRESS", ":3010"),
		SlowLog:          getEnvAsInt("SLOW_LOG", 1000), // 1 second
		Version:          COMPONENT_VERSION,
	}

	// URLEncode the db password.
	conf.DBPass = url.QueryEscape(conf.DBPass)

	// Initialize Kube Client
	conf.KubeClient = getKubeClient()

	return conf
}

// Format and print environment to logger.
func (cfg *Config) PrintConfig() {
	// Make a copy to redact secrets and sensitive information.
	tmp := *cfg
	tmp.DBPass = "[REDACTED]"

	// Convert to JSON for nicer formatting.
	cfgJSON, err := json.MarshalIndent(tmp, "", "\t")
	if err != nil {
		klog.Warning("Encountered a problem formatting configuration. ", err)
		klog.Infof("Configuration %#v\n", tmp)
	}
	klog.Infof("Using configuration:\n%s\n", string(cfgJSON))
}

// Simple helper function to read an environment or return a default value
func getEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

// Simple helper function to read an environment variable into integer or return a default value
func getEnvAsInt(name string, defaultVal int) int {
	valueStr := getEnv(name, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultVal
}

// Helper function to read an environment variable into integer32 or return a default value
func getEnvAsInt32(name string, defaultVal int32) int32 {
	valueStr := getEnv(name, "")
	if value, err := strconv.ParseInt(valueStr, 10, 32); err == nil {
		return int32(value)
	}
	return defaultVal
}

// Validate required configuration.
func (cfg *Config) Validate() error {
	if cfg.DBName == "" {
		return errors.New("Required environment DB_NAME is not set.")
	}
	if cfg.DBUser == "" {
		return errors.New("Required environment DB_USER is not set.")
	}
	if cfg.DBPass == "" {
		return errors.New("Required environment DB_PASS is not set.")
	}
	return nil
}
