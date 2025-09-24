package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Database DatabaseConfig
	Listener ListenerConfig
}

type DatabaseConfig struct {
	Path string
}

type ListenerConfig struct {
	LookbackWindow  time.Duration
	PollingInterval time.Duration
	CleanupInterval time.Duration
	AssetsFile      string
}

func Load() *Config {
	return &Config{
		Database: DatabaseConfig{
			Path: getEnvString("DATABASE_PATH", "addresses.db"),
		},
		Listener: ListenerConfig{
			LookbackWindow:  getEnvDuration("LISTENER_LOOKBACK_WINDOW", 6*time.Hour),
			PollingInterval: getEnvDuration("LISTENER_POLLING_INTERVAL", 30*time.Second),
			CleanupInterval: getEnvDuration("LISTENER_CLEANUP_INTERVAL", 15*time.Minute),
			AssetsFile:      getEnvString("ASSETS_FILE", "assets.yaml"),
		},
	}
}

func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
