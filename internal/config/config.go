package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Database DatabaseConfig
	Listener ListenerConfig
}

type DatabaseConfig struct {
	Path            string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	PingTimeout     time.Duration
}

type ListenerConfig struct {
	LookbackWindow  time.Duration
	PollingInterval time.Duration
	CleanupInterval time.Duration
	AssetsFile      string
}

func Load() (*Config, error) {
	lookbackWindow, err := getEnvDuration("LISTENER_LOOKBACK_WINDOW", 6*time.Hour)
	if err != nil {
		return nil, err
	}

	pollingInterval, err := getEnvDuration("LISTENER_POLLING_INTERVAL", 30*time.Second)
	if err != nil {
		return nil, err
	}

	cleanupInterval, err := getEnvDuration("LISTENER_CLEANUP_INTERVAL", 15*time.Minute)
	if err != nil {
		return nil, err
	}

	connMaxLifetime, err := getEnvDuration("DB_CONN_MAX_LIFETIME", 5*time.Minute)
	if err != nil {
		return nil, err
	}

	connMaxIdleTime, err := getEnvDuration("DB_CONN_MAX_IDLE_TIME", 30*time.Second)
	if err != nil {
		return nil, err
	}

	pingTimeout, err := getEnvDuration("DB_PING_TIMEOUT", 5*time.Second)
	if err != nil {
		return nil, err
	}

	return &Config{
		Database: DatabaseConfig{
			Path:            getEnvString("DATABASE_PATH", "addresses.db"),
			MaxOpenConns:    getEnvInt("DB_MAX_OPEN_CONNS", 25),
			MaxIdleConns:    getEnvInt("DB_MAX_IDLE_CONNS", 5),
			ConnMaxLifetime: connMaxLifetime,
			ConnMaxIdleTime: connMaxIdleTime,
			PingTimeout:     pingTimeout,
		},
		Listener: ListenerConfig{
			LookbackWindow:  lookbackWindow,
			PollingInterval: pollingInterval,
			CleanupInterval: cleanupInterval,
			AssetsFile:      getEnvString("ASSETS_FILE", "assets.yaml"),
		},
	}, nil
}

func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) (time.Duration, error) {
	if value := os.Getenv(key); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return 0, fmt.Errorf("invalid duration for %s: %q (%v)", key, value, err)
		}
		return duration, nil
	}
	return defaultValue, nil
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
