package models

import "time"

// Config represents the application configuration
type Config struct {
	Database DatabaseConfig
	Listener ListenerConfig
}

// DatabaseConfig holds database connection settings
type DatabaseConfig struct {
	Path            string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	PingTimeout     time.Duration
}

// ListenerConfig holds transaction listener settings
type ListenerConfig struct {
	LookbackWindow  time.Duration
	PollingInterval time.Duration
	CleanupInterval time.Duration
	AssetsFile      string
}
