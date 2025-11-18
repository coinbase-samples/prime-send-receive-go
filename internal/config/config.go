/**
 * Copyright 2025-present Coinbase Global, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"prime-send-receive-go/internal/models"
)

func Load() (*models.Config, error) {
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

	return &models.Config{
		Database: models.DatabaseConfig{
			Path:             getEnvString("DATABASE_PATH", "addresses.db"),
			MaxOpenConns:     getEnvInt("DB_MAX_OPEN_CONNS", 25),
			MaxIdleConns:     getEnvInt("DB_MAX_IDLE_CONNS", 5),
			ConnMaxLifetime:  connMaxLifetime,
			ConnMaxIdleTime:  connMaxIdleTime,
			PingTimeout:      pingTimeout,
			CreateDummyUsers: getEnvBool("CREATE_DUMMY_USERS", false),
		},
		Listener: models.ListenerConfig{
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
			return 0, fmt.Errorf("invalid duration for %s: %q (%w)", key, value, err)
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

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}
