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

package common

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

type AssetConfig struct {
	Symbol  string `yaml:"symbol"`
	Network string `yaml:"network"`
}

type AssetsConfig struct {
	Assets []AssetConfig `yaml:"assets"`
}

func LoadAssetConfig(assetsFile string) ([]AssetConfig, error) {
	var assetsPath string
	if filepath.IsAbs(assetsFile) {
		assetsPath = assetsFile
	} else {
		wd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("failed to get working directory: %w", err)
		}
		assetsPath = filepath.Join(wd, assetsFile)
	}

	data, err := os.ReadFile(assetsPath)
	if err != nil {
		return nil, fmt.Errorf("unable to read %s: %w", assetsFile, err)
	}

	var config AssetsConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("unable to parse %s: %w", assetsFile, err)
	}

	for i, asset := range config.Assets {
		if asset.Symbol == "" {
			return nil, fmt.Errorf("asset at index %d missing symbol", i)
		}
		if asset.Network == "" {
			return nil, fmt.Errorf("asset at index %d missing network", i)
		}
	}

	return config.Assets, nil
}

func LoadAssetSymbols(assetsFile string) ([]string, error) {
	assets, err := LoadAssetConfig(assetsFile)
	if err != nil {
		return nil, err
	}

	symbols := make([]string, len(assets))
	for i, asset := range assets {
		symbols[i] = fmt.Sprintf("%s-%s", asset.Symbol, asset.Network)
	}

	return symbols, nil
}
