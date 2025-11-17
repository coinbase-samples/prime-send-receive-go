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
