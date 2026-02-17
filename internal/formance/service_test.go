package formance

import (
	"testing"

	"github.com/shopspring/decimal"
)

// ---------- Unit tests for pure helpers (no Formance stack needed) ----------

func TestFormanceAsset(t *testing.T) {
	tests := []struct {
		symbol string
		want   string
	}{
		{"USDC", "USDC/6"},
		{"BTC", "BTC/8"},
		{"ETH", "ETH/18"},
		{"UNKNOWN", "UNKNOWN/6"}, // default precision
	}
	for _, tt := range tests {
		if got := formanceAsset(tt.symbol); got != tt.want {
			t.Errorf("formanceAsset(%q) = %q, want %q", tt.symbol, got, tt.want)
		}
	}
}

func TestAssetSymbol(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"USDC/6", "USDC"},
		{"BTC/8", "BTC"},
		{"ETH/18", "ETH"},
		{"PLAIN", "PLAIN"},
	}
	for _, tt := range tests {
		if got := assetSymbol(tt.input); got != tt.want {
			t.Errorf("assetSymbol(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestPrecisionFor(t *testing.T) {
	if precisionFor("USDC") != 6 {
		t.Error("expected USDC precision 6")
	}
	if precisionFor("BTC") != 8 {
		t.Error("expected BTC precision 8")
	}
	if precisionFor("ETH") != 18 {
		t.Error("expected ETH precision 18")
	}
	if precisionFor("DOGE") != 6 {
		t.Error("expected unknown precision default 6")
	}
}

func TestBigIntToDecimal(t *testing.T) {
	// 1_000_000 smallest units of USDC (precision 6) = 1.0
	d := decimal.NewFromInt(1_000_000)
	result := bigIntToDecimal(d.BigInt(), "USDC")
	if !result.Equal(decimal.NewFromFloat(1.0)) {
		t.Errorf("expected 1.0, got %s", result.String())
	}

	// 100_000_000 smallest units of BTC (precision 8) = 1.0
	d = decimal.NewFromInt(100_000_000)
	result = bigIntToDecimal(d.BigInt(), "BTC")
	if !result.Equal(decimal.NewFromFloat(1.0)) {
		t.Errorf("expected 1.0, got %s", result.String())
	}

	// nil should return zero
	result = bigIntToDecimal(nil, "USDC")
	if !result.IsZero() {
		t.Errorf("expected 0, got %s", result.String())
	}
}

func TestIsConflictError(t *testing.T) {
	// nil error should not be a conflict
	if isConflictError(nil) {
		t.Error("nil should not be a conflict error")
	}
}

func TestAddrMetaKey(t *testing.T) {
	got := addrMetaKey("0xABC123DeF")
	want := "deposit_addr_0xabc123def"
	if got != want {
		t.Errorf("addrMetaKey = %q, want %q", got, want)
	}
}

func TestParseAddressesFromMeta_ListFormat(t *testing.T) {
	meta := map[string]string{
		"deposit_addresses":  `{"USDC":["0xABC","0xDEF"],"BTC":["bc1qxyz"]}`,
		"wallet_ids":         `{"USDC":"wlt_001","BTC":"wlt_002"}`,
		"account_identifier": "acc_123",
	}

	// All assets -- 3 total (2 USDC + 1 BTC)
	addrs := parseAddressesFromMeta("user1", "ethereum-mainnet", "", meta)
	if len(addrs) != 3 {
		t.Fatalf("expected 3 addresses, got %d", len(addrs))
	}

	// Filtered by USDC -- 2 addresses
	addrs = parseAddressesFromMeta("user1", "ethereum-mainnet", "USDC", meta)
	if len(addrs) != 2 {
		t.Fatalf("expected 2 USDC addresses, got %d", len(addrs))
	}
}

func TestParseAddressesFromMeta_LegacyFormat(t *testing.T) {
	// Legacy single-string format should still work.
	meta := map[string]string{
		"deposit_addresses":  `{"USDC":"0xABC","BTC":"bc1qxyz"}`,
		"wallet_ids":         `{"USDC":"wlt_001","BTC":"wlt_002"}`,
		"account_identifier": "acc_123",
	}

	addrs := parseAddressesFromMeta("user1", "ethereum-mainnet", "", meta)
	if len(addrs) != 2 {
		t.Fatalf("expected 2 addresses, got %d", len(addrs))
	}

	addrs = parseAddressesFromMeta("user1", "ethereum-mainnet", "USDC", meta)
	if len(addrs) != 1 {
		t.Fatalf("expected 1 address, got %d", len(addrs))
	}
	if addrs[0].Address != "0xABC" {
		t.Errorf("expected 0xABC, got %s", addrs[0].Address)
	}
}

func TestParseAddressesFromMeta_Empty(t *testing.T) {
	addrs := parseAddressesFromMeta("user1", "ethereum-mainnet", "", map[string]string{})
	if len(addrs) != 0 {
		t.Fatalf("expected 0 addresses, got %d", len(addrs))
	}
}

func TestAppendUnique(t *testing.T) {
	s := appendUnique([]string{"0xABC"}, "0xDEF")
	if len(s) != 2 {
		t.Fatalf("expected 2, got %d", len(s))
	}
	// Duplicate (case-insensitive) should not be added.
	s = appendUnique(s, "0xabc")
	if len(s) != 2 {
		t.Fatalf("expected 2 after duplicate, got %d", len(s))
	}
}
