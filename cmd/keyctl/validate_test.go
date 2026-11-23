package main

import "testing"

func TestValidateCreateFlags(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		keyName  string
		rps      float64
		burst    int
		capUSD   float64
		strategy string
		wantErr  bool
	}{
		{name: "valid, all defaults", keyName: "demo", wantErr: false},
		{name: "missing name", keyName: "", wantErr: true},
		{name: "negative rps", keyName: "demo", rps: -1, wantErr: true},
		{name: "negative burst", keyName: "demo", burst: -1, wantErr: true},
		{name: "negative cap", keyName: "demo", capUSD: -5, wantErr: true},
		{name: "unknown strategy", keyName: "demo", strategy: "yolo", wantErr: true},
		{name: "valid strategy", keyName: "demo", strategy: "cheapest", wantErr: false},
		{name: "zero rps/burst/cap means unlimited, not an error", keyName: "demo", rps: 0, burst: 0, capUSD: 0, wantErr: false},
		{name: "positive values", keyName: "demo", rps: 10, burst: 20, capUSD: 5, wantErr: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateCreateFlags(c.keyName, c.rps, c.burst, c.capUSD, c.strategy)
			if (err != nil) != c.wantErr {
				t.Errorf("validateCreateFlags(%q, %v, %v, %v, %q) = %v, wantErr %v",
					c.keyName, c.rps, c.burst, c.capUSD, c.strategy, err, c.wantErr)
			}
		})
	}
}
