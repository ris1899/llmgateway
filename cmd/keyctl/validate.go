package main

import (
	"errors"

	"llmgateway/internal/config"
)

// validateCreateFlags checks "create" flags before any Postgres I/O, split
// out from main's switch so it's testable without a database connection.
func validateCreateFlags(name string, rps float64, burst int, capUSD float64, strategy string) error {
	if name == "" {
		return errors.New("-name is required")
	}
	// ratelimit.Check treats any rps <= 0 as unlimited; reject negative here
	// instead of silently creating a key whose limit doesn't mean what the operator typed.
	if rps < 0 {
		return errors.New("-rps must not be negative")
	}
	if burst < 0 {
		return errors.New("-burst must not be negative")
	}
	if capUSD < 0 {
		return errors.New("-cap must not be negative")
	}
	if strategy != "" && !config.ValidStrategy(strategy) {
		return errors.New("unknown strategy " + strategy)
	}
	return nil
}
