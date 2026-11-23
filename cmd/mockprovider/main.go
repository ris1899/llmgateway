// mockprovider runs one fake LLM provider. Run several on different ports to
// simulate several providers, and change their behavior at runtime:
//
//	curl -X PATCH localhost:9001/admin/behavior -d '{"error_rate":1}'
//	curl -X PATCH localhost:9001/admin/behavior -d '{"fault":"hang"}'
//	curl localhost:9001/admin/behavior
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"llmgateway/internal/mockprovider"
)

func main() {
	d := mockprovider.DefaultBehavior()
	addr := flag.String("addr", ":9001", "listen address")
	name := flag.String("name", "mock", "provider name, shown in replies")
	latency := flag.Duration("latency", time.Duration(d.Latency), "latency before response / first token")
	jitter := flag.Duration("jitter", 0, "extra random latency in [0, jitter)")
	errRate := flag.Float64("error-rate", 0, "probability of an injected error")
	errStatus := flag.Int("error-status", d.ErrorStatus, "HTTP status for injected errors")
	fault := flag.String("fault", "", `"hang" or "reset" for every request`)
	outTokens := flag.Int("output-tokens", d.OutputTokens, "tokens per reply")
	tps := flag.Float64("tokens-per-sec", d.TokensPerSec, "streaming speed (0 = unlimited)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	srv, err := mockprovider.New(*name, mockprovider.Behavior{
		Latency: mockprovider.Duration(*latency), Jitter: mockprovider.Duration(*jitter),
		ErrorRate: *errRate, ErrorStatus: *errStatus, Fault: *fault,
		OutputTokens: *outTokens, TokensPerSec: *tps,
	})
	if err != nil {
		log.Error("invalid behavior", "err", err)
		os.Exit(1)
	}

	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shCtx)
	}()

	log.Info("mock provider listening", "name", *name, "addr", *addr)
	if err := httpSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
}
