// Command loadgen is a small load generator for the gateway's benchmark
// scripts. It exists instead of reusing k6 for two cases k6 doesn't cover
// well on this project: very high concurrent streaming connection counts, and
// reading gateway-specific response headers (X-Gateway-*) into structured
// per-request records for later analysis (CSV, timelines).
//
// Two modes:
//
//	rate:    open-model constant arrival rate load test (see runRate).
//	streams: sustained concurrent streaming connections (see runStreams).
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// headerFlags collects repeated -header k:v flags.
type headerFlags map[string]string

func (h headerFlags) String() string { return "" }
func (h headerFlags) Set(v string) error {
	parts := strings.SplitN(v, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("header must be k:v, got %q", v)
	}
	h[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	return nil
}

// result is one request's outcome, recorded regardless of success so errors
// show up in the summary and CSV too.
type result struct {
	startUnixMs int64
	latencyMs   float64
	ttftMs      float64 // -1 if not applicable (non-stream, or no token seen)
	status      int
	provider    string
	overheadMs  float64
	cache       string
	err         string
}

func main() {
	mode := flag.String("mode", "rate", "rate|streams")

	// shared
	url := flag.String("url", "http://localhost:8080/v1", "base URL (no trailing /chat/completions)")
	model := flag.String("model", "mock-chat", "model name sent in the request body")
	stream := flag.Bool("stream", false, "use stream:true (rate mode) / always true in streams mode")
	csvPath := flag.String("csv", "", "optional path to write per-request CSV rows")
	timelinePath := flag.String("timeline", "", "optional path to write per-second timeline CSV (rate mode only)")

	// rate mode
	rps := flag.Float64("rps", 100, "requests per second (rate mode)")
	duration := flag.Duration("duration", 10*time.Second, "test duration (rate mode)")
	var tempSet bool
	var temperature float64
	flag.Func("temperature", "temperature to send in body (omitted if not set)", func(s string) error {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		temperature, tempSet = v, true
		return nil
	})

	// streams mode
	conns := flag.Int("conns", 1000, "number of concurrent streaming connections")
	ramp := flag.Duration("ramp", 10*time.Second, "time to ramp up to -conns (streams mode)")
	hold := flag.Duration("hold", 30*time.Second, "time to hold at -conns after ramp (streams mode)")

	headers := make(headerFlags)
	flag.Var(headers, "header", "extra request header k:v, repeatable")

	flag.Parse()

	transport := &http.Transport{
		MaxIdleConns:        20000,
		MaxIdleConnsPerHost: 20000,
		MaxConnsPerHost:     0, // unlimited: we want real concurrency, not queuing in the transport
		IdleConnTimeout:     90 * time.Second,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: transport}

	switch *mode {
	case "rate":
		runRate(client, rateOpts{
			url: *url, model: *model, stream: *stream,
			rps: *rps, duration: *duration,
			tempSet: tempSet, temperature: temperature,
			headers: headers, csvPath: *csvPath, timelinePath: *timelinePath,
		})
	case "streams":
		runStreams(client, streamsOpts{
			url: *url, model: *model, conns: *conns, ramp: *ramp, hold: *hold,
			tempSet: tempSet, temperature: temperature,
			headers: headers, csvPath: *csvPath,
		})
	default:
		log.Fatalf("unknown -mode %q", *mode)
	}
}

// ---------------- request building / execution ----------------

func buildBody(model string, stream bool, tempSet bool, temperature float64) []byte {
	var b bytes.Buffer
	b.WriteString(`{"model":"`)
	b.WriteString(model)
	b.WriteString(`","messages":[{"role":"user","content":"Say hello in one sentence."}]`)
	if stream {
		b.WriteString(`,"stream":true`)
	}
	if tempSet {
		fmt.Fprintf(&b, `,"temperature":%g`, temperature)
	}
	b.WriteString(`}`)
	return b.Bytes()
}

// doRequest issues one chat completion request and returns a filled result.
// For streams it reads to EOF so the connection is fully drained, and records
// TTFT as the time to the first SSE "data:" line carrying content.
func doRequest(ctx context.Context, client *http.Client, u, model string, stream bool, tempSet bool, temperature float64, headers map[string]string) result {
	start := time.Now()
	r := result{startUnixMs: start.UnixMilli(), ttftMs: -1}

	body := buildBody(model, stream, tempSet, temperature)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(u, "/")+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		r.err = err.Error()
		return r
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		r.err = err.Error()
		r.latencyMs = msSince(start)
		return r
	}
	defer resp.Body.Close()

	r.status = resp.StatusCode
	r.provider = resp.Header.Get("X-Gateway-Provider")
	r.cache = resp.Header.Get("X-Gateway-Cache")
	if v := resp.Header.Get("X-Gateway-Overhead-Ms"); v != "" {
		r.overheadMs, _ = strconv.ParseFloat(v, 64)
	}

	if stream {
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		gotFirst := false
		for sc.Scan() {
			line := sc.Text()
			if !gotFirst && strings.HasPrefix(line, "data:") && !strings.Contains(line, "[DONE]") {
				r.ttftMs = msSince(start)
				gotFirst = true
			}
		}
		if err := sc.Err(); err != nil && r.err == "" {
			r.err = err.Error()
		}
	} else {
		_, err := io.Copy(io.Discard, resp.Body)
		if err != nil && r.err == "" {
			r.err = err.Error()
		}
	}
	r.latencyMs = msSince(start)
	return r
}

func msSince(t time.Time) float64 { return float64(time.Since(t)) / float64(time.Millisecond) }

// ---------------- rate mode ----------------

type rateOpts struct {
	url, model            string
	stream                bool
	rps                   float64
	duration              time.Duration
	tempSet               bool
	temperature           float64
	headers               headerFlags
	csvPath, timelinePath string
}

func runRate(client *http.Client, o rateOpts) {
	interval := time.Duration(float64(time.Second) / o.rps)
	deadline := time.Now().Add(o.duration)

	var mu sync.Mutex
	var results []result
	var sent int64

	var wg sync.WaitGroup
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), o.duration+30*time.Second)
	defer cancel()

	fmt.Printf("rate mode: %.1f rps for %s against %s model=%s stream=%v\n", o.rps, o.duration, o.url, o.model, o.stream)

	for now := range ticker.C {
		if now.After(deadline) {
			break
		}
		atomic.AddInt64(&sent, 1)
		wg.Add(1)
		// Launch unconditionally per tick: an open model. A slow server must
		// show up as latency, never as fewer requests sent ("coordinated
		// omission").
		go func() {
			defer wg.Done()
			r := doRequest(ctx, client, o.url, o.model, o.stream, o.tempSet, o.temperature, o.headers)
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}()
	}
	wg.Wait()

	printSummary(results, sent, o.rps, o.duration, o.stream)
	if o.csvPath != "" {
		if err := writeCSV(o.csvPath, results); err != nil {
			log.Printf("csv write: %v", err)
		}
	}
	if o.timelinePath != "" {
		if err := writeTimeline(o.timelinePath, results); err != nil {
			log.Printf("timeline write: %v", err)
		}
	}
}

// ---------------- streams mode ----------------

type streamsOpts struct {
	url, model  string
	conns       int
	ramp, hold  time.Duration
	tempSet     bool
	temperature float64
	headers     headerFlags
	csvPath     string
}

func runStreams(client *http.Client, o streamsOpts) {
	checkFDLimit(o.conns)

	ctx, cancel := context.WithTimeout(context.Background(), o.ramp+o.hold+60*time.Second)
	defer cancel()

	stopAt := time.Now().Add(o.ramp + o.hold)

	var openNow, peakOpen, completed, errored int64
	var ttfts []float64
	var ttftMu sync.Mutex

	var wg sync.WaitGroup
	fmt.Printf("streams mode: ramping to %d conns over %s, holding %s\n", o.conns, o.ramp, o.hold)

	launchOne := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stopAt) {
				cur := atomic.AddInt64(&openNow, 1)
				for {
					peak := atomic.LoadInt64(&peakOpen)
					if cur <= peak || atomic.CompareAndSwapInt64(&peakOpen, peak, cur) {
						break
					}
				}
				r := doRequest(ctx, client, o.url, o.model, true, o.tempSet, o.temperature, o.headers)
				atomic.AddInt64(&openNow, -1)
				if r.err != "" || (r.status != 0 && r.status >= 400) {
					atomic.AddInt64(&errored, 1)
				} else {
					atomic.AddInt64(&completed, 1)
				}
				if r.ttftMs >= 0 {
					ttftMu.Lock()
					ttfts = append(ttfts, r.ttftMs)
					ttftMu.Unlock()
				}
			}
		}()
	}

	// Ramp: spread connection starts evenly over the ramp window.
	if o.conns > 0 && o.ramp > 0 {
		step := o.ramp / time.Duration(o.conns)
		for i := 0; i < o.conns; i++ {
			launchOne()
			time.Sleep(step)
		}
	} else {
		for i := 0; i < o.conns; i++ {
			launchOne()
		}
	}

	wg.Wait()

	sort.Float64s(ttfts)
	fmt.Println("\n--- streams summary ---")
	fmt.Printf("peak concurrent open streams: %d\n", peakOpen)
	fmt.Printf("completed streams: %d\n", completed)
	fmt.Printf("errored streams:   %d\n", errored)
	if len(ttfts) > 0 {
		fmt.Printf("TTFT p50=%.1fms p99=%.1fms\n", pct(ttfts, 0.50), pct(ttfts, 0.99))
	}
}

// checkFDLimit warns and tries to raise the soft fd limit if it looks too
// low for the requested connection count.
func checkFDLimit(conns int) {
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err != nil {
		log.Printf("warning: could not read RLIMIT_NOFILE: %v", err)
		return
	}
	want := uint64(2*conns + 1000)
	if rlim.Cur < want {
		log.Printf("warning: ulimit -n is %d, want >= %d for %d connections; raising soft limit to hard limit (%d)", rlim.Cur, want, conns, rlim.Max)
		rlim.Cur = rlim.Max
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlim); err != nil {
			log.Printf("warning: could not raise RLIMIT_NOFILE: %v (run `ulimit -n 65536` before invoking loadgen, or on macOS `sudo launchctl limit maxfiles 65536 200000`)", err)
		}
	}
}

// ---------------- summary / output ----------------

func printSummary(results []result, sent int64, targetRPS float64, duration time.Duration, stream bool) {
	var ok int
	errByType := map[string]int{}
	var lat, overhead, ttft []float64
	providerCount := map[string]int{}
	cacheCount := map[string]int{}

	for _, r := range results {
		lat = append(lat, r.latencyMs)
		if r.err != "" {
			errByType[classifyErr(r.err)]++
			continue
		}
		if r.status >= 200 && r.status < 300 {
			ok++
		} else {
			errByType[fmt.Sprintf("http_%d", r.status)]++
		}
		overhead = append(overhead, r.overheadMs)
		if r.provider != "" {
			providerCount[r.provider]++
		}
		if r.cache != "" {
			cacheCount[r.cache]++
		}
		if stream && r.ttftMs >= 0 {
			ttft = append(ttft, r.ttftMs)
		}
	}

	sort.Float64s(lat)
	sort.Float64s(overhead)
	sort.Float64s(ttft)

	fmt.Println("\n--- rate summary ---")
	fmt.Printf("sent: %d  ok: %d  errors: %d\n", sent, ok, len(results)-ok)
	for k, v := range errByType {
		fmt.Printf("  error[%s]: %d\n", k, v)
	}
	achieved := float64(sent) / duration.Seconds()
	fmt.Printf("achieved rps: %.1f (target %.1f)\n", achieved, targetRPS)
	if len(lat) > 0 {
		fmt.Printf("latency ms: p50=%.1f p90=%.1f p99=%.1f p999=%.1f max=%.1f\n",
			pct(lat, 0.50), pct(lat, 0.90), pct(lat, 0.99), pct(lat, 0.999), lat[len(lat)-1])
	}
	if len(overhead) > 0 {
		fmt.Printf("overhead ms: p50=%.1f p99=%.1f max=%.1f\n",
			pct(overhead, 0.50), pct(overhead, 0.99), overhead[len(overhead)-1])
	}
	if stream && len(ttft) > 0 {
		fmt.Printf("ttft ms: p50=%.1f p99=%.1f\n", pct(ttft, 0.50), pct(ttft, 0.99))
	}
	if len(providerCount) > 0 {
		fmt.Printf("provider distribution: %s\n", distStr(providerCount))
	}
	if len(cacheCount) > 0 {
		fmt.Printf("cache distribution: %s\n", distStr(cacheCount))
	}
	fmt.Printf("goroutines at exit (sanity): %d\n", runtime.NumGoroutine())
}

func distStr(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

func classifyErr(e string) string {
	switch {
	case strings.Contains(e, "deadline exceeded") || strings.Contains(e, "context deadline"):
		return "timeout"
	case strings.Contains(e, "connection refused"):
		return "conn_refused"
	case strings.Contains(e, "reset by peer"):
		return "conn_reset"
	case strings.Contains(e, "EOF"):
		return "eof"
	default:
		return "other"
	}
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func writeCSV(path string, results []result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"unix_ms_start", "latency_ms", "ttft_ms", "status", "provider", "overhead_ms", "cache", "error"})
	for _, r := range results {
		_ = w.Write([]string{
			strconv.FormatInt(r.startUnixMs, 10),
			strconv.FormatFloat(r.latencyMs, 'f', 2, 64),
			strconv.FormatFloat(r.ttftMs, 'f', 2, 64),
			strconv.Itoa(r.status),
			r.provider,
			strconv.FormatFloat(r.overheadMs, 'f', 2, 64),
			r.cache,
			r.err,
		})
	}
	return w.Error()
}

// writeTimeline buckets results by the second they started in and writes one
// row per second with counts, latency percentiles and provider distribution.
func writeTimeline(path string, results []result) error {
	if len(results) == 0 {
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = f.WriteString("second,sent,ok,errors,p50,p99,providers\n")
		return err
	}

	minSec := results[0].startUnixMs / 1000
	for _, r := range results {
		s := r.startUnixMs / 1000
		if s < minSec {
			minSec = s
		}
	}

	type bucket struct {
		sent, ok, errs int
		lat            []float64
		providers      map[string]int
	}
	buckets := map[int64]*bucket{}
	for _, r := range results {
		sec := r.startUnixMs/1000 - minSec
		b, ok := buckets[sec]
		if !ok {
			b = &bucket{providers: map[string]int{}}
			buckets[sec] = b
		}
		b.sent++
		if r.err == "" && r.status >= 200 && r.status < 300 {
			b.ok++
			b.lat = append(b.lat, r.latencyMs)
			if r.provider != "" {
				b.providers[r.provider]++
			}
		} else {
			b.errs++
		}
	}

	var maxSec int64
	for s := range buckets {
		if s > maxSec {
			maxSec = s
		}
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"second", "sent", "ok", "errors", "p50", "p99", "providers"})
	for s := int64(0); s <= maxSec; s++ {
		b, ok := buckets[s]
		if !ok {
			_ = w.Write([]string{strconv.FormatInt(s, 10), "0", "0", "0", "0", "0", ""})
			continue
		}
		sort.Float64s(b.lat)
		provParts := make([]string, 0, len(b.providers))
		keys := make([]string, 0, len(b.providers))
		for k := range b.providers {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			provParts = append(provParts, fmt.Sprintf("%s=%d", k, b.providers[k]))
		}
		_ = w.Write([]string{
			strconv.FormatInt(s, 10),
			strconv.Itoa(b.sent),
			strconv.Itoa(b.ok),
			strconv.Itoa(b.errs),
			strconv.FormatFloat(pct(b.lat, 0.50), 'f', 2, 64),
			strconv.FormatFloat(pct(b.lat, 0.99), 'f', 2, 64),
			strings.Join(provParts, ";"),
		})
	}
	return w.Error()
}
