// Command test_search benchmarks Agora cluster latency against the coordinator
// HTTP API and reports latency percentiles plus which nodes served (or were
// degraded during) the run. Use it to measure behavior under single-node
// failure: start the cluster, stop one worker, and re-run.
//
//	go run ./scripts/test_search.go -q "climate treaty" -k 5 -n 500 -c 32
//	go run ./scripts/test_search.go -url http://localhost:8080 -q "sanctions"
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type apiResult struct {
	DocID   string  `json:"doc_id"`
	Score   float64 `json:"score"`
	Snippet string  `json:"text_snippet"`
	NodeID  string  `json:"node_id"`
}

type apiResponse struct {
	Query           string      `json:"query"`
	Results         []apiResult `json:"results"`
	ExecutionTimeMs int64       `json:"execution_time_ms"`
	ServedBy        []string    `json:"served_by"`
	DegradedNodes   []string    `json:"degraded_nodes"`
}

func main() {
	base := flag.String("url", "http://localhost:8080", "coordinator base URL")
	query := flag.String("q", "climate treaty", "search query")
	k := flag.Int("k", 5, "top-k")
	n := flag.Int("n", 200, "total requests")
	c := flag.Int("c", 16, "concurrency")
	flag.Parse()

	endpoint := fmt.Sprintf("%s/search?q=%s&k=%d", *base, url.QueryEscape(*query), *k)

	// One priming request: show the ranked results and cluster coverage.
	first, err := doRequest(endpoint)
	if err != nil {
		fmt.Fprintf(os.Stderr, "priming request failed: %v\n", err)
		os.Exit(1)
	}
	printResults(*query, first)

	fmt.Printf("\n== load: %d requests, concurrency %d ==\n", *n, *c)
	latencies := make([]time.Duration, *n)
	var failures atomic.Int64
	servedSeen := &sync.Map{}
	degradedSeen := &sync.Map{}

	jobs := make(chan int)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *c; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				t0 := time.Now()
				resp, err := doRequest(endpoint)
				latencies[i] = time.Since(t0)
				if err != nil {
					failures.Add(1)
					continue
				}
				for _, s := range resp.ServedBy {
					servedSeen.Store(s, struct{}{})
				}
				for _, d := range resp.DegradedNodes {
					degradedSeen.Store(d, struct{}{})
				}
			}
		}()
	}
	for i := 0; i < *n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	wall := time.Since(start)

	report(latencies, failures.Load(), wall, keys(servedSeen), keys(degradedSeen))
}

func doRequest(endpoint string) (*apiResponse, error) {
	resp, err := http.Get(endpoint)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	var out apiResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func printResults(query string, r *apiResponse) {
	fmt.Printf("query: %q\n", query)
	fmt.Printf("served_by=%v degraded=%v coordinator_time=%dms\n", r.ServedBy, r.DegradedNodes, r.ExecutionTimeMs)
	for i, res := range r.Results {
		fmt.Printf("  %d. [%.4f] %s (%s)\n      %s\n", i+1, res.Score, res.DocID, res.NodeID, truncate(res.Snippet, 90))
	}
}

func report(latencies []time.Duration, failures int64, wall time.Duration, served, degraded []string) {
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	fmt.Printf("requests:    %d (failures: %d)\n", len(latencies), failures)
	fmt.Printf("wall time:   %v\n", wall.Round(time.Millisecond))
	fmt.Printf("throughput:  %.0f req/s\n", float64(len(latencies))/wall.Seconds())
	fmt.Printf("latency p50: %v\n", pct(sorted, 50).Round(time.Microsecond))
	fmt.Printf("latency p95: %v\n", pct(sorted, 95).Round(time.Microsecond))
	fmt.Printf("latency p99: %v\n", pct(sorted, 99).Round(time.Microsecond))
	fmt.Printf("latency max: %v\n", sorted[len(sorted)-1].Round(time.Microsecond))
	sort.Strings(served)
	sort.Strings(degraded)
	fmt.Printf("served_by seen:  %v\n", served)
	fmt.Printf("degraded seen:   %v\n", degraded)
}

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func keys(m *sync.Map) []string {
	var out []string
	m.Range(func(k, _ any) bool {
		out = append(out, k.(string))
		return true
	})
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
