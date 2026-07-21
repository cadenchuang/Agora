package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	pb "github.com/cadenchuang/agora/proto/agorapb"
)

// searchAPIResult mirrors a SearchResult for the JSON client API.
type searchAPIResult struct {
	DocID   string  `json:"doc_id"`
	Score   float64 `json:"score"`
	Snippet string  `json:"text_snippet"`
	NodeID  string  `json:"node_id"`
}

type searchAPIResponse struct {
	Query           string            `json:"query"`
	Results         []searchAPIResult `json:"results"`
	ExecutionTimeMs int64             `json:"execution_time_ms"`
	ServedBy        []string          `json:"served_by"`
	DegradedNodes   []string          `json:"degraded_nodes"`
}

type clusterStatus struct {
	Coordinator string             `json:"coordinator"`
	AliveCount  int                `json:"alive_count"`
	Workers     []clusterStatusRow `json:"workers"`
}

type clusterStatusRow struct {
	NodeID    string `json:"node_id"`
	Address   string `json:"address"`
	DocCount  int64  `json:"document_count"`
	LastSeenS string `json:"last_seen"`
}

func newHTTPHandler(router *Router, pool *WorkerPool, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	// GET /search?q=...&k=...&<filterKey>=<filterVal>
	mux.HandleFunc("/search", func(w http.ResponseWriter, req *http.Request) {
		query := req.URL.Query().Get("q")
		if query == "" {
			writeJSONError(w, http.StatusBadRequest, "missing required query parameter 'q'")
			return
		}
		topK := 10
		if kStr := req.URL.Query().Get("k"); kStr != "" {
			if k, err := strconv.Atoi(kStr); err == nil && k > 0 {
				topK = k
			}
		}

		// Any query param prefixed with "f_" becomes a metadata filter.
		var filters []*pb.Filter
		for key, vals := range req.URL.Query() {
			if len(key) > 2 && key[:2] == "f_" && len(vals) > 0 {
				filters = append(filters, &pb.Filter{Key: key[2:], Value: vals[0]})
			}
		}

		pbReq := &pb.SearchRequest{
			Query:     query,
			TopK:      int32(topK),
			Filters:   filters,
			RequestId: strconv.FormatInt(time.Now().UnixNano(), 36),
		}

		resp := router.Search(req.Context(), pbReq)
		writeJSON(w, http.StatusOK, toAPIResponse(query, resp))
	})

	// GET /status — cluster liveness snapshot.
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		alive := pool.Alive()
		rows := make([]clusterStatusRow, 0, len(alive))
		for _, wk := range alive {
			rows = append(rows, clusterStatusRow{
				NodeID:    wk.nodeID,
				Address:   wk.address,
				DocCount:  wk.docCount,
				LastSeenS: wk.lastSeen.Format(time.RFC3339Nano),
			})
		}
		writeJSON(w, http.StatusOK, clusterStatus{
			Coordinator: "agora-coordinator",
			AliveCount:  len(rows),
			Workers:     rows,
		})
	})

	// GET /healthz — coordinator's own liveness.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	return logRequests(log, mux)
}

func toAPIResponse(query string, resp *pb.SearchResponse) searchAPIResponse {
	results := make([]searchAPIResult, 0, len(resp.GetResults()))
	for _, r := range resp.GetResults() {
		results = append(results, searchAPIResult{
			DocID:   r.GetDocId(),
			Score:   r.GetScore(),
			Snippet: r.GetTextSnippet(),
			NodeID:  r.GetNodeId(),
		})
	}
	served := resp.GetServedBy()
	if served == nil {
		served = []string{}
	}
	degraded := resp.GetDegradedNodes()
	if degraded == nil {
		degraded = []string{}
	}
	return searchAPIResponse{
		Query:           query,
		Results:         results,
		ExecutionTimeMs: resp.GetExecutionTimeMs(),
		ServedBy:        served,
		DegradedNodes:   degraded,
	}
}

func logRequests(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Debug("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
			"elapsed_ms", time.Since(start).Milliseconds(),
		)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
