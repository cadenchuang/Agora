package main

import (
	"container/heap"
	"sort"

	pb "github.com/cadenchuang/agora/proto/agorapb"
)

// resultHeap is a min-heap of SearchResult ordered by score. Keeping the
// smallest score at the root lets us maintain the global top-K in O(M log K)
// where M is the total number of hits across all workers: we push while the
// heap is under capacity, and thereafter only admit a hit if it beats the
// current minimum, popping the min to make room.
type resultHeap []*pb.SearchResult

func (h resultHeap) Len() int            { return len(h) }
func (h resultHeap) Less(i, j int) bool  { return h[i].Score < h[j].Score }
func (h resultHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *resultHeap) Push(x any)         { *h = append(*h, x.(*pb.SearchResult)) }
func (h *resultHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// mergeTopK consumes per-worker result slices (each already locally ranked) and
// returns the global top-K in descending score order. A bounded min-heap keeps
// memory proportional to K rather than to the total hit count.
func mergeTopK(partials [][]*pb.SearchResult, k int) []*pb.SearchResult {
	if k <= 0 {
		return nil
	}
	h := &resultHeap{}
	heap.Init(h)

	for _, part := range partials {
		for _, r := range part {
			if h.Len() < k {
				heap.Push(h, r)
				continue
			}
			// Admit only if it beats the weakest result currently kept.
			if r.Score > (*h)[0].Score {
				heap.Pop(h)
				heap.Push(h, r)
			}
		}
	}

	out := make([]*pb.SearchResult, h.Len())
	// Draining a min-heap yields ascending order; fill back-to-front for desc.
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(*pb.SearchResult)
	}

	// Stable deterministic tie-break: score desc, then doc_id asc.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].DocId < out[j].DocId
	})
	return out
}
