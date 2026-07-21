// Package index implements Agora's from-scratch information-retrieval core:
// a thread-safe in-memory inverted index and a pure-Go BM25 ranking function.
//
// The design keeps a single RWMutex around the whole index. Search is a
// read-mostly workload, so many concurrent Search calls proceed in parallel
// under RLock while ingestion takes the write lock. For Agora's per-shard
// corpus sizes this is simpler and faster than sharded striping, and it keeps
// the derived aggregates (docCount, totalLen) trivially consistent.
package index

import (
	"errors"
	"math"
	"sort"
	"sync"
)

// Default BM25 hyperparameters. k1 controls term-frequency saturation; b
// controls how strongly document length normalizes the score.
const (
	DefaultK1 = 1.2
	DefaultB  = 0.75
)

// ErrEmptyDocID is returned when attempting to index a document without an id.
var ErrEmptyDocID = errors.New("index: document id must not be empty")

// posting is one entry in a term's posting list: the document containing the
// term and how many times it appears there.
type posting struct {
	docID string
	tf    int
}

// document holds the metadata Agora needs at scoring and result-rendering time.
type document struct {
	id       string
	length   int               // token count after tokenization (|D|).
	snippet  string            // short excerpt returned with results.
	metadata map[string]string // used for filter matching.
}

// Result is a single scored hit produced by the local engine.
type Result struct {
	DocID   string
	Score   float64
	Snippet string
}

// InvertedIndex is a thread-safe in-memory index over a document partition.
type InvertedIndex struct {
	k1 float64
	b  float64

	mu       sync.RWMutex
	postings map[string][]posting // term -> posting list.
	docs     map[string]document  // docID -> document metadata.
	totalLen int64                // sum of all document lengths (for avgdl).
}

// New creates an index with the default BM25 hyperparameters.
func New() *InvertedIndex {
	return NewWithParams(DefaultK1, DefaultB)
}

// NewWithParams creates an index with explicit BM25 hyperparameters.
func NewWithParams(k1, b float64) *InvertedIndex {
	return &InvertedIndex{
		k1:       k1,
		b:        b,
		postings: make(map[string][]posting),
		docs:     make(map[string]document),
	}
}

// Add ingests a single document. The text is tokenized; term frequencies are
// accumulated into the posting lists and the document length is recorded.
// Re-adding an existing docID is treated as an error to keep aggregates honest;
// callers should ensure ids are unique within a shard.
func (idx *InvertedIndex) Add(docID, text, snippet string, metadata map[string]string) error {
	if docID == "" {
		return ErrEmptyDocID
	}

	tokens := Tokenize(text)
	tf := make(map[string]int, len(tokens))
	for _, t := range tokens {
		tf[t]++
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if _, exists := idx.docs[docID]; exists {
		return errors.New("index: duplicate document id: " + docID)
	}

	for term, freq := range tf {
		idx.postings[term] = append(idx.postings[term], posting{docID: docID, tf: freq})
	}

	idx.docs[docID] = document{
		id:       docID,
		length:   len(tokens),
		snippet:  snippet,
		metadata: metadata,
	}
	idx.totalLen += int64(len(tokens))
	return nil
}

// DocCount returns the number of indexed documents.
func (idx *InvertedIndex) DocCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.docs)
}

// avgdlLocked returns the average document length. Caller must hold at least a
// read lock. Returns 0 for an empty index.
func (idx *InvertedIndex) avgdlLocked() float64 {
	n := len(idx.docs)
	if n == 0 {
		return 0
	}
	return float64(idx.totalLen) / float64(n)
}

// idfLocked computes the BM25 (probabilistic, "plus-one") inverse document
// frequency for a term:
//
//	idf(q) = ln( 1 + (N - n(q) + 0.5) / (n(q) + 0.5) )
//
// The leading 1+ guarantees a non-negative IDF even for very common terms,
// avoiding the classic negative-IDF anomaly. Caller must hold a read lock.
func (idx *InvertedIndex) idfLocked(term string) float64 {
	n := float64(len(idx.postings[term]))
	N := float64(len(idx.docs))
	return math.Log(1 + (N-n+0.5)/(n+0.5))
}

// Search tokenizes the query, scores every candidate document with BM25, and
// returns the top-k results by descending score. Documents are candidates only
// if they contain at least one query term. `filters` (ANDed) must all match a
// document's metadata for it to be eligible.
//
// Formula (per query term q_i against document D):
//
//	score += IDF(q_i) * ( f(q_i,D) * (k1 + 1) ) /
//	                     ( f(q_i,D) + k1 * (1 - b + b * (|D| / avgdl)) )
func (idx *InvertedIndex) Search(query string, topK int, filters map[string]string) []Result {
	queryTerms := Tokenize(query)
	if len(queryTerms) == 0 || topK <= 0 {
		return nil
	}

	// Deduplicate query terms; a term repeated in the query shouldn't
	// double-count its posting list contribution.
	uniqueTerms := make(map[string]struct{}, len(queryTerms))
	for _, t := range queryTerms {
		uniqueTerms[t] = struct{}{}
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	avgdl := idx.avgdlLocked()
	if avgdl == 0 {
		return nil
	}

	scores := make(map[string]float64)
	for term := range uniqueTerms {
		list, ok := idx.postings[term]
		if !ok {
			continue
		}
		idf := idx.idfLocked(term)
		for _, p := range list {
			doc := idx.docs[p.docID]
			if !matchesFilters(doc.metadata, filters) {
				continue
			}
			f := float64(p.tf)
			denom := f + idx.k1*(1-idx.b+idx.b*(float64(doc.length)/avgdl))
			scores[p.docID] += idf * (f * (idx.k1 + 1)) / denom
		}
	}

	return topKResults(scores, idx.docs, topK)
}

// matchesFilters reports whether a document's metadata satisfies all filters.
// An empty filter set matches everything.
func matchesFilters(metadata, filters map[string]string) bool {
	for k, v := range filters {
		if metadata[k] != v {
			return false
		}
	}
	return true
}

// topKResults sorts the scored documents and returns the highest-scoring k.
// Sort is by score desc, with docID asc as a deterministic tie-breaker.
func topKResults(scores map[string]float64, docs map[string]document, topK int) []Result {
	if len(scores) == 0 {
		return nil
	}
	results := make([]Result, 0, len(scores))
	for docID, score := range scores {
		results = append(results, Result{
			DocID:   docID,
			Score:   score,
			Snippet: docs[docID].snippet,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].DocID < results[j].DocID
	})

	if len(results) > topK {
		results = results[:topK]
	}
	return results
}
