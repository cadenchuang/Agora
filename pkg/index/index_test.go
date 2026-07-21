package index

import (
	"math"
	"reflect"
	"sync"
	"testing"
)

func TestTokenize(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"lowercase", "The SENATE Votes", []string{"senate", "votes"}},
		{"punctuation", "war, peace; treaty!", []string{"war", "peace", "treaty"}},
		{"stopwords", "the war of the worlds", []string{"war", "worlds"}},
		{"alphanumeric", "article-19 bill 2024", []string{"article", "19", "bill", "2024"}},
		{"empty", "   ,,, ;;; ", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Tokenize(tt.in)
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Tokenize(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// buildCorpus indexes a tiny, hand-verifiable corpus used across scoring tests.
func buildCorpus(t *testing.T) *InvertedIndex {
	t.Helper()
	idx := New()
	docs := []struct{ id, text string }{
		{"d1", "the senate passed the climate treaty"},
		{"d2", "the senate debated the treaty at length in a long treaty debate"},
		{"d3", "geopolitical tensions rise over trade sanctions"},
	}
	for _, d := range docs {
		if err := idx.Add(d.id, d.text, d.text, nil); err != nil {
			t.Fatalf("Add(%s): %v", d.id, err)
		}
	}
	return idx
}

func TestBM25RankingOrder(t *testing.T) {
	idx := buildCorpus(t)
	// "treaty" appears twice in d2, once in d1, never in d3.
	got := idx.Search("treaty", 10, nil)
	if len(got) != 2 {
		t.Fatalf("expected 2 hits for 'treaty', got %d: %+v", len(got), got)
	}
	if got[0].DocID != "d2" || got[1].DocID != "d1" {
		t.Fatalf("expected d2 ranked above d1, got %+v", got)
	}
	if got[0].Score <= got[1].Score {
		t.Fatalf("expected strictly higher score for d2: %+v", got)
	}
}

// TestBM25ExactScore verifies the scorer against a value computed by hand from
// the BM25 formula, guarding against silent math regressions.
func TestBM25ExactScore(t *testing.T) {
	idx := New()
	// Two docs; query term "alpha" appears once in d1, absent in d2.
	if err := idx.Add("d1", "alpha beta", "alpha beta", nil); err != nil {
		t.Fatal(err)
	}
	if err := idx.Add("d2", "beta gamma delta", "beta gamma delta", nil); err != nil {
		t.Fatal(err)
	}

	// N=2, n(alpha)=1 -> idf = ln(1 + (2-1+0.5)/(1+0.5)) = ln(2).
	// avgdl = (2+3)/2 = 2.5, |d1|=2, f=1, k1=1.2, b=0.75.
	idf := math.Log(2.0)
	f := 1.0
	dl, avgdl := 2.0, 2.5
	denom := f + DefaultK1*(1-DefaultB+DefaultB*(dl/avgdl))
	want := idf * (f * (DefaultK1 + 1)) / denom

	got := idx.Search("alpha", 5, nil)
	if len(got) != 1 || got[0].DocID != "d1" {
		t.Fatalf("expected single hit d1, got %+v", got)
	}
	if math.Abs(got[0].Score-want) > 1e-9 {
		t.Fatalf("BM25 score = %.12f, want %.12f", got[0].Score, want)
	}
}

func TestSearchFiltersAndTopK(t *testing.T) {
	idx := New()
	_ = idx.Add("us1", "trade sanctions policy", "us1", map[string]string{"region": "us"})
	_ = idx.Add("eu1", "trade sanctions policy", "eu1", map[string]string{"region": "eu"})

	got := idx.Search("trade sanctions", 10, map[string]string{"region": "eu"})
	if len(got) != 1 || got[0].DocID != "eu1" {
		t.Fatalf("filter failed, got %+v", got)
	}

	got = idx.Search("trade", 1, nil)
	if len(got) != 1 {
		t.Fatalf("topK=1 should bound results, got %d", len(got))
	}
}

func TestEmptyAndDuplicateGuards(t *testing.T) {
	idx := New()
	if err := idx.Add("", "x", "x", nil); err != ErrEmptyDocID {
		t.Fatalf("expected ErrEmptyDocID, got %v", err)
	}
	if err := idx.Add("d1", "x", "x", nil); err != nil {
		t.Fatal(err)
	}
	if err := idx.Add("d1", "y", "y", nil); err == nil {
		t.Fatal("expected duplicate id error")
	}
	if idx.Search("anything", 5, nil) != nil {
		t.Fatal("query with no matching terms should return nil")
	}
}

// TestConcurrentAccess exercises the RWMutex under the race detector.
func TestConcurrentAccess(t *testing.T) {
	idx := New()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := "doc" + string(rune('A'+n%26)) + string(rune('0'+n/26))
			_ = idx.Add(id, "senate treaty debate sanctions", id, nil)
		}(i)
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = idx.Search("treaty sanctions", 5, nil)
			_ = idx.DocCount()
		}()
	}
	wg.Wait()
}
