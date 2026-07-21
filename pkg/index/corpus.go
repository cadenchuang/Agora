package index

import (
	"encoding/json"
	"fmt"
	"os"
	"unicode/utf8"
)

// CorpusDoc is the on-disk representation of a document in a shard JSON file.
type CorpusDoc struct {
	ID       string            `json:"id"`
	Text     string            `json:"text"`
	Metadata map[string]string `json:"metadata"`
}

// snippetLen bounds the excerpt length stored per document.
const snippetLen = 160

// LoadShardFile reads a shard JSON file (an array of CorpusDoc) and ingests
// every document into idx. It returns the number of documents indexed.
func LoadShardFile(idx *InvertedIndex, path string) (int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read shard %q: %w", path, err)
	}

	var docs []CorpusDoc
	if err := json.Unmarshal(raw, &docs); err != nil {
		return 0, fmt.Errorf("parse shard %q: %w", path, err)
	}

	for _, d := range docs {
		if err := idx.Add(d.ID, d.Text, snippet(d.Text), d.Metadata); err != nil {
			return 0, fmt.Errorf("index doc %q: %w", d.ID, err)
		}
	}
	return len(docs), nil
}

// snippet returns a truncated, rune-safe excerpt of the document text.
func snippet(text string) string {
	if utf8.RuneCountInString(text) <= snippetLen {
		return text
	}
	runes := []rune(text)
	return string(runes[:snippetLen]) + "..."
}
