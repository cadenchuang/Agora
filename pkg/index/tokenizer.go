package index

import (
	"strings"
	"unicode"
)

// stopWords is a small English stop-word set. Kept intentionally compact: an
// IR engine wants to drop only the highest-frequency function words so that
// BM25's IDF term does the rest of the discrimination.
var stopWords = map[string]struct{}{
	"a": {}, "an": {}, "and": {}, "are": {}, "as": {}, "at": {}, "be": {},
	"but": {}, "by": {}, "for": {}, "if": {}, "in": {}, "into": {}, "is": {},
	"it": {}, "no": {}, "not": {}, "of": {}, "on": {}, "or": {}, "such": {},
	"that": {}, "the": {}, "their": {}, "then": {}, "there": {}, "these": {},
	"they": {}, "this": {}, "to": {}, "was": {}, "will": {}, "with": {},
}

// Tokenize normalizes raw text into a slice of index terms:
//   - split on any non-letter/non-digit rune (strips punctuation),
//   - lower-case,
//   - drop stop-words and empty tokens.
//
// Order is preserved so callers can compute term frequencies directly.
func Tokenize(text string) []string {
	fields := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})

	tokens := make([]string, 0, len(fields))
	for _, f := range fields {
		term := strings.ToLower(f)
		if _, isStop := stopWords[term]; isStop {
			continue
		}
		tokens = append(tokens, term)
	}
	return tokens
}
