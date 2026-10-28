package bridge

import (
	"errors"
	"slices"
	"strconv"
	"strings"
)

// The headers of a schedule's occurrences are stored once, as a
// template, and decoded on every occurrence. The encoding is
// plain text, because schedule metadata is stored as JSON
// (Postgres rejects NUL in it, and JSON mangles invalid UTF-8),
// and length-prefixed, so values may hold any character:
//
//	<groups>,<values>;  then per key: <len>:<key><count>;  then per value: <len>:<value>
//
// Decoding slices the stored string, so keys and values cost no
// allocation: an occurrence pays one map and one backing slice.

var errTemplate = errors.New("bridge: malformed header template")

// encodeTemplate encodes h, keys sorted so equal headers encode
// equally.
func encodeTemplate(h Header) string {
	keys := make([]string, 0, len(h))
	values := 0
	for k, v := range h {
		keys = append(keys, k)
		values += len(v)
	}
	slices.Sort(keys)

	var b strings.Builder
	b.WriteString(strconv.Itoa(len(keys)))
	b.WriteByte(',')
	b.WriteString(strconv.Itoa(values))
	b.WriteByte(';')
	field := func(s string) {
		b.WriteString(strconv.Itoa(len(s)))
		b.WriteByte(':')
		b.WriteString(s)
	}
	for _, k := range keys {
		field(k)
		b.WriteString(strconv.Itoa(len(h[k])))
		b.WriteByte(';')
		for _, v := range h[k] {
			field(v)
		}
	}
	return b.String()
}

// decodeTemplate decodes s into a header with room for extra
// more single-valued keys, and returns the values those keys
// can take, one slot each (see Header.setSlot).
func decodeTemplate(s string, extra int) (Header, []string, error) {
	groups, rest, ok := number(s, ',')
	if !ok {
		return nil, nil, errTemplate
	}
	values, rest, ok := number(rest, ';')
	if !ok {
		return nil, nil, errTemplate
	}

	h := make(Header, groups+extra)
	vals := make([]string, values+extra)
	used := 0
	for range groups {
		var k string
		if k, rest, ok = field(rest); !ok {
			return nil, nil, errTemplate
		}
		var n int
		if n, rest, ok = number(rest, ';'); !ok || used+n > values {
			return nil, nil, errTemplate
		}
		for i := range n {
			if vals[used+i], rest, ok = field(rest); !ok {
				return nil, nil, errTemplate
			}
		}
		h[k] = vals[used : used+n : used+n]
		used += n
	}
	if rest != "" || used != values {
		return nil, nil, errTemplate
	}
	return h, vals[values:], nil
}

// number reads a decimal number ended by sep.
func number(s string, sep byte) (int, string, bool) {
	i := strings.IndexByte(s, sep)
	if i <= 0 {
		return 0, s, false
	}
	n, err := strconv.Atoi(s[:i])
	if err != nil || n < 0 {
		return 0, s, false
	}
	return n, s[i+1:], true
}

// field reads <len>:<bytes>.
func field(s string) (string, string, bool) {
	n, rest, ok := number(s, ':')
	if !ok || n > len(rest) {
		return "", s, false
	}
	return rest[:n], rest[n:], true
}

// setSlot sets key to value in a header whose key is known not
// to be there in another case, using one of the slots returned
// by decodeTemplate instead of allocating.
func (h Header) setSlot(key, value string, slots []string) []string {
	slots[0] = value
	h[key] = slots[:1:1]
	return slots[1:]
}
