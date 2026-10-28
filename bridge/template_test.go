package bridge

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"
)

func TestTemplateRoundTrip(t *testing.T) {
	cases := []Header{
		{},
		{"ce-type": {"com.acme.charge"}, "content-type": {"application/json"}},
		{"x-multi": {"a", "", "c"}, "x-empty": {""}, "ce-source": {"/billing"}},
		{"x-odd": {"1:2;3,4", "ñandú 🦤", "line\nbreak"}, "9": {"0:"}},
	}
	for _, h := range cases {
		s := encodeTemplate(h)

		// It is stored inside JSON metadata: it must survive it.
		raw, err := json.Marshal(map[string]string{"h": s})
		if err != nil {
			t.Fatal(err)
		}
		var back map[string]string
		if err := json.Unmarshal(raw, &back); err != nil || back["h"] != s {
			t.Fatalf("%q does not survive JSON: %q", s, back["h"])
		}

		got, slots, err := decodeTemplate(s, 2)
		if err != nil {
			t.Fatalf("decode %q: %v", s, err)
		}
		if len(slots) != 2 {
			t.Fatalf("%d slots, want 2", len(slots))
		}
		if !maps.EqualFunc(got, h, slices.Equal) {
			t.Fatalf("decoded %v, want %v", got, h)
		}

		// Slots and template values do not alias.
		slots = got.setSlot("ce-id", "a", slots)
		got.setSlot("ce-time", "b", slots)
		if got.Get("ce-id") != "a" || got.Get("ce-time") != "b" {
			t.Fatalf("slots: %v", got)
		}
		for k, v := range h {
			if !slices.Equal(got[k], v) {
				t.Fatalf("setting slots changed %s: %v", k, got[k])
			}
		}
	}
}

func TestTemplateMalformed(t *testing.T) {
	good := encodeTemplate(Header{"a": {"b"}})
	for _, s := range []string{"", "x", "1,1;", "1,1;1:a1;5:b", "1,2;1:a1;1:b", good + "x", "{\"a\":[\"b\"]}"} {
		if _, _, err := decodeTemplate(s, 0); err == nil {
			t.Errorf("%q decoded", s)
		}
	}
}
