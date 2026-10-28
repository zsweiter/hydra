package parsers

import (
	"sync"
	"time"
)

// locations caches time zones by name. time.LoadLocation reads
// and parses the zone file on every call (several KB each), so
// a million schedules in one zone would hold a million copies.
var locations sync.Map // string -> *time.Location

// LoadLocation is time.LoadLocation, cached: every rule of a
// zone shares one *time.Location.
func LoadLocation(name string) (*time.Location, error) {
	if loc, ok := locations.Load(name); ok {
		return loc.(*time.Location), nil
	}

	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}

	actual, _ := locations.LoadOrStore(name, loc)
	return actual.(*time.Location), nil
}
