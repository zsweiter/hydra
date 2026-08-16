package domain

import (
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

type ID ulid.ULID

var ZeroValue = ID{}

// entropy is a monotonic ULID entropy source. It is wrapped
// with a mutex because MonotonicEntropy is not thread-safe
// but still much faster than calling crypto/rand per ULID.
var entropy = &lockedEntropy{
	inner: ulid.Monotonic(rand.Reader, 0),
}

type lockedEntropy struct {
	mu    sync.Mutex
	inner io.Reader
}

func (e *lockedEntropy) Read(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.inner.Read(p)
}

func NewID() (ID, error) {
	ms := ulid.Timestamp(time.Now().UTC())
	id, err := ulid.New(ms, entropy)
	if err != nil {
		return ID{}, err
	}

	return ID(id), nil
}

func MustNewID() ID {
	id, err := NewID()
	if err != nil {
		panic(err)
	}

	return id
}

func ParseID(s string) (ID, error) {
	return parseIDString(s)
}

// parseString accepts both the canonical base32 ULID encoding and the
// hex encoding produced by ID.String.
func parseIDString(s string) (ID, error) {
	if len(s) == hex.EncodedLen(len(ID{})) {
		raw, err := hex.DecodeString(s)
		if err != nil {
			return ID{}, err
		}

		var u ulid.ULID
		copy(u[:], raw)
		return ID(u), nil
	}

	parsed, err := ulid.Parse(s)
	if err != nil {
		return ID{}, err
	}

	return ID(parsed), nil
}

func MustParseID(s string) ID {
	id, err := ParseID(s)
	if err != nil {
		panic(err)
	}

	return id
}

func NewZero() ID {
	return ID(ulid.ULID{})
}

// String returns the string representation of the ULID.
func (id ID) String() string {
	u := ulid.ULID(id)
	return hex.EncodeToString(u[:])
}

// MarshalJSON implements the json.Marshaler interface.
func (id ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (id *ID) UnmarshalJSON(b []byte) error {
	var s string

	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	parsed, err := parseIDString(s)
	if err != nil {
		return fmt.Errorf("invalid ULID: %w", err)
	}

	*id = ID(parsed)
	return nil
}

// Value implements the driver.Valuer interface for database compatibility.
func (id ID) Value() (driver.Value, error) {
	return id.String(), nil
}

func (id ID) IsZero() bool {
	return id == ZeroValue
}

// Scan implements the sql.Scanner interface for database compatibility.
func (id *ID) Scan(value any) error {
	if value == nil {
		*id = ID{}
		return nil
	}

	switch v := value.(type) {

	case string:
		parsed, err := parseIDString(v)
		if err != nil {
			return fmt.Errorf("failed to parse ULID string: %w", err)
		}
		*id = parsed
		return nil

	case []byte:
		parsed, err := parseIDString(string(v))
		if err != nil {
			return fmt.Errorf("failed to parse ULID bytes: %w", err)
		}
		*id = parsed
		return nil

	default:
		return fmt.Errorf("unsupported type for ULID: %T", value)
	}
}

func (id ID) Equals(other ID) bool {
	return id == other
}

func FromString(s string) (ID, error) {
	return parseIDString(s)
}

func FromStringPtr(s *string) *ID {
	if s == nil {
		return nil
	}

	id, err := FromString(*s)
	if err != nil {
		return nil
	}

	return &id
}

func IfNil(id *ID, defaultID ID) ID {
	if id == nil {
		return defaultID
	}
	return *id
}

func IsZero(id ID) bool {
	return ulid.ULID(id) == ulid.ULID{}
}
