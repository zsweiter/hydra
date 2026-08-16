package domain

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"strings"
)

// enumString returns the display name for a numeric enum value,
// falling back to the unknown name for out-of-range values.
//
//go:inline
func enumString[T ~uint8](v T, names []string, unknown string) string {
	if int(v) < len(names) {
		return names[int(v)]
	}
	return unknown
}

// enumIsValid reports whether v is one of the defined enum values.
//
//go:inline
func enumIsValid[T ~uint8](v T, names []string) bool {
	return int(v) < len(names)
}

// enumParse converts a string into an enum value.
//
// Matching is case-insensitive and trims surrounding whitespace.
// It returns an error when the string does not match any defined
// value.
func enumParse[T ~uint8](names []string, s string) (T, error) {
	norm := strings.ToLower(strings.TrimSpace(s))

	for i, name := range names {
		if name == norm {
			return T(i), nil
		}
	}

	return 0, fmt.Errorf("unknown %T value %q", *new(T), s)
}

// enumMarshalJSON encodes an enum value as its display name.
func enumMarshalJSON[T ~uint8](v T, names []string, unknown string) ([]byte, error) {
	return []byte(`"` + enumString(v, names, unknown) + `"`), nil
}

// enumUnmarshalJSON decodes an enum value from its display name.
func enumUnmarshalJSON[T ~uint8](v *T, names []string, data []byte) error {
	parsed, err := enumParse[T](names, string(bytes.Trim(data, `"`)))
	if err != nil {
		return err
	}

	*v = parsed
	return nil
}

// enumValue encodes an enum value as its display name for persistence.
func enumValue[T ~uint8](v T, names []string, unknown string) (driver.Value, error) {
	return enumString(v, names, unknown), nil
}

// enumScan decodes an enum value from its persisted form.
//
// It accepts text values (string, []byte) as produced by enumValue,
// as well as integer values for backwards compatibility with rows
// persisted before text storage.
func enumScan[T ~uint8](v *T, names []string, value any) error {
	if value == nil {
		return fmt.Errorf("cannot scan NULL into %T", *v)
	}

	switch x := value.(type) {
	case string:
		parsed, err := enumParse[T](names, x)
		if err != nil {
			return err
		}
		*v = parsed
	case []byte:
		parsed, err := enumParse[T](names, string(x))
		if err != nil {
			return err
		}
		*v = parsed
	case int64:
		if x < 0 || int(x) >= len(names) {
			return fmt.Errorf("invalid %T value %d", *v, x)
		}
		*v = T(x)
	case int:
		if x < 0 || x >= len(names) {
			return fmt.Errorf("invalid %T value %d", *v, x)
		}
		*v = T(x)
	default:
		return fmt.Errorf("unsupported type for %T scan: %T", *v, value)
	}

	return nil
}
