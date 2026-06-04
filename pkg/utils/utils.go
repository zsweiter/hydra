package utils

import "reflect"

// When returns `a` if cond is true, otherwise `b`.
// Works like a ternary operator in other languages.
func When[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

// WhenFunc evaluates and returns the result of `a()` if cond is true,
// otherwise it evaluates and returns `b()`. Useful to avoid evaluating
// both branches if they are expensive operations.
func WhenFunc[T any](cond bool, a, b func() T) T {
	if cond {
		return a()
	}
	return b()
}

func IfNull[T any](v T, def T) T {
	if reflect.ValueOf(v).IsZero() {
		return def
	}

	return v
}

func IfNil[T any](v *T, def T) T {
	if v == nil {
		return def
	}

	return *v
}

func WhenErr[T any](cond bool, a, b func() (T, error)) (T, error) {
	if cond {
		return a()
	}
	return b()
}

func Optional[T any](ptr *T) any {
	if ptr == nil {
		return nil
	}
	return *ptr
}

func Coalesce[T any](values ...T) T {
	for _, v := range values {
		if !reflect.ValueOf(v).IsZero() {
			return v
		}
	}
	var zero T
	return zero
}
