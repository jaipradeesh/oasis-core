package abci

import (
	"errors"
	"fmt"
)

type errorUnavailableState struct {
	inner error
}

func (e *errorUnavailableState) Error() string {
	return fmt.Sprintf("unavailable/corrupted state: %s", e.inner.Error())
}

func (e *errorUnavailableState) Unwrap() error {
	return e.inner
}

// UnavailableStateError wraps an error in an unavailable state error.
func UnavailableStateError(err error) error {
	if err == nil {
		return nil
	}
	return &errorUnavailableState{err}
}

// IsUnavailableStateError returns true if any error in err's chain is an unavailable state error.
func IsUnavailableStateError(err error) bool {
	var e *errorUnavailableState
	return errors.As(err, &e)
}
