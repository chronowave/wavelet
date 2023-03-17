package server

import "errors"

var (
	ErrInvalidArrowFlightSchema        = errors.New("invalid arrow flight schema format")
	ErrInvalidArrowFlightConfiguration = errors.New("invalid arrow flight server configuration")

	ErrMissingFlightDescriptor   = errors.New("missing flight path descriptor")
	ErrWrongFlightDescriptorPath = errors.New("expect one flight descriptor path")
	ErrEmptyFlightData           = errors.New("flight data body is empty")
)
