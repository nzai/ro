package ro

import "errors"

var (
	ErrBadRequest         = errors.New("bad request")
	ErrInvalidResultCount = errors.New("invalid result count")
	ErrConfigUndefined    = errors.New("redis config undefined")
	ErrRecordNotFound     = errors.New("record not found")
)
