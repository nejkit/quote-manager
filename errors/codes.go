package errors

import "errors"

var (
	ErrorNotFound        = errors.New("NotFound")
	ErrorInternal        = errors.New("InternalError")
	ErrorNonceExpired    = errors.New("ExpiredNonce")
	ErrorParalelLocks    = errors.New("ParralelLocks")
	ErrorBlockedResource = errors.New("BlockedResource")
)
