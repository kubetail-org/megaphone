package megaphone

import "errors"

// ErrClosed is returned when attempting to subscribe to a closed Megaphone instance.
var ErrClosed = errors.New("megaphone: closed")
