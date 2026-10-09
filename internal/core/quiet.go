package core

import "time"

// QuietDue is the debounce deadline shared by the rpc batcher and the herdr
// done batch: a burst is due at quiet after its newest record, or at retryAt
// when a previous attempt has to be retried later, whichever is later. A zero
// retryAt never delays the quiet window, so a fresh burst - and a restart with
// pending records - fires exactly quiet after its newest record.
func QuietDue(last, retryAt time.Time, quiet time.Duration) time.Time {
	due := last.Add(quiet)
	if retryAt.After(due) {
		return retryAt
	}
	return due
}
