package transport

import "time"

// RetryPolicy controls how many attempts a failed group gets and how long the
// dispatcher waits between attempts.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// DefaultRetryPolicy is the policy used by the demo server.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    200 * time.Millisecond,
	}
}

// NextDelay computes the exponential backoff for a given attempt number.
func (p RetryPolicy) NextDelay(attempt int) time.Duration {
	if p.BaseDelay <= 0 {
		return 0
	}
	delay := p.BaseDelay
	for i := 1; i < attempt; i++ {
		delay *= 2
		if p.MaxDelay > 0 && delay >= p.MaxDelay {
			return p.MaxDelay
		}
	}
	return delay
}

// WithAttempts returns a copy of the policy with a different attempt budget.
func (p RetryPolicy) WithAttempts(attempts int) RetryPolicy {
	p.MaxAttempts = attempts
	return p
}
