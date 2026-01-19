package retry

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"
)

var (
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
	ErrContextCancelled   = errors.New("context cancelled during retry")
)

// BackoffStrategy defines how to calculate delay between retries
type BackoffStrategy interface {
	// NextDelay returns the delay before the next retry attempt
	NextDelay(attempt int) time.Duration
	// Reset resets the backoff state
	Reset()
}

// ConstantBackoff uses a fixed delay between retries
type ConstantBackoff struct {
	Delay time.Duration
}

func NewConstantBackoff(delay time.Duration) *ConstantBackoff {
	return &ConstantBackoff{Delay: delay}
}

func (b *ConstantBackoff) NextDelay(attempt int) time.Duration {
	return b.Delay
}

func (b *ConstantBackoff) Reset() {}

// ExponentialBackoff increases delay exponentially with optional jitter
type ExponentialBackoff struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Jitter       float64 // 0.0 to 1.0 for jitter factor
}

func NewExponentialBackoff(initial, max time.Duration) *ExponentialBackoff {
	return &ExponentialBackoff{
		InitialDelay: initial,
		MaxDelay:     max,
		Multiplier:   2.0,
		Jitter:       0.1, // 10% jitter
	}
}

func (b *ExponentialBackoff) NextDelay(attempt int) time.Duration {
	delay := float64(b.InitialDelay) * math.Pow(b.Multiplier, float64(attempt))

	// Apply max cap
	if delay > float64(b.MaxDelay) {
		delay = float64(b.MaxDelay)
	}

	// Apply jitter
	if b.Jitter > 0 {
		jitter := delay * b.Jitter * (rand.Float64()*2 - 1) // -jitter to +jitter
		delay += jitter
	}

	return time.Duration(delay)
}

func (b *ExponentialBackoff) Reset() {}

// LinearBackoff increases delay linearly
type LinearBackoff struct {
	InitialDelay time.Duration
	Increment    time.Duration
	MaxDelay     time.Duration
}

func NewLinearBackoff(initial, increment, max time.Duration) *LinearBackoff {
	return &LinearBackoff{
		InitialDelay: initial,
		Increment:    increment,
		MaxDelay:     max,
	}
}

func (b *LinearBackoff) NextDelay(attempt int) time.Duration {
	delay := b.InitialDelay + time.Duration(attempt)*b.Increment
	if delay > b.MaxDelay {
		delay = b.MaxDelay
	}
	return delay
}

func (b *LinearBackoff) Reset() {}

// RetryConfig configures retry behavior
type RetryConfig struct {
	MaxAttempts int
	Backoff     BackoffStrategy
	// RetryableErrors is a function that returns true if the error should be retried
	RetryableErrors func(error) bool
}

// DefaultRetryConfig returns sensible defaults
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts: 3,
		Backoff:     NewExponentialBackoff(100*time.Millisecond, 5*time.Second),
		RetryableErrors: func(err error) bool {
			// By default, retry all errors
			return err != nil
		},
	}
}

// Retryer handles retry logic with backoff
type Retryer struct {
	config *RetryConfig
}

// NewRetryer creates a new retryer
func NewRetryer(config *RetryConfig) *Retryer {
	if config == nil {
		config = DefaultRetryConfig()
	}
	return &Retryer{config: config}
}

// RetryResult contains the result of a retry operation
type RetryResult struct {
	Attempts int
	LastErr  error
	Success  bool
}

// Do executes the operation with retry logic
func (r *Retryer) Do(ctx context.Context, operation func() error) *RetryResult {
	result := &RetryResult{}

	for attempt := 0; attempt < r.config.MaxAttempts; attempt++ {
		result.Attempts = attempt + 1

		// Check context before each attempt
		select {
		case <-ctx.Done():
			result.LastErr = ErrContextCancelled
			return result
		default:
		}

		// Execute operation
		err := operation()
		if err == nil {
			result.Success = true
			return result
		}

		result.LastErr = err

		// Check if error is retryable
		if !r.config.RetryableErrors(err) {
			return result
		}

		// Don't wait after the last attempt
		if attempt < r.config.MaxAttempts-1 {
			delay := r.config.Backoff.NextDelay(attempt)
			select {
			case <-ctx.Done():
				result.LastErr = ErrContextCancelled
				return result
			case <-time.After(delay):
				// Continue to next attempt
			}
		}
	}

	result.LastErr = ErrMaxRetriesExceeded
	return result
}

// DoWithResult executes an operation that returns a value
func DoWithResult[T any](ctx context.Context, r *Retryer, operation func() (T, error)) (T, *RetryResult) {
	var result T
	retryResult := &RetryResult{}

	for attempt := 0; attempt < r.config.MaxAttempts; attempt++ {
		retryResult.Attempts = attempt + 1

		select {
		case <-ctx.Done():
			retryResult.LastErr = ErrContextCancelled
			return result, retryResult
		default:
		}

		var err error
		result, err = operation()
		if err == nil {
			retryResult.Success = true
			return result, retryResult
		}

		retryResult.LastErr = err

		if !r.config.RetryableErrors(err) {
			return result, retryResult
		}

		if attempt < r.config.MaxAttempts-1 {
			delay := r.config.Backoff.NextDelay(attempt)
			select {
			case <-ctx.Done():
				retryResult.LastErr = ErrContextCancelled
				return result, retryResult
			case <-time.After(delay):
			}
		}
	}

	retryResult.LastErr = ErrMaxRetriesExceeded
	return result, retryResult
}

// CircuitBreaker implements circuit breaker pattern
type CircuitBreaker struct {
	failureThreshold int
	resetTimeout     time.Duration

	failures    int
	state       string // "closed", "open", "half-open"
	lastFailure time.Time
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(failureThreshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
		state:            "closed",
	}
}

// Execute runs the operation through the circuit breaker
func (cb *CircuitBreaker) Execute(operation func() error) error {
	if !cb.canExecute() {
		return errors.New("circuit breaker is open")
	}

	err := operation()
	cb.recordResult(err)
	return err
}

func (cb *CircuitBreaker) canExecute() bool {
	switch cb.state {
	case "closed":
		return true
	case "open":
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = "half-open"
			return true
		}
		return false
	case "half-open":
		return true
	}
	return false
}

func (cb *CircuitBreaker) recordResult(err error) {
	if err == nil {
		cb.failures = 0
		cb.state = "closed"
		return
	}

	cb.failures++
	cb.lastFailure = time.Now()

	if cb.failures >= cb.failureThreshold {
		cb.state = "open"
	}
}

// GetState returns the current circuit breaker state
func (cb *CircuitBreaker) GetState() string {
	return cb.state
}

// GetFailures returns the current failure count
func (cb *CircuitBreaker) GetFailures() int {
	return cb.failures
}
