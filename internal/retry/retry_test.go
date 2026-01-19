package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConstantBackoff(t *testing.T) {
	backoff := NewConstantBackoff(100 * time.Millisecond)

	for i := 0; i < 5; i++ {
		delay := backoff.NextDelay(i)
		if delay != 100*time.Millisecond {
			t.Errorf("Expected 100ms, got %v", delay)
		}
	}
}

func TestExponentialBackoff(t *testing.T) {
	backoff := &ExponentialBackoff{
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     1 * time.Second,
		Multiplier:   2.0,
		Jitter:       0, // No jitter for predictable tests
	}

	expected := []time.Duration{
		100 * time.Millisecond,  // 100 * 2^0
		200 * time.Millisecond,  // 100 * 2^1
		400 * time.Millisecond,  // 100 * 2^2
		800 * time.Millisecond,  // 100 * 2^3
		1000 * time.Millisecond, // capped at max
	}

	for i, exp := range expected {
		delay := backoff.NextDelay(i)
		if delay != exp {
			t.Errorf("Attempt %d: expected %v, got %v", i, exp, delay)
		}
	}
}

func TestLinearBackoff(t *testing.T) {
	backoff := NewLinearBackoff(100*time.Millisecond, 50*time.Millisecond, 300*time.Millisecond)

	expected := []time.Duration{
		100 * time.Millisecond, // 100 + 0*50
		150 * time.Millisecond, // 100 + 1*50
		200 * time.Millisecond, // 100 + 2*50
		250 * time.Millisecond, // 100 + 3*50
		300 * time.Millisecond, // capped at max
	}

	for i, exp := range expected {
		delay := backoff.NextDelay(i)
		if delay != exp {
			t.Errorf("Attempt %d: expected %v, got %v", i, exp, delay)
		}
	}
}

func TestRetryer_Success(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts: 3,
		Backoff:     NewConstantBackoff(10 * time.Millisecond),
		RetryableErrors: func(err error) bool {
			return err != nil
		},
	}
	retryer := NewRetryer(config)

	attempts := 0
	result := retryer.Do(context.Background(), func() error {
		attempts++
		return nil // Success on first try
	})

	if !result.Success {
		t.Fatal("Expected success")
	}
	if result.Attempts != 1 {
		t.Errorf("Expected 1 attempt, got %d", result.Attempts)
	}
	if attempts != 1 {
		t.Errorf("Expected operation called once, got %d", attempts)
	}
}

func TestRetryer_SuccessAfterRetries(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts: 5,
		Backoff:     NewConstantBackoff(10 * time.Millisecond),
		RetryableErrors: func(err error) bool {
			return err != nil
		},
	}
	retryer := NewRetryer(config)

	attempts := 0
	result := retryer.Do(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary error")
		}
		return nil // Success on third try
	})

	if !result.Success {
		t.Fatal("Expected success")
	}
	if result.Attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", result.Attempts)
	}
}

func TestRetryer_MaxRetriesExceeded(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts: 3,
		Backoff:     NewConstantBackoff(10 * time.Millisecond),
		RetryableErrors: func(err error) bool {
			return err != nil
		},
	}
	retryer := NewRetryer(config)

	attempts := 0
	result := retryer.Do(context.Background(), func() error {
		attempts++
		return errors.New("permanent error")
	})

	if result.Success {
		t.Fatal("Expected failure")
	}
	if result.Attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", result.Attempts)
	}
	if !errors.Is(result.LastErr, ErrMaxRetriesExceeded) {
		t.Errorf("Expected ErrMaxRetriesExceeded, got %v", result.LastErr)
	}
}

func TestRetryer_NonRetryableError(t *testing.T) {
	nonRetryableErr := errors.New("non-retryable")

	config := &RetryConfig{
		MaxAttempts: 5,
		Backoff:     NewConstantBackoff(10 * time.Millisecond),
		RetryableErrors: func(err error) bool {
			return err != nil && err != nonRetryableErr
		},
	}
	retryer := NewRetryer(config)

	attempts := 0
	result := retryer.Do(context.Background(), func() error {
		attempts++
		return nonRetryableErr
	})

	if result.Success {
		t.Fatal("Expected failure")
	}
	if result.Attempts != 1 {
		t.Errorf("Expected 1 attempt (no retry), got %d", result.Attempts)
	}
}

func TestRetryer_ContextCancellation(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts: 10,
		Backoff:     NewConstantBackoff(100 * time.Millisecond),
		RetryableErrors: func(err error) bool {
			return err != nil
		},
	}
	retryer := NewRetryer(config)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	result := retryer.Do(ctx, func() error {
		return errors.New("error")
	})

	if result.Success {
		t.Fatal("Expected failure due to cancellation")
	}
	if !errors.Is(result.LastErr, ErrContextCancelled) {
		t.Errorf("Expected ErrContextCancelled, got %v", result.LastErr)
	}
}

func TestCircuitBreaker_Closed(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)

	// Should start closed
	if cb.GetState() != "closed" {
		t.Errorf("Expected closed, got %s", cb.GetState())
	}

	// Should execute successfully
	err := cb.Execute(func() error {
		return nil
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestCircuitBreaker_OpensAfterFailures(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)

	// Fail 3 times
	for i := 0; i < 3; i++ {
		cb.Execute(func() error {
			return errors.New("error")
		})
	}

	// Circuit should be open
	if cb.GetState() != "open" {
		t.Errorf("Expected open, got %s", cb.GetState())
	}

	// Execution should be rejected
	err := cb.Execute(func() error {
		return nil
	})
	if err == nil {
		t.Fatal("Expected error when circuit is open")
	}
}

func TestCircuitBreaker_ResetsAfterSuccess(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)

	// Fail 2 times (not enough to open)
	for i := 0; i < 2; i++ {
		cb.Execute(func() error {
			return errors.New("error")
		})
	}

	if cb.GetFailures() != 2 {
		t.Errorf("Expected 2 failures, got %d", cb.GetFailures())
	}

	// Success should reset counter
	cb.Execute(func() error {
		return nil
	})

	if cb.GetFailures() != 0 {
		t.Errorf("Expected 0 failures after success, got %d", cb.GetFailures())
	}
}
