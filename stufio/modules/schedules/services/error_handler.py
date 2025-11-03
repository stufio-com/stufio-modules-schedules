"""
Error handling and retry mechanisms for the schedules system.
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Callable
from enum import Enum

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    """Available retry strategies."""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_INTERVAL = "fixed_interval"
    IMMEDIATE = "immediate"


class ErrorType(Enum):
    """Types of errors that can occur during schedule execution."""
    KAFKA_CONNECTION_ERROR = "kafka_connection_error"
    KAFKA_PUBLISH_ERROR = "kafka_publish_error"
    REDIS_CONNECTION_ERROR = "redis_connection_error"
    CLICKHOUSE_CONNECTION_ERROR = "clickhouse_connection_error"
    SERIALIZATION_ERROR = "serialization_error"
    TIMEOUT_ERROR = "timeout_error"
    VALIDATION_ERROR = "validation_error"
    CIRCUIT_BREAKER_OPEN = "circuit_breaker_open"
    UNKNOWN_ERROR = "unknown_error"


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker for preventing cascading failures."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type = Exception
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitBreakerState.CLOSED
    
    async def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        if self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitBreakerState.HALF_OPEN
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            # Check if this is an expected exception type
            if isinstance(e, self.expected_exception):
                self._on_failure()
                raise e
            else:
                # Unexpected exception, re-raise without affecting circuit breaker
                raise e
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
        return (datetime.utcnow() - self.last_failure_time).seconds >= self.recovery_timeout
    
    def _on_success(self):
        """Handle successful execution."""
        self.failure_count = 0
        self.state = CircuitBreakerState.CLOSED
    
    def _on_failure(self):
        """Handle failed execution."""
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN


class RetryConfig:
    """Configuration for retry behavior."""
    
    def __init__(
        self,
        max_attempts: int = 3,
        strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
        base_delay: int = 60,  # seconds
        max_delay: int = 3600,  # seconds
        multiplier: float = 2.0,
        jitter: bool = True
    ):
        self.max_attempts = max_attempts
        self.strategy = strategy
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter = jitter
    
    def calculate_delay(self, attempt: int) -> int:
        """Calculate delay for the given attempt number."""
        if self.strategy == RetryStrategy.IMMEDIATE:
            return 0
        elif self.strategy == RetryStrategy.FIXED_INTERVAL:
            delay = self.base_delay
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.base_delay * attempt
        elif self.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.base_delay * (self.multiplier ** (attempt - 1))
        else:
            delay = self.base_delay
        
        # Apply maximum delay limit
        delay = min(delay, self.max_delay)
        
        # Add jitter to prevent thundering herd
        if self.jitter:
            import random
            delay = delay + random.randint(0, int(delay * 0.1))
        
        return int(delay)


class ErrorClassifier:
    """Classifies errors and determines appropriate retry strategies."""
    
    ERROR_STRATEGIES = {
        ErrorType.KAFKA_CONNECTION_ERROR: RetryConfig(
            max_attempts=5,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            base_delay=30,
            max_delay=600
        ),
        ErrorType.KAFKA_PUBLISH_ERROR: RetryConfig(
            max_attempts=3,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            base_delay=10,
            max_delay=300
        ),
        ErrorType.REDIS_CONNECTION_ERROR: RetryConfig(
            max_attempts=3,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            base_delay=5,
            max_delay=60
        ),
        ErrorType.CLICKHOUSE_CONNECTION_ERROR: RetryConfig(
            max_attempts=3,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            base_delay=10,
            max_delay=120
        ),
        ErrorType.SERIALIZATION_ERROR: RetryConfig(
            max_attempts=1,  # Don't retry serialization errors
            strategy=RetryStrategy.IMMEDIATE
        ),
        ErrorType.TIMEOUT_ERROR: RetryConfig(
            max_attempts=2,
            strategy=RetryStrategy.LINEAR_BACKOFF,
            base_delay=60
        ),
        ErrorType.VALIDATION_ERROR: RetryConfig(
            max_attempts=1,  # Don't retry validation errors
            strategy=RetryStrategy.IMMEDIATE
        ),
        ErrorType.CIRCUIT_BREAKER_OPEN: RetryConfig(
            max_attempts=1,
            strategy=RetryStrategy.IMMEDIATE
        ),
        ErrorType.UNKNOWN_ERROR: RetryConfig(
            max_attempts=2,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            base_delay=60
        )
    }
    
    @classmethod
    def classify_error(cls, error: Exception) -> ErrorType:
        """Classify an error to determine its type."""
        error_str = str(error).lower()
        
        # Check for specific error patterns
        if "kafka" in error_str:
            if "connection" in error_str or "broker" in error_str:
                return ErrorType.KAFKA_CONNECTION_ERROR
            else:
                return ErrorType.KAFKA_PUBLISH_ERROR
        elif "redis" in error_str or "connection pool" in error_str:
            return ErrorType.REDIS_CONNECTION_ERROR
        elif "clickhouse" in error_str or "database" in error_str:
            return ErrorType.CLICKHOUSE_CONNECTION_ERROR
        elif "timeout" in error_str:
            return ErrorType.TIMEOUT_ERROR
        elif "json" in error_str or "serializ" in error_str:
            return ErrorType.SERIALIZATION_ERROR
        elif "validation" in error_str or "invalid" in error_str:
            return ErrorType.VALIDATION_ERROR
        elif "circuit breaker" in error_str:
            return ErrorType.CIRCUIT_BREAKER_OPEN
        else:
            return ErrorType.UNKNOWN_ERROR
    
    @classmethod
    def get_retry_config(cls, error_type: ErrorType) -> RetryConfig:
        """Get retry configuration for the given error type."""
        return cls.ERROR_STRATEGIES.get(error_type, cls.ERROR_STRATEGIES[ErrorType.UNKNOWN_ERROR])


class ErrorHandler:
    """Handles errors and manages retry logic for schedule operations."""
    
    def __init__(self):
        self.circuit_breakers = {}
        self.error_stats = {}
    
    def get_circuit_breaker(self, service_name: str) -> CircuitBreaker:
        """Get or create a circuit breaker for a service."""
        if service_name not in self.circuit_breakers:
            self.circuit_breakers[service_name] = CircuitBreaker()
        return self.circuit_breakers[service_name]
    
    async def execute_with_retry(
        self,
        func: Callable,
        *args,
        service_name: str = "default",
        custom_retry_config: Optional[RetryConfig] = None,
        **kwargs
    ):
        """Execute a function with automatic retry and circuit breaker protection."""
        circuit_breaker = self.get_circuit_breaker(service_name)
        last_error = None
        
        for attempt in range(1, (custom_retry_config.max_attempts if custom_retry_config else 3) + 1):
            try:
                return await circuit_breaker.call(func, *args, **kwargs)
            
            except Exception as error:
                last_error = error
                error_type = ErrorClassifier.classify_error(error)
                retry_config = custom_retry_config or ErrorClassifier.get_retry_config(error_type)
                
                # Update error statistics
                self._update_error_stats(service_name, error_type)
                
                # Log the error
                logger.warning(
                    f"Attempt {attempt}/{retry_config.max_attempts} failed for {service_name}: "
                    f"{error_type.value} - {str(error)}"
                )
                
                # Don't retry if this was the last attempt
                if attempt >= retry_config.max_attempts:
                    logger.error(
                        f"All {retry_config.max_attempts} attempts failed for {service_name}. "
                        f"Final error: {str(error)}"
                    )
                    break
                
                # Calculate delay and wait
                delay = retry_config.calculate_delay(attempt)
                if delay > 0:
                    logger.info(f"Retrying {service_name} in {delay} seconds...")
                    await asyncio.sleep(delay)
        
        # If we get here, all attempts failed
        if last_error:
            raise last_error
        else:
            raise RuntimeError(f"Operation failed for {service_name} with no recorded error")
    
    def _update_error_stats(self, service_name: str, error_type: ErrorType):
        """Update error statistics for monitoring."""
        if service_name not in self.error_stats:
            self.error_stats[service_name] = {}
        
        if error_type.value not in self.error_stats[service_name]:
            self.error_stats[service_name][error_type.value] = {
                "count": 0,
                "last_occurrence": None
            }
        
        self.error_stats[service_name][error_type.value]["count"] += 1
        self.error_stats[service_name][error_type.value]["last_occurrence"] = datetime.utcnow()
    
    def get_error_stats(self) -> Dict[str, Any]:
        """Get error statistics for monitoring."""
        return self.error_stats
    
    def get_circuit_breaker_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all circuit breakers."""
        status = {}
        for service_name, breaker in self.circuit_breakers.items():
            status[service_name] = {
                "state": breaker.state.value,
                "failure_count": breaker.failure_count,
                "last_failure_time": breaker.last_failure_time.isoformat() if breaker.last_failure_time else None
            }
        return status
    
    def reset_circuit_breaker(self, service_name: str) -> bool:
        """Manually reset a circuit breaker."""
        if service_name in self.circuit_breakers:
            breaker = self.circuit_breakers[service_name]
            breaker.state = CircuitBreakerState.CLOSED
            breaker.failure_count = 0
            breaker.last_failure_time = None
            logger.info(f"Circuit breaker for {service_name} has been reset")
            return True
        return False


# Global error handler instance
error_handler = ErrorHandler()
