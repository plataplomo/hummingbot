"""WebSocket processing models for type-safe metrics.

This module contains models for WebSocket message processing metrics
and processor monitoring.
"""

from __future__ import annotations

import time

from pydantic import BaseModel, Field


class ProcessingMetrics(BaseModel):
    """Type-safe metrics for WebSocket message processing.

    Replaces dict-based metrics collection with fully typed model,
    ensuring type safety and providing computed fields for derived metrics.
    """

    total_processed: int = Field(default=0, ge=0, description="Total number of messages processed")

    validation_errors: int = Field(
        default=0, ge=0, description="Number of validation errors encountered"
    )

    transformation_errors: int = Field(
        default=0, ge=0, description="Number of transformation errors encountered"
    )

    handler_errors: int = Field(default=0, ge=0, description="Number of handler errors encountered")

    total_processing_time_seconds: float = Field(
        default=0.0, ge=0.0, description="Total time spent processing messages in seconds"
    )

    start_timestamp: float = Field(
        default_factory=time.time, description="Timestamp when metrics collection started"
    )

    def get_uptime_seconds(self) -> float:
        """Calculate uptime in seconds.

        Returns:
            Current uptime in seconds since metrics collection started.
        """
        return time.time() - self.start_timestamp

    def get_average_processing_time_ms(self) -> float:
        """Calculate average processing time per message in milliseconds.

        Returns:
            Average processing time in milliseconds, or 0 if no messages processed.
        """
        if self.total_processed == 0:
            return 0.0
        return (self.total_processing_time_seconds / self.total_processed) * 1000

    def get_messages_per_second(self) -> float:
        """Calculate message processing rate.

        Returns:
            Messages processed per second, or 0 if no uptime.
        """
        uptime = self.get_uptime_seconds()
        if uptime == 0:
            return 0.0
        return self.total_processed / uptime

    def get_error_rate(self) -> float:
        """Calculate overall error rate.

        Returns:
            Ratio of errors to total messages processed (0.0 to 1.0).
        """
        if self.total_processed == 0:
            return 0.0
        total_errors = self.validation_errors + self.transformation_errors + self.handler_errors
        return total_errors / self.total_processed

    def get_total_errors(self) -> int:
        """Calculate total number of errors.

        Returns:
            Sum of all error types.
        """
        return self.validation_errors + self.transformation_errors + self.handler_errors

    def get_success_rate(self) -> float:
        """Calculate success rate.

        Returns:
            Ratio of successful messages to total processed (0.0 to 1.0).
        """
        return 1.0 - self.get_error_rate()

    def record_processing_time(self, processing_time_seconds: float) -> None:
        """Record processing time for a message.

        Args:
            processing_time_seconds: Time taken to process message in seconds.
        """
        self.total_processed += 1
        self.total_processing_time_seconds += processing_time_seconds

    def record_validation_error(self) -> None:
        """Record a validation error."""
        self.validation_errors += 1

    def record_transformation_error(self) -> None:
        """Record a transformation error."""
        self.transformation_errors += 1

    def record_handler_error(self) -> None:
        """Record a handler error."""
        self.handler_errors += 1

    def reset(self) -> ProcessingMetrics:
        """Reset all metrics counters.

        Returns:
            New ProcessingMetrics instance with reset values.
        """
        return ProcessingMetrics()

    def merge(self, other: ProcessingMetrics) -> ProcessingMetrics:
        """Merge metrics from another instance.

        Args:
            other: Another ProcessingMetrics instance to merge.

        Returns:
            New ProcessingMetrics instance with merged values.
        """
        # Use the earlier start timestamp
        start_timestamp = min(self.start_timestamp, other.start_timestamp)

        return ProcessingMetrics(
            total_processed=self.total_processed + other.total_processed,
            validation_errors=self.validation_errors + other.validation_errors,
            transformation_errors=self.transformation_errors + other.transformation_errors,
            handler_errors=self.handler_errors + other.handler_errors,
            total_processing_time_seconds=(
                self.total_processing_time_seconds + other.total_processing_time_seconds
            ),
            start_timestamp=start_timestamp,
        )


class ProcessorMetrics(BaseModel):
    """Complete metrics for a WebSocket processor instance.

    Includes processing metrics plus processor metadata for comprehensive monitoring.
    """

    processor_name: str = Field(description="Name of the processor")
    raw_model_name: str = Field(description="Name of the raw model class")
    transformer_type: str = Field(description="Type of transformer used")
    processing_metrics: ProcessingMetrics = Field(description="Processing metrics data")

    @classmethod
    def from_processor(
        cls,
        processor_name: str,
        raw_model_name: str,
        transformer_type: str,
        processing_metrics: ProcessingMetrics,
    ) -> ProcessorMetrics:
        """Create processor metrics from components.

        Args:
            processor_name: Name of the processor
            raw_model_name: Name of the raw model class
            transformer_type: Type of transformer used
            processing_metrics: Processing metrics data

        Returns:
            ProcessorMetrics instance with all data.
        """
        return cls(
            processor_name=processor_name,
            raw_model_name=raw_model_name,
            transformer_type=transformer_type,
            processing_metrics=processing_metrics,
        )
