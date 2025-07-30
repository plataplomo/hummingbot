"""Check pipeline orchestrator implementation with TypedBaseChecker support."""

import asyncio

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.checks.checkers.typed_base_checker import TypedBaseChecker
from cyberdelta.core.risk.checks.interfaces.check_interfaces import (
    BaseCheckerInterface,
)
from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Type alias for checkers that can be used in the pipeline
PipelineChecker = BaseCheckerInterface | TypedBaseChecker[CheckResult]


class CheckPipeline:
    """Orchestrates the execution of multiple checkers in a pipeline."""

    def __init__(self, checkers: list[PipelineChecker] | None = None) -> None:
        """Initialize the check pipeline.

        Args:
            checkers: List of checkers to include in the pipeline
        """
        self.checkers: list[PipelineChecker] = checkers or []
        self.logger = get_logger(self.__class__.__name__)

        # Pipeline configuration
        self.stop_on_first_failure = True
        self.max_concurrent_checks = 5
        self.timeout_seconds = 30.0

    async def run_pipeline(
        self,
        opportunity: ArbitrageOpportunity,
        checkers: list[PipelineChecker] | None = None,
    ) -> CheckResult:
        """Run a pipeline of checkers against an opportunity.

        Args:
            opportunity: The arbitrage opportunity to check
            checkers: Optional list of checkers to use (defaults to self.checkers)

        Returns:
            CheckResult indicating overall pipeline success/failure
        """
        checkers_to_run = checkers or self.checkers

        if not checkers_to_run:
            return CheckResult.success(
                message="No checkers configured - pipeline passed",
                details={"checkers_count": 0},
            )

        self.logger.info("Running pipeline", checker_count=len(checkers_to_run))

        try:
            # Execute checkers and collect results
            results, counts = await self._execute_checkers(checkers_to_run, opportunity)

            # Build pipeline details and determine result
            return self._build_pipeline_result(checkers_to_run, results, counts)

        except TimeoutError:
            return CheckResult.error(
                message=f"Pipeline timed out after {self.timeout_seconds} seconds",
                details={
                    "timeout_seconds": self.timeout_seconds,
                    "checkers_count": len(checkers_to_run),
                },
            )
        except Exception as e:
            self.logger.exception("Unexpected error in pipeline")
            return CheckResult.error(
                message=f"Pipeline failed with unexpected error: {e!s}",
                details={"exception": str(e), "checkers_count": len(checkers_to_run)},
            )

    async def _execute_checkers(
        self, checkers_to_run: list[PipelineChecker], opportunity: ArbitrageOpportunity
    ) -> tuple[list[CheckResult], dict[str, int]]:
        """Execute checkers and return results with counts.

        Returns:
            Tuple of (list of check results, dictionary of result counts).
        """
        results: list[CheckResult] = []
        counts = {"passed": 0, "failed": 0, "skipped": 0, "error": 0}

        if self.stop_on_first_failure:
            # Sequential execution with early stopping
            for checker in checkers_to_run:
                context = self._create_check_context(checker, opportunity)
                result = await asyncio.wait_for(
                    checker.check(opportunity, context),
                    timeout=self.timeout_seconds,
                )
                results.append(result)
                self._update_counts(result, counts)

                # Stop on failure or error
                if result.failed or result.has_error:
                    break
        else:
            # Concurrent execution
            tasks: list[asyncio.Task[CheckResult]] = []
            for checker in checkers_to_run:
                context = self._create_check_context(checker, opportunity)
                task = asyncio.create_task(
                    asyncio.wait_for(
                        checker.check(opportunity, context),
                        timeout=self.timeout_seconds,
                    ),
                )
                tasks.append(task)

            # Wait for all tasks to complete
            gather_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process concurrent results
            for i, gather_result in enumerate(gather_results):
                result = self._process_gather_result(gather_result, checkers_to_run[i])
                results.append(result)
                self._update_counts(result, counts)

        return results, counts

    def _create_check_context(
        self, checker: PipelineChecker, opportunity: ArbitrageOpportunity
    ) -> CheckContext:
        """Create check context for a checker.

        Returns:
            CheckContext instance for the given checker and opportunity.
        """
        return CheckContext(
            check_name=checker.name,
            config={"pipeline": True},
            metadata={"opportunity_symbol": getattr(opportunity, "symbol", None)},
        )

    def _update_counts(self, result: CheckResult, counts: dict[str, int]) -> None:
        """Update result counts based on check result."""
        if result.passed:
            counts["passed"] += 1
        elif result.failed:
            counts["failed"] += 1
        elif result.skipped:
            counts["skipped"] += 1
        elif result.has_error:
            counts["error"] += 1

    def _process_gather_result(
        self, gather_result: CheckResult | BaseException, checker: PipelineChecker
    ) -> CheckResult:
        """Process a result from asyncio.gather.

        Returns:
            CheckResult, either the original result or an error result if an exception occurred.
        """
        if isinstance(gather_result, BaseException):
            return CheckResult.error(
                message=(f"Checker {checker.name} failed with exception: {gather_result!s}"),
                details={"checker": checker.name, "exception": str(gather_result)},
            )
        # gather_result must be CheckResult at this point due to type annotation
        return gather_result

    def _build_pipeline_result(
        self,
        checkers_to_run: list[PipelineChecker],
        results: list[CheckResult],
        counts: dict[str, int],
    ) -> CheckResult:
        """Build the final pipeline result.

        Returns:
            CheckResult representing the overall pipeline execution result.
        """
        pipeline_details = {
            "total_checks": len(results),
            "passed": counts["passed"],
            "failed": counts["failed"],
            "skipped": counts["skipped"],
            "errors": counts["error"],
            "stop_on_first_failure": self.stop_on_first_failure,
            "checker_results": [
                {
                    "checker": checkers_to_run[i].name,
                    "status": results[i].status.value,
                    "message": results[i].message,
                    "execution_time_ms": results[i].execution_time_ms,
                }
                for i in range(len(results))
            ],
        }

        # Determine overall pipeline result
        if counts["failed"] > 0 or counts["error"] > 0:
            failed_checkers = [
                checkers_to_run[i].name
                for i in range(len(results))
                if results[i].failed or results[i].has_error
            ]

            return CheckResult.failure(
                message=(
                    f"Pipeline failed: {counts['failed']} failures, {counts['error']} errors in "
                    f"checkers: {', '.join(failed_checkers)}"
                ),
                details=pipeline_details,
            )

        return CheckResult.success(
            message=(
                f"Pipeline passed: {counts['passed']} checks passed, {counts['skipped']} skipped"
            ),
            details=pipeline_details,
        )

    def add_checker(self, checker: PipelineChecker) -> None:
        """Add a checker to the pipeline.

        Args:
            checker: The checker to add
        """
        if checker not in self.checkers:
            self.checkers.append(checker)
            self.logger.info("Added checker to pipeline", checker_name=checker.name)
        else:
            self.logger.warning("Checker already in pipeline", checker_name=checker.name)

    def remove_checker(self, checker_name: str) -> None:
        """Remove a checker from the pipeline.

        Args:
            checker_name: Name of the checker to remove
        """
        for i, checker in enumerate(self.checkers):
            if checker.name == checker_name:
                removed_checker = self.checkers.pop(i)
                self.logger.info("Removed checker from pipeline", checker_name=removed_checker.name)
                return

        self.logger.warning("Checker not found in pipeline", checker_name=checker_name)

    def get_checker(self, checker_name: str) -> PipelineChecker | None:
        """Get a checker by name.

        Args:
            checker_name: Name of the checker to get

        Returns:
            The checker if found, None otherwise
        """
        for checker in self.checkers:
            if checker.name == checker_name:
                return checker
        return None

    def list_checkers(self) -> list[str]:
        """List all checker names in the pipeline.

        Returns:
            List of checker names
        """
        return [checker.name for checker in self.checkers]

    def set_stop_on_first_failure(self, stop: bool) -> None:
        """Set whether to stop on first failure.

        Args:
            stop: Whether to stop on first failure
        """
        self.stop_on_first_failure = stop
        self.logger.info("Set stop_on_first_failure", stop_on_first_failure=stop)

    def set_max_concurrent_checks(self, max_concurrent: int) -> None:
        """Set maximum number of concurrent checks.

        Args:
            max_concurrent: Maximum number of concurrent checks
        """
        self.max_concurrent_checks = max_concurrent
        self.logger.info("Set max_concurrent_checks", max_concurrent_checks=max_concurrent)

    def set_timeout(self, timeout_seconds: float) -> None:
        """Set timeout for pipeline execution.

        Args:
            timeout_seconds: Timeout in seconds
        """
        self.timeout_seconds = timeout_seconds
        self.logger.info("Set timeout", timeout_seconds=timeout_seconds)

    def __len__(self) -> int:
        """Return number of checkers in pipeline."""
        return len(self.checkers)

    def __str__(self) -> str:
        """String representation of the pipeline.

        Returns:
            String representation showing the number of checkers.
        """
        return f"CheckPipeline({len(self.checkers)} checkers)"

    def __repr__(self) -> str:
        """Detailed representation of the pipeline.

        Returns:
            Detailed string representation including checker names.
        """
        checker_names = [checker.name for checker in self.checkers]
        return f"CheckPipeline(checkers={checker_names})"

    # Alias for backward compatibility
    async def run_checks(
        self,
        opportunity: ArbitrageOpportunity,
        checkers: list[PipelineChecker] | None = None,
    ) -> CheckResult:
        """Alias for run_pipeline to maintain backward compatibility.

        Args:
            opportunity: The arbitrage opportunity to check
            checkers: Optional list of checkers to use (defaults to self.checkers)

        Returns:
            CheckResult indicating overall pipeline success/failure
        """
        return await self.run_pipeline(opportunity, checkers)
