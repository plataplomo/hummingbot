
# Rule: Concurrency Correctness Proof

**Mandate:** Any use of shared mutable state or complex synchronization primitives requires explicit justification and analysis of potential race conditions, deadlocks, or starvation.

**Guidelines:**
- Minimize shared mutable state wherever possible.
- Justify the choice and granularity of any locking mechanism (`asyncio.Lock`, `Semaphore`). Analyze potential contention.
- Analyze task interaction patterns: Identify potential race conditions in accessing shared resources.
- Ensure proper handling of task cancellation and cleanup in concurrent operations.
- Evaluate backpressure mechanisms: How does the system handle being overwhelmed with data or tasks?
- Prefer simpler, provably correct concurrency patterns over complex, potentially flawed ones.
