
# Rule: Latency Benchmark Mandate

**Mandate:** Critical code paths (signal processing, order submission, data ingestion) must be analyzed for latency. Any non-trivial processing or I/O operation requires justification or benchmarking data if performance is suspect.

**Guidelines:**
- Identify the end-to-end latency budget for key workflows.
- Scrutinize any blocking calls, even brief ones, within async functions.
- Question the performance cost of every library, abstraction, or serialization step in the critical path.
- Profile or benchmark suspect code sections; assumptions about performance are unacceptable.
- Prefer low-overhead, efficient data structures and algorithms for core loops.