
# Rule: System Resource Efficiency

**Mandate:** Code must demonstrate efficient use of system resources (CPU, memory, network sockets, file descriptors). Analyze potential for leaks or excessive consumption.

**Guidelines:**
- Analyze object creation/destruction patterns within hot loops for potential GC pressure.
- Evaluate memory footprint of cached data and in-memory state (`PortfolioTracker`, `DataHandler`). Justify sizes.
- Scrutinize network I/O: Are connections pooled/reused effectively? Is data buffered efficiently? Are WebSockets handling ping/pong correctly to avoid zombie connections?
- Check for potential file descriptor leaks (ensure files/sockets are reliably closed).
- Consider CPU usage: Are there CPU-bound tasks blocking the event loop? Should they be run in executors?
