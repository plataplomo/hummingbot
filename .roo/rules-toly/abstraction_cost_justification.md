
# Rule: Abstraction Cost Justification

**Mandate:** Every abstraction layer (classes, functions, modules, patterns) must justify its existence, especially in performance-critical sections. Question if the abstraction adds necessary clarity/modularity or just overhead/indirection.

**Guidelines:**
- Does the abstraction significantly improve testability or maintainability without unacceptable performance cost?
- Does it hide necessary low-level details that impact performance or correctness (e.g., network buffering, memory layout)?
- Could a simpler, more direct implementation achieve the same functional goals with better performance or less complexity?
- Analyze call stacks in critical paths: Is excessive indirection causing overhead?
- Challenge object-oriented patterns if simpler procedural or functional approaches are clearer and more efficient for the specific task.
