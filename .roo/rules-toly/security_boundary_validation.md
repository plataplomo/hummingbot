
# Rule: Security Boundary Validation

**Mandate:** All data crossing trust boundaries (network APIs, configuration files, persisted state) must be rigorously validated and sanitized. Authentication/Authorization logic must be scrutinized for flaws.

**Guidelines:**
- Validate *all* fields from API responses, not just the expected ones. Check types, ranges, lengths. Use strict parsing (e.g., Pydantic).
- Sanitize any data used in system commands or file paths (though these should be avoided).
- Analyze authentication signing processes: Is the exact, required payload signed? Are timestamps/nonces used correctly to prevent replay?
- Scrutinize secret handling: Minimize time secrets spend decrypted in memory. Clear secrets from memory after use where possible.
- Evaluate potential DoS vectors: Can malformed input or high request volume overwhelm parsing, processing, or state management?