---
description: 
globs: ["*.py,*.pyi"]
alwaysApply: true
---
# Rule: Assume Hostile Input - Rigorous Validation

**Mandate:** Treat ALL external input as potentially malicious and untrusted. This includes data from exchange APIs (REST & WebSocket), configuration files, persisted state files, and environment variables. Implement strict validation at the point of entry.

**Guidelines:**
- **API Responses:** Validate the *entire structure* and *all expected fields* of API responses using strict parsing (e.g., Pydantic models). Check types, ranges, lengths, and allowed values. Do not trust optional fields to be present or of the correct type unless explicitly handled. Log or reject unexpected/malformed data immediately.
- **Configuration/State:** Validate configuration files (`config.yaml`) and loaded state (`state.json`) against a defined schema (e.g., using Pydantic) on startup. Reject invalid configurations or state.
- **User Input (If Any):** If any CLI arguments or user inputs exist, sanitize and validate them rigorously.
- **Denial of Service:** Consider how malformed or excessively large inputs could impact parsing performance or memory usage. Implement reasonable size limits.
- **Principle:** Never pass raw, unvalidated external data directly into core application logic. Validate first.

# Rule: Secure Authentication Implementation & Verification

**Mandate:** Implement exchange authentication protocols (HMAC, EIP-712/Signing) precisely according to specification and security best practices. Verify the implementation's security posture.

**Guidelines:**
- **Correct Signing:** Ensure the exact, required payload (correct parameters, order, encoding) is signed. Do not include extraneous data or omit required fields.
- **Timestamp/Nonce/Window:** Use timestamps/nonces correctly to prevent replay attacks as specified by the exchange. Use the narrowest acceptable validity window (`X-Window` for Backpack). Ensure clock synchronization if relying on timestamps.
- **Key Usage:** Use cryptographic libraries correctly and securely. Avoid deprecated algorithms or weak parameters.
- **Error Handling:** Treat authentication failures returned by the exchange as critical security events. Log them clearly but avoid logging sensitive details. Implement potential lockouts or alerts after repeated failures.
- **Review Exchange Docs Critically:** Understand the potential weaknesses or edge cases of the exchange's specific authentication mechanism.

# Rule: Secure Secrets Management Lifecycle

**Mandate:** Sensitive credentials (API keys, private keys, database passwords) must **NEVER** be hardcoded or committed to version control. Manage them securely throughout their lifecycle (loading, in-memory handling, storage).

**Guidelines:**
- **Loading:** Use the approved `SecretsManager` approach. Load secrets from secure, external locations (environment variables, designated non-repository files like `~/.cyberdelta/secrets.yaml`, or dedicated secrets management systems in production).
- **In-Memory Handling:** Minimize the time secrets spend decrypted in memory. Load them as late as possible. Do not log secrets. Avoid passing secrets unnecessarily between components. Clear secret variables from memory after use if practical (though Python GC makes guarantees difficult).
- **Storage (External):** Secrets files must have strict file permissions (readable only by the application user). Use encrypted storage where possible (e.g., encrypted volumes, OS keychain).
- **`.gitignore`:** Ensure `.env`, `secrets.yaml`, and any other potential secret-containing files are explicitly listed in `.gitignore`.
- **No Hardcoding:** Absolutely no API keys, private keys, or passwords directly in `.py` or `.yaml` files within the repository.


# Rule: Secure Coding Practices (Python Specific)

**Mandate:** Avoid common Python security pitfalls and follow secure coding guidelines.

**Guidelines:**
- **Avoid `eval()` and `exec()`:** Never use `eval()` or `exec()` with untrusted input.
- **Input Sanitization:** While covered by Rule 1, reiterate the need to sanitize any data *before* it's used in potentially dangerous operations (e.g., constructing file paths, database queries - though these should be limited).
- **Avoid Pickle:** Do not use `pickle` for deserializing data from untrusted sources due to arbitrary code execution risks. Use safer formats like JSON if applicable for state persistence, validating rigorously upon load.
- **Dependency Security:** Keep dependencies updated. Use tools like `pip-audit` or GitHub Dependabot to scan for known vulnerabilities in third-party libraries. Address critical vulnerabilities promptly. (`09_Dependencies.md`)
- **Error Message Verbosity:** Avoid leaking sensitive system information (file paths, internal configurations, stack traces) in error messages exposed externally (e.g., API responses, potentially logs sent to external systems). Log detailed errors internally, provide generic errors externally.


# Rule: Transport Layer Security (TLS/SSL)

**Mandate:** Ensure all network communication with external APIs (REST and WebSocket) uses secure, encrypted channels (HTTPS, WSS) with proper certificate validation.

**Guidelines:**
- **Use HTTPS/WSS:** Verify that all API base URLs in the configuration start with `https://` or `wss://`.
- **Certificate Validation:** Ensure the HTTP client (`aiohttp`) performs standard TLS certificate validation by default. Do not disable certificate verification unless absolutely necessary for specific, justified local testing scenarios (and never in production).
- **No Downgrades:** Prevent accidental downgrades to insecure protocols (HTTP/WS).
- **Library Updates:** Keep underlying TLS/SSL libraries (provided by Python and the OS) updated to patch known vulnerabilities.


