# Security Audit Report: Part 3 - Secrets Management Lifecycle

**Rule Reference:** `Secrets_Management_Lifecycle.mdc` (Implied rule - based on user prompt)

**Assessment Summary:** Significant Weaknesses

**Detailed Findings:**

The management of sensitive secrets like API keys and the Ethereum private key presents significant security risks throughout the application lifecycle.

1.  **Loading:**
    *   `SecretsManager` (`cyberdelta/config/secrets_manager.py`) correctly loads secrets from an external YAML file (`secrets.yaml`), identified via environment variable or default paths (e.g., `~/.cyberdelta/secrets.yaml`).
    *   It uses `yaml.safe_load`, preventing YAML-based code execution attacks.

2.  **Storage in Memory (CRITICAL FLAW):**
    *   Upon loading, `SecretsManager` stores the entire content of the secrets file into the `self.secrets` dictionary (line 48).
    *   API client initializers (`BackpackAPI.__init__`, `HyperliquidAPI.__init__`) retrieve necessary keys (API key/secret, private key, wallet address) from this dictionary and store them directly as **plain text strings** in instance variables (e.g., `self._api_key`, `self._api_secret`, `self._private_key`).
    *   For Hyperliquid, the plain text `self._private_key` is used to instantiate a `web3` account object (`self._account`), which likely also keeps the key material in memory.
    *   These secrets remain **decrypted in memory** for the entire lifetime of the `SecretsManager` and the API client objects.

3.  **Access and Transmission:**
    *   Secrets are accessed via the `SecretsManager.get()` method and passed directly to the API client constructors. There's no indication of unnecessary logging or propagation beyond the API clients.

4.  **File Permissions (High Severity Weakness):**
    *   `SecretsManager._get_secrets_path` finds the secrets file but performs **no checks on its file system permissions**.
    *   The application will load secrets from a world-readable `secrets.yaml` file without warning, relying solely on correct OS-level configuration.

5.  **Lifecycle/Clearing:**
    *   There is **no mechanism to clear secrets** from memory (e.g., overwrite variables) after they have been used for initialization or signing. They persist until the objects are garbage collected.

**Code Snippets:**

*   **Loading into Dictionary (`cyberdelta/config/secrets_manager.py`):**
    ```python
    # Secrets loaded and stored directly in a dictionary
    with open(secrets_path) as f:
        self.secrets = yaml.safe_load(f)
    self.secrets_loaded = True
    ```

*   **Storing in API Client Instance Variables (`cyberdelta/apis/backpack.py`, `cyberdelta/apis/hyperliquid.py`):**
    ```python
    # BackpackAPI.__init__
    self._api_key = secrets.get("BACKPACK_API_KEY")
    self._api_secret = secrets.get("BACKPACK_API_SECRET")

    # HyperliquidAPI.__init__
    self._wallet_address = secrets.get("wallet_address")
    self._private_key = secrets.get("private_key") # Stored as plain text string
    # ... later used ...
    self._account = w3.eth.account.from_key(self._private_key) # Key likely held by web3 object too
    ```

**Mermaid Snippets:**

*   **Secret Handling Flow:**
    ```mermaid
    graph TD
        A[secrets.yaml File] -- Read --> B(SecretsManager);
        B -- Plain Text Dict --> C{API Client Init};
        C -- Plain Text Instance Vars --> D(API Client Object);
        D -- Use for Signing --> E(Authentication Logic);

        subgraph "Memory Exposure Risk"
            direction LR
            B -- secrets dict --> R1[Risk Point 1];
            D -- _api_key, _private_key, _account --> R2[Risk Point 2];
        end

        F[OS File System] -- Permissions? --> A;
    ```

**Recommendations:**

1.  **Minimize Plain Text Storage (Critical):** Avoid storing plain text secrets directly in long-lived instance variables.
    *   **Option A (Ideal for Private Keys):** Use a dedicated secrets management service (e.g., HashiCorp Vault, AWS Secrets Manager) and fetch secrets only when needed for signing, clearing them immediately after. This requires infrastructure changes.
    *   **Option B (Improvement):** Load secrets in `SecretsManager`. When API clients initialize, pass the secrets *directly* to the signing functions (`_sign_request`, `_authenticate`) *each time they are called*, instead of storing them in instance variables. Retrieve them from `SecretsManager` within the signing function scope. This reduces the duration secrets are held decrypted but doesn't eliminate storage in `SecretsManager`.
    *   **Option C (Partial for Private Key):** Keep the `web3` `self._account` object in `HyperliquidAPI` (as it needs the key internally) but **avoid storing the raw `self._private_key` string** after initializing `self._account`. Set `self._private_key = None` immediately after use in `__init__`. This still leaves the key within the `_account` object's memory.

2.  **Implement Secrets File Permission Checks (High):** In `SecretsManager.load_secrets`, after finding the `secrets_path`, check its permissions (e.g., using `os.stat` and checking `st_mode`). Log a critical error and refuse to load the file if permissions are too broad (e.g., readable by group or others). Define what constitutes secure permissions (e.g., owner read-only `0o400` or read-write `0o600`).

3.  **Consider Memory Protection (Advanced):** For extremely sensitive keys like the private key, investigate libraries or techniques for secure memory handling (e.g., `memguard` library, although OS support varies) to reduce the risk of keys being swapped to disk or easily dumped, if Option A/B are not feasible.

4.  **Audit Logging:** Ensure no secrets are ever logged, even at DEBUG level. Review all logging statements that handle configuration or API interaction data.

**Severity Assessment:**

*   Plain Text Storage in Memory: **Critical** (especially for the private key)
*   Lack of File Permission Checks: **High**
*   Persistence in Memory (Lifecycle): **Medium**