
**Sub-Prompt 1.1.B (Corrective): Modify Pydantic Secrets Models for Optional Testnet Private Key and Seed Passphrase**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** HL_TESTNET_P1B_SECRETS_MODELS_CORRECTIVE
**Task:** Update `PrivateKeyAuthSecrets` for Optional Testnet Private Key and Seed Passphrase

**1. Goal:**
Modify `cyberdelta/config/secrets_models.py` to enhance `PrivateKeyAuthSecrets` by adding:
1.  An optional `private_key_testnet: SecretStr | None`.
2.  An optional `testnet_seed_passphrase: SecretStr | None`.

**2. Why This Is Important:**
This provides flexibility for testnet authentication:
- `private_key_testnet`: Allows users to specify a distinct private key for testnet.
- `testnet_seed_passphrase`: Allows users to specify a seed passphrase for deriving multiple testnet wallets.
The system can prioritize one over the other or use the main `private_key` as a fallback for testnet if neither testnet-specific option is provided.

**3. File to Modify:**
*   `cyberdelta/config/secrets_models.py`

**4. Detailed Steps & Implementation Guidance:**

   *   **Locate `PrivateKeyAuthSecrets` Model.**
   *   **Add `private_key_testnet` Field:**
        *   `private_key_testnet: SecretStr | None = Field(default=None, description="Optional dedicated private key for testnet environment. If not provided, the main 'private_key' or 'testnet_seed_passphrase' might be used for testnet operations.")`
        *   Make it optional (`| None = Field(default=None)`).
   *   **Add/Verify `testnet_seed_passphrase` Field (as previously discussed):**
        *   `testnet_seed_passphrase: SecretStr | None = Field(default=None, description="Optional BIP-39 seed passphrase specifically for generating/using testnet wallets. Can be used if a dedicated 'private_key_testnet' is not provided.")`
   *   **Field Order:** Consider placing `private_key_testnet` before `testnet_seed_passphrase` if a direct key is generally preferred over a seed when both might be available conceptually (though a user would likely provide only one). For model definition, the order primarily affects documentation generation.
   *   **Existing Fields:** The existing `private_key` (for mainnet/default) and `passphrase` (for encrypting the main `private_key`) fields remain unchanged.

**5. Testing Requirements:**
*   Ensure `PrivateKeyAuthSecrets` can be instantiated correctly in various scenarios:
    *   With only `private_key`.
    *   With `private_key` and `private_key_testnet`.
    *   With `private_key` and `testnet_seed_passphrase`.
    *   With `private_key`, `private_key_testnet`, and `testnet_seed_passphrase`.
*   Verify that `private_key_testnet` and `testnet_seed_passphrase` default to `None`.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Model Architecture V1 (Pydantic secrets models).
```

---
And, we'll need to update the example secrets file accordingly.

---
**Sub-Prompt 1.1.C (Secrets Example Update - Corrective): Update Example `secrets.yaml` for Testnet Private Key**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** HL_TESTNET_P1C_EXAMPLE_SECRETS_CORRECTIVE
**Task:** Update Example `secrets.yaml` for Optional Testnet Private Key

**1. Goal:**
Update `cyberdelta/config/secrets.yaml.example` to include the new optional `private_key_testnet` field for Hyperliquid.

**2. Why This Is Important:**
The example secrets file must reflect all available configuration options for users.

**3. File to Modify:**
*   `cyberdelta/config/secrets.yaml.example`

**4. Detailed Steps & Implementation Guidance:**

   *   **Locate the `hyperliquid` section under `exchanges` in `secrets.yaml.example`.**
   *   **Add `private_key_testnet` Field:**
      *   Add a commented-out entry for `private_key_testnet`:
        ```yaml
        # Optional: Dedicated private key for Hyperliquid testnet
        # private_key_testnet: "YOUR_HYPERLIQUID_TESTNET_WALLET_PRIVATE_KEY_0x..." 
        ```
      *   Ensure this is placed appropriately alongside the existing `private_key`, `passphrase`, and the `testnet_seed_passphrase` fields.
      *   Update comments to clarify that if `is_mainnet_environment` is false in `config.yaml`, the system might look for `testnet_seed_passphrase` first, then `private_key_testnet`, then fall back to the main `private_key`. (This precedence logic will be built into the `HyperliquidAPIComponentsFactory`).

**5. Testing Requirements:**
*   Manual review by Human Lead to ensure the example file is clear and correctly updated.

**6. Project Rules Adherence:**
*   Documentation clarity.
```

---

Finally, the logic in `HyperliquidAPIComponentsFactory.create_authenticator` (covered by **Sub-Prompt 1.1.D (Revised)**) will need to be aware of this new `private_key_testnet` and implement the precedence for selecting the key/seed to use when `is_mainnet_environment` is false:

1.  If `is_mainnet_environment` is `False`:
    1.  Prefer `secrets.testnet_seed_passphrase` if provided (for deriving accounts).
    2.  Else, prefer `secrets.private_key_testnet` if provided.
    3.  Else, fall back to using the main `secrets.private_key`.
2.  If `is_mainnet_environment` is `True`:
    1.  Use `secrets.private_key`.

This makes the authenticator creation truly environment-aware based on the secrets provided. I'll ensure Sub-Prompt 1.1.D (Revised) reflects this updated logic for the factory.

Thank you for ensuring we cover all bases! This makes the testnet configuration much more versatile.