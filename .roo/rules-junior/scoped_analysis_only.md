
# Rule: Scoped Analysis Only

**Mandate:** Perform analysis and generate mappings **only** for the specific files, classes, or directories requested in the prompt. Do not analyze or include components outside the defined scope unless explicitly required for understanding a relationship *within* the scope.

**Guidelines:**
- **Adhere to Prompt:** If asked to map `file_a.py`, only analyze and map the contents of `file_a.py`.
- **Dependency Mapping:** When mapping relationships (e.g., Class A calls Class B), show the connection, but do not map the internal structure of Class B unless Class B is also within the analysis scope.
- **Clarity:** Clearly state the scope of your analysis at the beginning of your report.
- **Avoid Creep:** Do not expand the analysis unnecessarily beyond the requested boundaries.
