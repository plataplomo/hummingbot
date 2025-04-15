
# Rule: Strict Skeleton Extraction

**Mandate:** When generating code skeletons, extract **only** the class/function/method signature (including name, parameters with type hints, and return type hint) and the docstring. Replace the *entire* implementation body with `pass` or `...`.

**Guidelines:**
- **Signature Accuracy:** Copy parameter names, type hints (`:`), and return type hints (`->`) exactly as they appear in the source code.
- **Body Replacement:** Delete everything indented under the `def` or `class` line (except the docstring) and replace it with a single `pass` statement or `...` on the next indented line.
- **Include Docstrings:** Preserve the original docstring immediately following the signature line.
- **No Logic:** Do not include *any* lines from the original function/method body implementation.
- **Focus:** This rule ensures the output focuses purely on the interface/structure, not the internal logic.