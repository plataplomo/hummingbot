
# Rule: No Implementation Modification

**Mandate:** Your task is mapping, not refactoring or debugging internal logic. **DO NOT** modify the implementation body of any function or method beyond replacing it entirely with `pass` or `...` for skeleton generation.

**Guidelines:**
- **Read-Only Logic:** Treat the *content* inside functions/methods as read-only information used solely to understand relationships for diagrams.
- **No Bug Fixes:** Do not attempt to fix errors or improve logic found within implementation bodies. Report significant structural issues if necessary, but do not change the code.
- **No Refactoring:** Do not rename variables, extract methods, or change control flow within the original implementation when analyzing it.
- **Focus:** Your output is the *structure*, not improved *logic*.