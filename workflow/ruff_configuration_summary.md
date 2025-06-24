# Ruff Configuration Summary

## Consistent Settings Across All Tools

Our Ruff configuration ensures consistency between linting, formatting, and import sorting:

### Core Settings (shared)
- **Line length**: 100 characters
- **Target Python version**: py313
- **Quote style**: Double quotes
- **Indentation**: 4 spaces
- **Line endings**: Auto (Unix/Windows compatible)

### Linting Rules
```toml
[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors
    "F",   # pyflakes
    "I",   # isort (import sorting)
    "B",   # bugbear
    "UP",  # upgrade
    "ANN", # annotations
    "T",   # typing
    "D",   # docstrings
    "Q",   # quality
    "COM", # comments
    "S",   # security
    "C",   # complexity
]

ignore = [
    "D203",   # Blank line conflicts with D211
    "D213",   # Docstring summary conflicts with D212
    "COM812", # Trailing commas managed by formatter
]
```

### Import Sorting (isort replacement)
```toml
[tool.ruff.lint.isort]
known-first-party = ["cyberdelta"]
known-third-party = ["pytest", "pydantic", "numpy", "pandas"]
force-single-line = false
combine-as-imports = true
split-on-trailing-comma = true
lines-after-imports = 2
case-sensitive = false
```

### Formatting (Black replacement)
```toml
[tool.ruff.format]
quote-style = "double"
indent-style = "space"
skip-magic-trailing-comma = false
line-ending = "auto"
docstring-code-format = true
docstring-code-line-length = 72
```

## Key Consistency Points

1. **Line Length**: All tools respect the 100-character limit
2. **Quotes**: Double quotes everywhere for consistency
3. **Trailing Commas**: Managed by formatter, not linter rules
4. **Import Organization**: 2 blank lines after imports, proper grouping
5. **Docstring Formatting**: Code in docstrings gets formatted with shorter lines

## Usage Order

The pre-commit hooks run in the correct order:
1. `ruff` (linting + import sorting + fixes)
2. `ruff-format` (code formatting)

This ensures imports are sorted before formatting, preventing conflicts.
