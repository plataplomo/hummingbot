# Ruff Migration Guide - Replacing Black and isort

## Overview

We've migrated from using Black + isort to Ruff for Python code formatting and import sorting. Ruff is significantly faster (up to 1000x) and provides the same functionality with a single tool.

## What Changed

### Pre-commit Configuration
- **Removed**: Black and isort hooks
- **Added**: `ruff` (linting + import sorting) and `ruff-format` (formatting)

### Configuration
All configuration is now in `pyproject.toml` under `[tool.ruff]` sections:
- Line length: 100 (consistent across linting and formatting)
- Import sorting: Enabled with "I" rule, configured to match isort's Black profile
- Formatting: Black-compatible settings with double quotes and magic trailing comma
- Import organization: 2 blank lines after imports, split on trailing comma

## Usage

### Command Line

Instead of:
```bash
black .
isort .
```

Now use:
```bash
# Fix linting issues and sort imports
ruff check --fix .

# Format code
ruff format .

# Or both in one command
ruff check --fix . && ruff format .
```

### Pre-commit

Pre-commit will automatically run both steps:
1. `ruff` - Lints and fixes issues (including import sorting)
2. `ruff-format` - Formats code

Just run:
```bash
pre-commit run --all-files
```

### VS Code Integration

Install the official Ruff extension:
1. Search for "Ruff" by Astral Software in VS Code extensions
2. Install it
3. It will automatically use our `pyproject.toml` configuration

### Key Differences

1. **Import Sorting**: Ruff's import sorting is nearly identical to isort with `profile = "black"`
2. **Speed**: Ruff is orders of magnitude faster
3. **Single Tool**: No need to manage multiple tools and their configurations

### Troubleshooting

If you see formatting differences:
1. Ensure you're using the latest ruff version: `pip install --upgrade ruff`
2. Run `ruff format` after `ruff check --fix` (pre-commit does this automatically)
3. Check that VS Code is using the Ruff extension, not Black

### Migration Checklist

- [x] Updated `.pre-commit-config.yaml`
- [x] Configured ruff in `pyproject.toml`
- [x] Added formatter configuration for Black compatibility
- [x] Added isort configuration for import sorting
- [ ] Team installs Ruff VS Code extension
- [ ] Team updates local pre-commit: `pre-commit autoupdate`

## Benefits

1. **Performance**: ~1000x faster than Black + isort
2. **Simplicity**: One tool instead of multiple
3. **Consistency**: Single source of configuration
4. **Active Development**: Ruff is actively maintained and improving
