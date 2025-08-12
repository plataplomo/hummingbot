"""Configuration override utilities.

Provides common utilities for applying configuration overrides
across different configuration models.
"""

from __future__ import annotations


def apply_config_overrides(
    target: dict[str, object], 
    overrides_dict: dict[str, object]
) -> None:
    """Apply configuration overrides recursively.
    
    Recursively applies override values to a target configuration dictionary.
    For nested dictionaries, the function merges recursively. For other types,
    the override value replaces the target value.
    
    Args:
        target: Target configuration dictionary to modify in-place
        overrides_dict: Override values to apply
    """
    for key, value in overrides_dict.items():
        if isinstance(value, dict) and key in target and isinstance(target[key], dict):
            apply_config_overrides(target[key], value)  # type: ignore[arg-type]
        else:
            target[key] = value