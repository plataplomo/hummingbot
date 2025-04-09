#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Configuration Manager for loading and validating application configuration.
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class ConfigManager:
    """
    Manages loading and validation of configuration.
    
    This class handles loading configuration from file, validates it against
    expected schema, and provides access to configuration values through a
    dot notation interface.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the ConfigManager.
        
        Args:
            config_path: Optional path to the configuration file.
                         If not provided, default locations will be checked.
        """
        self.config: Dict[str, Any] = {}
        self.config_path = config_path or self._get_default_config_path()
        self.loaded = False
    
    def load(self) -> bool:
        """
        Load configuration from file.
        
        Returns:
            bool: True if config was loaded successfully, False otherwise
        """
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            
            # Validate configuration against schema
            validation_result = self._validate_config()
            if not validation_result:
                return False
                
            self.loaded = True
            logger.info(f"Configuration loaded successfully from {self.config_path}")
            return True
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return False
    
    def _get_default_config_path(self) -> str:
        """
        Get default configuration path.
        
        Checks environment variable and standard locations for config file.
        
        Returns:
            str: Path to the configuration file
        """
        # Check environment variable first
        env_path = os.environ.get('CYBERDELTA_CONFIG_PATH')
        if env_path:
            return env_path
        
        # Look in standard locations
        default_paths = [
            os.path.join(os.getcwd(), 'config.yaml'),
            os.path.join(os.getcwd(), 'config', 'config.yaml'),
            os.path.join(os.path.dirname(__file__), 'config.yaml'),
        ]
        
        for path in default_paths:
            if os.path.exists(path):
                return path
        
        return default_paths[0]  # Return first default as fallback
    
    def _validate_config(self) -> bool:
        """
        Validate configuration against schema.
        
        For v0.0.1, perform basic validation to ensure all required 
        sections and critical parameters are present.
        
        Returns:
            bool: True if validation passed, False otherwise
        """
        # Check for required sections
        required_sections = ['general', 'exchanges', 'strategies', 'risk']
        missing_sections = []
        
        for section in required_sections:
            if section not in self.config:
                missing_sections.append(section)
        
        if missing_sections:
            logger.error(f"Missing required configuration sections: {', '.join(missing_sections)}")
            return False
        
        # Check for minimum exchange configuration
        exchanges = self.config.get('exchanges', {})
        if not exchanges.get('hyperliquid', {}).get('enabled', False):
            logger.error("Hyperliquid exchange must be enabled")
            return False
            
        if not exchanges.get('backpack', {}).get('enabled', False):
            logger.error("Backpack exchange must be enabled")
            return False
        
        # Check for conflicting parameters
        if self._has_conflicting_parameters():
            logger.warning("Configuration has potentially conflicting parameters")
            # Don't fail validation for this, but log a warning
        
        # Check for duplicate risk parameters
        if self._has_duplicate_risk_parameters():
            logger.warning("Configuration has duplicate risk parameters in different sections")
            # Don't fail validation for this, but log a warning
        
        return True
    
    def _has_conflicting_parameters(self) -> bool:
        """
        Check if configuration has potentially conflicting parameters.
        
        Returns:
            bool: True if conflicting parameters detected, False otherwise
        """
        # Example: Check if there are multiple log_level settings
        log_levels = []
        
        if 'general' in self.config and 'log_level' in self.config['general']:
            log_levels.append(('general.log_level', self.config['general']['log_level']))
        
        if 'logging' in self.config and 'level' in self.config['logging']:
            log_levels.append(('logging.level', self.config['logging']['level']))
        
        return len(log_levels) > 1
    
    def _has_duplicate_risk_parameters(self) -> bool:
        """
        Check if configuration has duplicate risk parameters in different sections.
        
        Returns:
            bool: True if duplicate risk parameters detected, False otherwise
        """
        # Example: Check if max_position_size is defined in multiple places
        position_size_params = []
        
        if 'risk' in self.config and 'max_position_size' in self.config['risk']:
            position_size_params.append(('risk.max_position_size', self.config['risk']['max_position_size']))
        
        if 'risk' in self.config and 'global' in self.config['risk'] and 'max_position_usd' in self.config['risk']['global']:
            position_size_params.append(('risk.global.max_position_usd', self.config['risk']['global']['max_position_usd']))
        
        if 'trading' in self.config and 'max_position_size' in self.config['trading']:
            position_size_params.append(('trading.max_position_size', self.config['trading']['max_position_size']))
        
        return len(position_size_params) > 1
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a configuration value by key path.
        
        Supports dot notation for accessing nested configuration values.
        
        Args:
            key_path: Dot-separated path to config value (e.g., "general.log_level")
            default: Default value if key doesn't exist
            
        Returns:
            Configuration value or default
        """
        if not self.loaded:
            self.load()
        
        # Handle dot notation for nested keys
        keys = key_path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def reload(self) -> bool:
        """
        Reload configuration from file.
        
        Returns:
            bool: True if reload was successful, False otherwise
        """
        self.loaded = False
        return self.load() 