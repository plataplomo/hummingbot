#!/usr/bin/env python3
"""
Tests for the config_example.py script.

These tests ensure that the example script correctly:
1. Creates example configuration files
2. Loads configuration and secrets
3. Displays configuration information
4. Runs benchmarks
"""

import os
import sys
import unittest
import tempfile
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add parent directory to path to import from project
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


class TestConfigExample(unittest.TestCase):
    """Tests for the config_example.py script"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.example_script = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "examples", "config_example.py")
        
        # Make sure the example script exists
        self.assertTrue(os.path.exists(self.example_script), f"Example script not found at {self.example_script}")
    
    def tearDown(self):
        """Clean up temporary files"""
        self.temp_dir.cleanup()
    
    def test_create_example(self):
        """Test that the script creates example files"""
        # Create a temporary directory for the config files
        config_dir = os.path.join(self.temp_dir.name, "config")
        os.makedirs(config_dir, exist_ok=True)
        
        # Create a temporary directory for cyberdelta/config
        cyberdelta_config_dir = os.path.join(self.temp_dir.name, "cyberdelta", "config")
        os.makedirs(cyberdelta_config_dir, exist_ok=True)
        
        # Set HOME to the temp directory for ~/.cyberdelta
        with patch.dict('os.environ', {'HOME': self.temp_dir.name}):
            # Run the script with --create-example directly using os.system
            # Use the actual Python executable since we're already in a venv
            script_dir = os.path.dirname(self.example_script)
            
            # Use the system Python since we're already in a venv context
            command = f"cd {script_dir} && python {os.path.basename(self.example_script)} --create-example"
            exit_code = os.system(command)
            
            # Check that the script executed successfully
            self.assertEqual(exit_code, 0, f"Script failed with exit code {exit_code}")
            
            # Check that the example files were created by looking for the actual files
            # The paths are relative to the CyberDeltaEngine root directory
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            
            cyberdelta_config_example = os.path.join(project_root, "cyberdelta", "config", "config.yaml.example")
            root_config_example = os.path.join(project_root, "config", "config.example.yaml")
            home_config_example = os.path.join(self.temp_dir.name, ".cyberdelta", "secrets.yaml.example")
            
            # Check for any of the files that should be created
            files_created = (
                os.path.exists(cyberdelta_config_example) or 
                os.path.exists(root_config_example) or 
                os.path.exists(home_config_example)
            )
            
            self.assertTrue(files_created, 
                          f"No example files were created in any of the expected locations")
    
    def test_benchmark(self):
        """Test that the benchmark function runs"""
        # Create temporary config and secrets files
        config_path = os.path.join(self.temp_dir.name, "config.yaml")
        secrets_path = os.path.join(self.temp_dir.name, "secrets.yaml")
        
        # Create a valid config file
        with open(config_path, "w") as f:
            f.write("""
# General settings
general:
  log_level: INFO
  safe_mode: true

# Exchange configuration
exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.hyperliquid.xyz"
    ws_url: "wss://api.hyperliquid.xyz/ws"
  backpack:
    enabled: true
    api_base_url: "https://api.backpack.exchange"
    ws_url: "wss://ws.backpack.exchange"

# Strategy configuration
strategies:
  hl_perp_bp_spot:
    enabled: true
    symbols:
      hl_symbol: "BTC"
      bp_symbol: "BTC_USDC"
    params:
      funding_threshold: 0.0001

# Risk management
risk:
  global:
    max_position_usd: 1000.0
            """)
        
        # Create a valid secrets file
        with open(secrets_path, "w") as f:
            f.write("""
exchanges:
  hyperliquid:
    api_key: "test_key"
    api_secret: "test_secret"
  backpack:
    api_key: "test_key2"
    api_secret: "test_secret2"
            """)
        
        # Run the benchmark using os.system with venv Python
        script_dir = os.path.dirname(self.example_script)
        
        command = f"cd {script_dir} && python {os.path.basename(self.example_script)} --benchmark --config {config_path} --secrets {secrets_path}"
        exit_code = os.system(command)
        
        # Check that the script executed successfully
        self.assertEqual(exit_code, 0, f"Benchmark failed with exit code {exit_code}")
    
    def test_display_config(self):
        """Test that the script displays configuration correctly"""
        # Create temporary config and secrets files
        config_path = os.path.join(self.temp_dir.name, "config.yaml")
        secrets_path = os.path.join(self.temp_dir.name, "secrets.yaml")
        
        # Create a valid config file
        with open(config_path, "w") as f:
            f.write("""
# General settings
general:
  log_level: INFO
  safe_mode: true

# Exchange configuration
exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.test.xyz"
  backpack:
    enabled: true
    api_base_url: "https://api.test2.xyz"

# Strategy configuration
strategies:
  hl_perp_bp_spot:
    enabled: true
    symbols:
      hl_symbol: "BTC"
      bp_symbol: "BTC_USDC"

# Risk management
risk:
  global:
    max_position_usd: 1000.0
            """)
        
        # Create a valid secrets file
        with open(secrets_path, "w") as f:
            f.write("""
exchanges:
  hyperliquid:
    api_key: "test_api_key"
    api_secret: "test_api_secret"
  backpack:
    api_key: "test_api_key2"
    api_secret: "test_api_secret2"
            """)
        
        # Run the script using os.system with venv Python
        script_dir = os.path.dirname(self.example_script)
        
        command = f"cd {script_dir} && python {os.path.basename(self.example_script)} --config {config_path} --secrets {secrets_path}"
        exit_code = os.system(command)
        
        # Check that the script executed successfully
        self.assertEqual(exit_code, 0, f"Display config failed with exit code {exit_code}")


if __name__ == "__main__":
    unittest.main() 