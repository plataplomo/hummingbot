#!/usr/bin/env python3
"""
Script to merge the /cyberdelta/tests directory into /tests
This consolidates all tests into a single directory structure.

This script uses the CyberDeltaEngine configuration system for proper logging setup.
"""

import glob
import logging
import os
import shutil
import sys
from pathlib import Path

from cyberdelta.config import get_app_settings
from cyberdelta.config.logging_config import setup_logging

logger = logging.getLogger(__name__)


def ensure_directory_exists(directory: str | Path) -> None:
    """
    Ensure the specified directory exists, create it if it doesn't.

    Args:
        directory: Directory path to create if it doesn't exist
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Created directory: {directory}")


def merge_directories(source_dir: str | Path, target_dir: str | Path) -> None:
    """
    Merge all files from source_dir into target_dir.

    Args:
        source_dir: Source directory to merge from
        target_dir: Target directory to merge into
    """
    # Ensure target directory exists
    ensure_directory_exists(target_dir)

    # Get list of files in source directory
    source_files: list[str] = glob.glob(os.path.join(source_dir, "*"))

    for source_file in source_files:
        file_name = os.path.basename(source_file)
        target_file = os.path.join(target_dir, file_name)

        if os.path.isdir(source_file):
            # Recursively merge subdirectories
            merge_directories(source_file, os.path.join(target_dir, file_name))
        else:
            # Check if the target file already exists
            if os.path.exists(target_file):
                logger.warning(f"File {target_file} already exists. Skipping.")
            else:
                # Copy the file
                shutil.copy2(source_file, target_file)
                logger.info(f"Copied: {source_file} -> {target_file}")

                # Update imports if it's a Python file
                if target_file.endswith(".py"):
                    update_imports(target_file)

    logger.info(f"Successfully merged {len(source_files)} files from {source_dir} to {target_dir}")


def update_imports(file_path: str | Path) -> None:
    """
    Update imports in the file to reflect the new directory structure.

    Args:
        file_path: Path to the Python file to update
    """
    try:
        with open(file_path, encoding="utf-8") as file:
            content = file.read()

        # Update common import patterns
        replacements = [
            ("from cyberdelta.tests", "from tests"),
            ("import cyberdelta.tests", "import tests"),
        ]

        updated_content = content
        for old, new in replacements:
            updated_content = updated_content.replace(old, new)

        if updated_content != content:
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(updated_content)
            logger.info(f"Updated imports in: {file_path}")

    except Exception as e:
        logger.error(f"Error updating imports in {file_path}: {e}")


def main() -> None:
    """
    Main function to merge test directories.
    """
    # Initialize configuration and logging
    try:
        app_settings = get_app_settings()
        setup_logging(app_settings)
        logger.info("Configuration and logging initialized successfully")
    except Exception as e:
        # Fallback to basic logging if config fails
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        logger.warning(f"Failed to initialize configuration: {e}. Using basic logging.")

    # Determine the project root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    # Source and target directories
    source_dir = os.path.join(project_root, "cyberdelta", "tests")
    target_dir = os.path.join(project_root, "tests")

    logger.info(f"Merging tests from {source_dir} to {target_dir}...")

    # Check if source directory exists
    if not os.path.exists(source_dir):
        logger.error(f"Source directory {source_dir} does not exist.")
        sys.exit(1)

    # Merge directories
    try:
        merge_directories(source_dir, target_dir)
        logger.info("Test directory merge completed successfully.")
        logger.info("Please review the merged files to ensure everything is correct.")
        logger.info("After verification, you can remove the original /cyberdelta/tests directory.")
    except Exception as e:
        logger.error(f"Error during directory merge: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
