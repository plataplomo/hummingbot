#!/usr/bin/env python3
"""
Script to check error handling consistency across all service methods.
"""

import ast
from pathlib import Path


def find_service_files() -> list[Path]:
    """Find all service files."""
    service_files = []
    base_path = Path("cyberdelta/apis")

    for exchange in ["backpack", "hyperliquid"]:
        services_dir = base_path / exchange / "services"
        if services_dir.exists():
            for file in services_dir.glob("*.py"):
                if file.name != "__init__.py":
                    service_files.append(file)

    return service_files


def extract_methods_from_file(file_path: Path) -> dict[str, list[str]]:
    """Extract class names and their public async methods from a file."""
    with open(file_path) as f:
        content = f.read()

    tree = ast.parse(content)
    classes_and_methods = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name.endswith("Service"):
            class_name = node.name
            methods = []

            for item in node.body:
                if (
                    isinstance(item, ast.AsyncFunctionDef)
                    and not item.name.startswith("_")
                    and item.name != "__init__"
                ):
                    methods.append(item.name)

            if methods:
                classes_and_methods[class_name] = methods

    return classes_and_methods


def check_error_handling_patterns(
    file_path: Path, class_name: str, method_name: str
) -> dict[str, bool]:
    """Check if a method has the required error handling patterns."""
    with open(file_path) as f:
        content = f.read()

    # Find the method in the file
    tree = ast.parse(content)
    method_node = None

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, ast.AsyncFunctionDef) and item.name == method_name:
                    method_node = item
                    break

    if not method_node:
        return {"found": False}

    # Convert method to string for pattern checking
    method_start = method_node.lineno
    method_lines = content.split("\n")[method_start - 1 :]

    # Find the end of the method (next method or class end)
    method_content = []
    indent_level = None
    for line in method_lines:
        if line.strip() and indent_level is None:
            indent_level = len(line) - len(line.lstrip())

        if (
            line.strip()
            and len(line) - len(line.lstrip()) <= indent_level
            and line.strip().startswith(("async def ", "def ", "class "))
        ):
            if method_content:  # If we already have content, this is the next method
                break

        method_content.append(line)

    method_text = "\n".join(method_content)

    patterns = {
        "input_validation": "# Service Input Parameter Validation" in method_text,
        "inspect_frame": "inspect.currentframe()" in method_text,
        "context_init": "status_code: int = 0" in method_text
        and "raw_response_content" in method_text,
        "try_except": "try:" in method_text,
        "except_api_error": "except APIError" in method_text,
        "except_transformation": "except TransformationError" in method_text,
        "except_validation": "except ValidationError" in method_text,
        "except_value_type": "except (ValueError, TypeError)" in method_text,
        "except_general": "except Exception" in method_text,
        "exc_info_true": "exc_info=True" in method_text,
    }

    patterns["found"] = True
    return patterns


def main():
    """Main function to check all service methods."""
    service_files = find_service_files()

    print("# Complete Service Classes and Methods Inventory")
    print("=" * 60)

    total_classes = 0
    total_methods = 0
    all_results = {}

    for file_path in sorted(service_files):
        print(f"\n## File: {file_path}")
        classes_and_methods = extract_methods_from_file(file_path)

        if not classes_and_methods:
            print("   No service classes found")
            continue

        for class_name, methods in classes_and_methods.items():
            total_classes += 1
            print(f"\n### {class_name}")
            print(f"   Methods: {len(methods)}")

            for method in sorted(methods):
                total_methods += 1
                print(f"   - {method}")

                # Check error handling patterns
                patterns = check_error_handling_patterns(file_path, class_name, method)
                all_results[f"{class_name}.{method}"] = patterns

    print("\n\n# Summary")
    print("=" * 60)
    print(f"Total Service Classes: {total_classes}")
    print(f"Total Public Methods: {total_methods}")

    # Check error handling consistency
    print("\n# Error Handling Pattern Analysis")
    print("=" * 60)

    pattern_names = [
        "input_validation",
        "inspect_frame",
        "context_init",
        "try_except",
        "except_api_error",
        "except_transformation",
        "except_validation",
        "except_value_type",
        "except_general",
        "exc_info_true",
    ]

    missing_patterns = {}
    for pattern in pattern_names:
        missing_patterns[pattern] = []

    for method_key, patterns in all_results.items():
        if not patterns.get("found", False):
            print(f"❌ {method_key}: Method not found in AST")
            continue

        for pattern in pattern_names:
            if not patterns.get(pattern, False):
                missing_patterns[pattern].append(method_key)

    all_good = True
    for pattern, missing_methods in missing_patterns.items():
        if missing_methods:
            all_good = False
            print(f"\n❌ Missing {pattern}:")
            for method in missing_methods:
                print(f"   - {method}")

    if all_good:
        print("\n✅ All methods have consistent error handling patterns!")
    else:
        print("\n⚠️  Found inconsistencies in error handling patterns")

    # Detailed breakdown by exchange
    print("\n# Breakdown by Exchange")
    print("=" * 60)

    backpack_methods = [k for k in all_results.keys() if "Backpack" in k]
    hyperliquid_methods = [k for k in all_results.keys() if "Hyperliquid" in k]

    print(f"Backpack methods: {len(backpack_methods)}")
    print(f"Hyperliquid methods: {len(hyperliquid_methods)}")


if __name__ == "__main__":
    main()
