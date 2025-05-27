#!/usr/bin/env python3
"""
Script to check API client method consistency for error handling and input validation.
Identifies methods that need simplification according to the new pattern.
"""

import ast
from pathlib import Path


def find_api_client_files() -> list[Path]:
    """Find API client files."""
    api_files = []
    base_path = Path("cyberdelta/apis")

    # Direct mapping of API files
    api_file_names = {"backpack": "bp_api.py", "hyperliquid": "hl_api.py"}

    for exchange, filename in api_file_names.items():
        api_file = base_path / exchange / filename
        if api_file.exists():
            api_files.append(api_file)

    return api_files


def extract_public_methods_from_file(file_path: Path) -> dict[str, list[str]]:
    """Extract class names and their public async methods from API files."""
    with open(file_path) as f:
        content = f.read()

    tree = ast.parse(content)
    classes_and_methods = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name.endswith("API"):
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


def analyze_method_patterns(file_path: Path, class_name: str, method_name: str) -> dict[str, any]:
    """Analyze a method for patterns that need to be simplified."""
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

    # Find the end of the method
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

    # Check for patterns that need to be removed/simplified
    patterns = {
        "found": True,
        "has_try_except_api_error": "try:" in method_text and "except APIError:" in method_text,
        "has_try_except_general": "try:" in method_text and "except Exception" in method_text,
        "has_service_call_wrapped": any(
            [
                ".account_service." in method_text,
                ".trading_service." in method_text,
                ".market_data_service." in method_text,
            ]
        )
        and "try:" in method_text,
        "has_basic_validation": any(
            [
                "if not " in method_text,
                "if " in method_text and " is None:" in method_text,
                "isinstance(" in method_text,
                "raise ValueError" in method_text,
                "raise TypeError" in method_text,
            ]
        ),
        "direct_service_return": any(
            [
                "return await self.account_service." in method_text,
                "return await self.trading_service." in method_text,
                "return await self.market_data_service." in method_text,
            ]
        ),
        "has_logger_error": "logger.error" in method_text,
        "has_logger_warning": "logger.warning" in method_text,
        "wraps_service_calls": False,  # Will be determined by more detailed analysis
    }

    # More detailed analysis for service call wrapping
    if patterns["has_service_call_wrapped"]:
        # Look for try blocks that wrap service calls
        lines = method_text.split("\n")
        in_try_block = False
        try_block_contains_service = False

        for line in lines:
            stripped = line.strip()
            if stripped.startswith("try:"):
                in_try_block = True
                try_block_contains_service = False
            elif stripped.startswith("except"):
                if in_try_block and try_block_contains_service:
                    patterns["wraps_service_calls"] = True
                in_try_block = False
            elif in_try_block and any(
                service in stripped
                for service in [".account_service.", ".trading_service.", ".market_data_service."]
            ):
                try_block_contains_service = True

    return patterns


def main():
    """Main function to check all API client methods."""
    api_files = find_api_client_files()

    print("# API Client Methods Consistency Check")
    print("=" * 60)
    print("Checking for patterns that need simplification:")
    print("1. try...except blocks wrapping service calls")
    print("2. Basic input validation (isinstance, if not, is None)")
    print("3. Error handling that should be removed")
    print()

    total_classes = 0
    total_methods = 0
    all_results = {}

    for file_path in sorted(api_files):
        print(f"\n## File: {file_path}")
        classes_and_methods = extract_public_methods_from_file(file_path)

        if not classes_and_methods:
            print("   No API client classes found")
            continue

        for class_name, methods in classes_and_methods.items():
            total_classes += 1
            print(f"\n### {class_name}")
            print(f"   Methods: {len(methods)}")

            for method in sorted(methods):
                total_methods += 1
                print(f"   - {method}")

                # Analyze method patterns
                patterns = analyze_method_patterns(file_path, class_name, method)
                all_results[f"{file_path.stem}.{class_name}.{method}"] = patterns

    print("\n\n# Summary")
    print("=" * 60)
    print(f"Total API Client Classes: {total_classes}")
    print(f"Total Public Methods: {total_methods}")

    # Analyze patterns that need attention
    print("\n# Methods That Need Simplification")
    print("=" * 60)

    issues_found = False

    # Methods with error handling around service calls
    service_wrapped_methods = []
    basic_validation_methods = []
    direct_return_methods = []

    for method_key, patterns in all_results.items():
        if not patterns.get("found", False):
            continue

        if patterns.get("wraps_service_calls", False):
            service_wrapped_methods.append(method_key)

        if patterns.get("has_basic_validation", False):
            basic_validation_methods.append(method_key)

        if patterns.get("direct_service_return", False):
            direct_return_methods.append(method_key)

    if service_wrapped_methods:
        issues_found = True
        print(
            f"\n❌ Methods with try/except wrapping service calls ({len(service_wrapped_methods)}):"
        )
        print("   (These should be simplified to direct service calls)")
        for method in service_wrapped_methods:
            print(f"   - {method}")

    if basic_validation_methods:
        issues_found = True
        print(f"\n⚠️  Methods with basic input validation ({len(basic_validation_methods)}):")
        print("   (Review if validation should be moved to service layer)")
        for method in basic_validation_methods:
            print(f"   - {method}")

    if direct_return_methods:
        print(f"\n✅ Methods already using direct service returns ({len(direct_return_methods)}):")
        for method in direct_return_methods:
            print(f"   - {method}")

    if not issues_found:
        print("\n✅ All API client methods appear to follow the simplified pattern!")
    else:
        print(
            f"\n📊 Summary: {len(service_wrapped_methods)} methods need error handling simplification"
        )
        print(f"           {len(basic_validation_methods)} methods may need validation review")

    print("\n# Pattern Details")
    print("=" * 60)

    pattern_counts = {
        "try_except_api_error": 0,
        "try_except_general": 0,
        "service_call_wrapped": 0,
        "basic_validation": 0,
        "direct_service_return": 0,
    }

    for patterns in all_results.values():
        if not patterns.get("found", False):
            continue
        if patterns.get("has_try_except_api_error", False):
            pattern_counts["try_except_api_error"] += 1
        if patterns.get("has_try_except_general", False):
            pattern_counts["try_except_general"] += 1
        if patterns.get("has_service_call_wrapped", False):
            pattern_counts["service_call_wrapped"] += 1
        if patterns.get("has_basic_validation", False):
            pattern_counts["basic_validation"] += 1
        if patterns.get("direct_service_return", False):
            pattern_counts["direct_service_return"] += 1

    for pattern, count in pattern_counts.items():
        print(f"{pattern}: {count} methods")


if __name__ == "__main__":
    main()
