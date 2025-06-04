#!/usr/bin/env python3
"""Script to automatically add docstrings to test functions missing them."""

import ast
import os
import re
from pathlib import Path
from typing import List, Optional, Tuple


class DocstringAdder(ast.NodeTransformer):
    """AST transformer that adds docstrings to functions without them."""
    
    def __init__(self, source_lines: List[str]):
        self.source_lines = source_lines
        self.modifications: List[Tuple[int, str]] = []
    
    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        """Visit function definitions and add docstrings if missing."""
        # Check if function already has a docstring
        has_docstring = (
            node.body and
            isinstance(node.body[0], ast.Expr) and
            isinstance(node.body[0].value, ast.Constant) and
            isinstance(node.body[0].value.value, str)
        )
        
        if not has_docstring and not node.name.startswith('_'):
            # Generate appropriate docstring based on function name
            docstring = self._generate_docstring(node.name)
            
            # Get the indentation of the function body
            if node.body:
                first_stmt_line = node.body[0].lineno - 1
                indent = self._get_indentation(first_stmt_line)
            else:
                # Empty function body, use function def indentation + 4 spaces
                func_line = node.lineno - 1
                func_indent = self._get_indentation(func_line)
                indent = func_indent + '    '
            
            # Record the modification
            insert_line = node.lineno
            self.modifications.append((insert_line, f'{indent}"""{docstring}"""'))
        
        self.generic_visit(node)
        return node
    
    def _get_indentation(self, line_idx: int) -> str:
        """Get the indentation of a specific line."""
        if 0 <= line_idx < len(self.source_lines):
            line = self.source_lines[line_idx]
            return re.match(r'^(\s*)', line).group(1)
        return '    '
    
    def _generate_docstring(self, func_name: str) -> str:
        """Generate an appropriate docstring based on function name."""
        # Convert function name to readable format
        readable_name = func_name.replace('_', ' ')
        
        # Patterns for different function types
        if func_name.startswith('test_'):
            # Test function
            description = readable_name[5:]  # Remove 'test '
            return f"Test {description}."
        
        elif func_name.startswith('valid_'):
            # Validation helper
            description = readable_name[6:]  # Remove 'valid '
            return f"Return valid {description} for testing."
        
        elif func_name.startswith('invalid_'):
            # Invalid data helper
            description = readable_name[8:]  # Remove 'invalid '
            return f"Return invalid {description} for testing."
        
        elif func_name.startswith('mock_'):
            # Mock helper
            description = readable_name[5:]  # Remove 'mock '
            return f"Return mock {description} for testing."
        
        elif func_name.startswith('create_'):
            # Creation helper
            description = readable_name[7:]  # Remove 'create '
            return f"Create {description} for testing."
        
        elif func_name.startswith('make_'):
            # Make helper
            description = readable_name[5:]  # Remove 'make '
            return f"Make {description} for testing."
        
        elif func_name.startswith('get_'):
            # Getter helper
            description = readable_name[4:]  # Remove 'get '
            return f"Get {description} for testing."
        
        elif func_name.endswith('_fixture'):
            # Fixture function
            description = readable_name[:-8]  # Remove ' fixture'
            return f"Provide {description} for testing."
        
        elif 'fixture' in func_name:
            # Other fixture patterns
            return f"Provide {readable_name} for testing."
        
        elif func_name.startswith('assert_'):
            # Assertion helper
            description = readable_name[7:]  # Remove 'assert '
            return f"Assert {description}."
        
        elif func_name.startswith('check_'):
            # Check helper
            description = readable_name[6:]  # Remove 'check '
            return f"Check {description}."
        
        elif func_name.startswith('verify_'):
            # Verification helper
            description = readable_name[7:]  # Remove 'verify '
            return f"Verify {description}."
        
        elif func_name.startswith('validate_'):
            # Validation helper
            description = readable_name[9:]  # Remove 'validate '
            return f"Validate {description}."
        
        elif func_name == 'setup':
            return "Set up test environment."
        
        elif func_name == 'teardown':
            return "Clean up test environment."
        
        elif func_name == 'setup_method':
            return "Set up test method."
        
        elif func_name == 'teardown_method':
            return "Clean up test method."
        
        elif func_name == 'setup_class':
            return "Set up test class."
        
        elif func_name == 'teardown_class':
            return "Clean up test class."
        
        else:
            # Generic helper function
            return f"Helper function for {readable_name}."


def find_functions_without_docstrings(file_path: Path) -> List[str]:
    """Find all public functions without docstrings in a file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        tree = ast.parse(content)
        missing_docstrings = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and not node.name.startswith('_'):
                # Check if function has a docstring
                has_docstring = (
                    node.body and
                    isinstance(node.body[0], ast.Expr) and
                    isinstance(node.body[0].value, ast.Constant) and
                    isinstance(node.body[0].value.value, str)
                )
                
                if not has_docstring:
                    missing_docstrings.append(f"{node.name} (line {node.lineno})")
        
        return missing_docstrings
    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
        return []


def add_docstrings_to_file(file_path: Path) -> bool:
    """Add docstrings to functions missing them in a file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.splitlines(keepends=True)
        
        # Parse the file
        tree = ast.parse(content)
        
        # Transform the AST to identify where to add docstrings
        transformer = DocstringAdder(lines)
        transformer.visit(tree)
        
        if not transformer.modifications:
            return False
        
        # Sort modifications by line number in reverse order to avoid offset issues
        transformer.modifications.sort(key=lambda x: x[0], reverse=True)
        
        # Apply modifications
        for line_no, docstring_line in transformer.modifications:
            # Find the line after the function definition
            func_def_line_idx = line_no - 1
            
            # Find the first line of the function body
            insert_idx = func_def_line_idx + 1
            while insert_idx < len(lines) and lines[insert_idx].strip() == '':
                insert_idx += 1
            
            # Insert the docstring
            lines.insert(insert_idx, docstring_line + '\n')
        
        # Write back the modified content
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        return True
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False


def main():
    """Main function to process all test files."""
    # Find the tests directory
    tests_dir = Path('/home/demute/code/CyberDeltaEngine/tests')
    
    if not tests_dir.exists():
        print(f"Tests directory not found: {tests_dir}")
        return
    
    # Find all Python files in tests directory
    python_files = list(tests_dir.rglob('*.py'))
    
    print(f"Found {len(python_files)} Python files in tests directory")
    
    # Process each file
    modified_files = []
    total_missing = 0
    
    for file_path in python_files:
        # Skip __pycache__ directories
        if '__pycache__' in str(file_path):
            continue
        
        # First check what functions are missing docstrings
        missing = find_functions_without_docstrings(file_path)
        
        if missing:
            total_missing += len(missing)
            print(f"\n{file_path.relative_to(tests_dir.parent)}:")
            for func_info in missing:
                print(f"  - {func_info}")
            
            # Add docstrings to the file
            if add_docstrings_to_file(file_path):
                modified_files.append(file_path)
                print(f"  ✓ Added docstrings to {len(missing)} functions")
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  Total functions missing docstrings: {total_missing}")
    print(f"  Files modified: {len(modified_files)}")
    
    if modified_files:
        print(f"\nModified files:")
        for file_path in modified_files:
            print(f"  - {file_path.relative_to(tests_dir.parent)}")


if __name__ == '__main__':
    main()