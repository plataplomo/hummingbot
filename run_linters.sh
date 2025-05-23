#!/bin/bash

# Script to run mypy, ruff, and pyright, capturing all output.

LINTER_OUTPUT_FILE="linters_output.txt"
# Default target directory is the current directory
TARGET_DIR="."

# --- Argument Parsing ---
# Look for a --dir=<path> argument
for arg in "$@"; do
  case "$arg" in
    --dir=*)
      TARGET_DIR="${arg#*=}" # Extract the value after --dir=
      shift # Remove the --dir argument from the list
      ;;
    *)
      # You could add handling for other arguments if needed,
      # or pass them on to the linters themselves.
      # For now, we'll assume only --dir is script-specific.
      ;;
  esac
done

# Ensure TARGET_DIR is not empty if parsing somehow failed (unlikely with default)
if [ -z "${TARGET_DIR}" ]; then
  echo "Error: TARGET_DIR is empty. Please specify with --dir=<path> or ensure a default is set."
  exit 1
fi

echo "Starting linters (mypy, ruff, pyright) on target directory: '${TARGET_DIR}'"
echo "All output (stdout and stderr) will be saved to '${LINTER_OUTPUT_FILE}'"

# Group the commands using parentheses (...) so that the redirection
# applies to the output of all commands within the group.
# Use semicolons (;) to ensure all commands run sequentially,
# regardless of the success or failure of previous commands.
(
  echo "--- MYPY ---" # Add a header for mypy output
  echo "Running: mypy \"${TARGET_DIR}\""
  mypy "${TARGET_DIR}"
  mypy_status=$?
  echo # Add a blank line for readability in the output file
  echo "Mypy exit status: $mypy_status"
  echo # Add a blank line for readability in the output file

  echo "--- RUFF ---" # Add a header for ruff output
  echo "Running: ruff check \"${TARGET_DIR}\""
  ruff check "${TARGET_DIR}"
  ruff_status=$?
  echo
  echo "Ruff exit status: $ruff_status"
  echo

  echo "--- PYRIGHT ---" # Add a header for pyright output
  echo "Running: pyright \"${TARGET_DIR}\""
  pyright "${TARGET_DIR}" # Pyright often infers the project from current dir or config
                           # If pyright needs a specific argument for the target, adjust here.
                           # For example, if it were `pyright --project .` or `pyright src/`
  pyright_status=$?
  echo
  echo "Pyright exit status: $pyright_status"
) > "${LINTER_OUTPUT_FILE}" 2>&1

echo "----------------------------------------"
echo "Linters finished."
echo "Combined output saved to '${LINTER_OUTPUT_FILE}'."

# Report individual statuses by extracting from the file
echo "Individual exit statuses (from ${LINTER_OUTPUT_FILE}):"
final_mypy_status=$(grep "Mypy exit status:" "${LINTER_OUTPUT_FILE}" | awk '{print $NF}')
final_ruff_status=$(grep "Ruff exit status:" "${LINTER_OUTPUT_FILE}" | awk '{print $NF}')
final_pyright_status=$(grep "Pyright exit status:" "${LINTER_OUTPUT_FILE}" | awk '{print $NF}')

echo "  Mypy:    ${final_mypy_status:-N/A}"
echo "  Ruff:    ${final_ruff_status:-N/A}"
echo "  Pyright: ${final_pyright_status:-N/A}"


if [ "${final_mypy_status}" = "0" ] && [ "${final_ruff_status}" = "0" ] && [ "${final_pyright_status}" = "0" ]; then
  echo "All linters passed successfully."
  exit 0
else
  echo "One or more linters reported issues or failed. Please check '${LINTER_OUTPUT_FILE}'."
  # Exit with a non-zero status if any linter failed
  if [ "${final_mypy_status}" != "0" ]; then exit "${final_mypy_status}"; fi
  if [ "${final_ruff_status}" != "0" ]; then exit "${final_ruff_status}"; fi
  if [ "${final_pyright_status}" != "0" ]; then exit "${final_pyright_status}"; fi
  exit 1 # Fallback error if statuses couldn't be parsed but we know there was an issue
fi