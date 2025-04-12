#!/bin/bash

echo "--- Mypy Error Count (from summary) ---"
# Run mypy, capture all output (stdout & stderr), allow non-zero exit
mypy_output=$(mypy cyberdelta tests 2>&1 || true)

# Try to find the explicit error count line "Found X errors..."
mypy_errors=$(echo "$mypy_output" | grep -oE 'Found [0-9]+ errors?' | grep -oE '[0-9]+') # Handle "error" vs "errors"

# Check if we found a number
if [[ -n "$mypy_errors" ]]; then
  echo "Mypy reported errors: $mypy_errors"
# Check if the success message is present
elif echo "$mypy_output" | grep -q "Success: no issues found"; then
  echo "Mypy reported errors: 0"
# Handle cases where neither expected line is found but there was output
elif [[ -n "$mypy_output" ]]; then
  echo "Mypy: Could not determine error count from output."
  echo "--- Mypy Output Start ---"
  echo "$mypy_output"
  echo "--- Mypy Output End ---"
# Handle no output at all (might indicate config issue)
else
    echo "Mypy: No output received. Check installation/configuration."
fi


echo "" # Separator


echo "--- Ruff Error Count (from summary) ---"
# Run ruff, capture all output (stdout & stderr), allow non-zero exit
# NOTE: Ruff often prints errors to stderr and summary to stdout, or vice-versa depending on invocation/errors. Capture both.
ruff_output=$(ruff check cyberdelta tests 2>&1 || true)

# Try to find the explicit error count line "Found X error(s)."
ruff_errors=$(echo "$ruff_output" | grep -oE 'Found [0-9]+ errors?\.?$' | grep -oE '[0-9]+') # Added optional . and s at the end

# Check if we found a number
if [[ -n "$ruff_errors" ]]; then
  echo "Ruff reported errors: $ruff_errors"
# If no error count line and the output is empty, assume success (ruff check is often quiet on success)
elif [[ -z "$ruff_output" ]]; then
  echo "Ruff reported errors: 0"
# Handle cases where there's output but no summary line (might be errors without summary, or other messages)
else
  # Check if the output *looks* like it contains errors even without the summary line
  if echo "$ruff_output" | grep -qE ':[0-9]+:[0-9]+: [A-Z]{1,4}[0-9]+ '; then
      echo "Ruff: Output found (likely errors), but couldn't parse summary count."
      # You *could* fallback to line count here as an estimate if needed:
      # estimate=$(echo "$ruff_output" | grep -cE ':[0-9]+:[0-9]+: [A-Z]{1,4}[0-9]+ ')
      # echo "Ruff reported errors (estimate): $estimate"
  else
      echo "Ruff: Non-empty output without standard error summary found. Assuming 0 errors."
  fi
  # Optionally print the unexpected output for debugging
  echo "--- Ruff Output Start ---"
  echo "$ruff_output"
  echo "--- Ruff Output End ---"
fi