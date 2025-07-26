#!/usr/bin/env python3
"""Extract private endpoint data from test runs for spot trading."""

import json
import subprocess
import yaml
from datetime import datetime
from pathlib import Path

output_dir = Path("workflow/hyperliquid_spot/private_endpoints")
output_dir.mkdir(parents=True, exist_ok=True)

# Look for VCR cassettes with spot order attempts
cassettes_dir = Path("tests/cassettes/apis/hyperliquid/spot")

if cassettes_dir.exists():
    for cassette_file in cassettes_dir.rglob("*.yaml"):
        try:
            with open(cassette_file, 'r') as f:
                data = yaml.safe_load(f)

            if data and 'interactions' in data:
                for idx, interaction in enumerate(data['interactions']):
                    request = interaction.get('request', {})
                    response = interaction.get('response', {})

                    # Look for exchange endpoint requests
                    if 'exchange' in request.get('uri', ''):
                        output_file = output_dir / f"exchange_request_{cassette_file.stem}_{idx}.json"
                        with open(output_file, 'w') as f:
                            json.dump({
                                "timestamp": datetime.now().isoformat(),
                                "cassette": str(cassette_file),
                                "request": {
                                    "uri": request.get('uri'),
                                    "method": request.get('method'),
                                    "body": request.get('body')
                                },
                                "response": {
                                    "status": response.get('status'),
                                    "body": response.get('body', {}).get('string')
                                }
                            }, f, indent=2)
                        print(f"Extracted: {output_file}")

        except Exception as e:
            print(f"Error processing {cassette_file}: {e}")

# Now run a simple test to get real spot order error
print("\nRunning a test to capture spot order response...")

# Run a spot order test
result = subprocess.run([
    "pytest", "-xvs", "--tb=short",
    "tests/integration/apis/hyperliquid/spot/test_hl_spot_orders_positive.py::TestHyperliquidSpotOrdersPrivate::test_place_spot_order_not_implemented",
    "--capture=no"
], capture_output=True, text=True)

print(f"\nTest output saved. Check {output_dir} for extracted data.")

# Also extract error from the test output
if "APIError" in result.stdout or "APIError" in result.stderr:
    with open(output_dir / "test_run_output.txt", 'w') as f:
        f.write("STDOUT:\n")
        f.write(result.stdout)
        f.write("\n\nSTDERR:\n")
        f.write(result.stderr)
    print("Test output saved to test_run_output.txt")
