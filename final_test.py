#!/usr/bin/env python3
import asyncio
from pathlib import Path
from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.config.secrets_manager import SecretsManager
from cyberdelta.apis.backpack.bp_api import BackpackAPI

async def test():
    # Load configuration using the same approach as test fixtures
    config_path = Path("tests/config/test_config.yaml")
    secrets_path = Path("tests/config/test_secrets.yaml")
    
    config_manager = ConfigManager(str(config_path))
    secrets_manager = SecretsManager(str(secrets_path))
    
    app_settings = config_manager.settings
    secrets_config = secrets_manager.secrets_data
    
    bp_config = app_settings.exchanges["backpack"]
    bp_secrets = secrets_config.exchanges["backpack"]
    
    api = BackpackAPI(exchange_config=bp_config, exchange_secrets=bp_secrets)
    
    try:
        print('🔍 TESTING BACKPACK BALANCE ENDPOINTS')
        print('=' * 60)
        
        print('1. SPOT BALANCES (/api/v1/capital):')
        spot_balances = await api.get_balances()
        print(f'   Keys: {list(spot_balances.keys())}')
        if 'USDC' in spot_balances:
            usdc = spot_balances['USDC']
            print(f'   USDC Spot: available=${usdc.available_quantity}, total=${usdc.total_quantity}')
        else:
            print('   USDC: NOT FOUND in spot')
        
        print()
        print('2. COLLATERAL ENDPOINT (/api/v1/capital/collateral):')
        try:
            # Use the HTTP client requester directly
            http_client_requester = api.account_service._http_client_requester
            
            # Make request to collateral endpoint
            raw_data, status_code, headers = await http_client_requester(
                method="GET",
                endpoint="/api/v1/capital/collateral",
                params={},
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )
            
            print(f'   Status: {status_code}')
            if status_code == 200 and raw_data is not None:
                print(f'   Collateral response: {raw_data}')
                if isinstance(raw_data, dict):
                    for key, value in raw_data.items():
                        if 'USDC' in str(key).upper():
                            print(f'   🎯 FOUND USDC IN COLLATERAL: {key} = {value}')
                        print(f'   Collateral key: {key} = {value}')
            else:
                print(f'   Error: Status {status_code}, Data: {raw_data}')
                
        except Exception as e:
            print(f'   Exception: {e}')
        
        print()
        print('3. SUMMARY:')
        spot_usdc = spot_balances.get('USDC')
        if spot_usdc and spot_usdc.available_quantity > 0:
            print(f'   ✅ USDC found in SPOT: ${spot_usdc.available_quantity}')
        else:
            print(f'   ❌ USDC in SPOT: ${spot_usdc.available_quantity if spot_usdc else "None"}')
            print('   🔍 Check collateral endpoint results above for your $1 USDC')
        
    finally:
        await api.close()

if __name__ == "__main__":
    asyncio.run(test()) 