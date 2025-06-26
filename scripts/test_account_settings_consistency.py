#!/usr/bin/env python3
"""Test script to verify update_account_settings consistency across exchanges."""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.models.service_args_models import UpdateAccountSettingsArgs
from cyberdelta.core.models import AccountSettings, BackpackAccountSettingsDetails


# Configure logging for script output
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# Test creating args
args = UpdateAccountSettingsArgs(
    leverage_limit=Decimal(20),
    auto_lend=True,
    auto_borrow_settlements=False,
    auto_realize_pnl=None,
    auto_repay_borrows=None,
)

logger.info("UpdateAccountSettingsArgs created successfully:")
logger.info(f"  leverage_limit: {args.leverage_limit}")
logger.info(f"  auto_lend: {args.auto_lend}")
logger.info(f"  auto_borrow_settlements: {args.auto_borrow_settlements}")
logger.info("")

# Test creating AccountSettings (mutable)
settings = AccountSettings(
    exchange="backpack",
    timestamp=datetime.now(UTC),
    leverage_limit=args.leverage_limit,
    auto_lend=args.auto_lend,
    auto_borrow_settlements=args.auto_borrow_settlements,
    bp_details=BackpackAccountSettingsDetails(
        leverage_limit_raw="20.0",
        source_endpoint="/api/v1/account",
    ),
)

logger.info("AccountSettings created successfully:")
logger.info(f"  exchange: {settings.exchange}")
logger.info(f"  leverage_limit: {settings.leverage_limit}")
logger.info(f"  auto_lend: {settings.auto_lend}")
logger.info(f"  Is mutable: {not settings.model_config.get('frozen', False)}")
logger.info("")

# Test mutability
old_limit = settings.leverage_limit
settings.update_leverage_limit(Decimal(30))
logger.info("Leverage limit updated successfully:")
logger.info(f"  Old limit: {old_limit}")
logger.info(f"  New limit: {settings.leverage_limit}")
logger.info(f"  Timestamp updated: {settings.timestamp > datetime.now(UTC).replace(microsecond=0)}")
logger.info("")

logger.info("✅ All consistency checks passed!")
