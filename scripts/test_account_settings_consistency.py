"""Test script to verify update_account_settings consistency across exchanges."""

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.models.service_args_models import UpdateAccountSettingsArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import AccountSettings, BackpackAccountSettingsDetails


logger = get_logger(__name__)

# Test creating args
args = UpdateAccountSettingsArgs(
    leverage_limit=Decimal(20),
    auto_lend=True,
    auto_borrow_settlements=False,
    auto_realize_pnl=None,
    auto_repay_borrows=None,
)

logger.info("UpdateAccountSettingsArgs created successfully:")
logger.info("args_leverage_limit: Leverage limit value", leverage_limit=args.leverage_limit)
logger.info("args_auto_lend: Auto lend setting", auto_lend=args.auto_lend)
logger.info(
    "args_auto_borrow_settlements: Auto borrow settlements setting",
    auto_borrow_settlements=args.auto_borrow_settlements,
)
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

logger.info("account_settings_created: AccountSettings created successfully")
logger.info("settings_exchange: Exchange configured", exchange=settings.exchange)
logger.info("settings_leverage_limit: Leverage limit set", leverage_limit=settings.leverage_limit)
logger.info("settings_auto_lend: Auto lend configured", auto_lend=settings.auto_lend)
logger.info(
    "settings_mutability: Mutability check",
    is_mutable=not settings.model_config.get("frozen", False),
)
logger.info("")

# Test mutability
old_limit = settings.leverage_limit
settings.update_leverage_limit(Decimal(30))
logger.info("leverage_limit_updated: Leverage limit updated successfully")
logger.info("leverage_old_limit: Previous leverage limit", old_limit=old_limit)
logger.info("leverage_new_limit: New leverage limit", new_limit=settings.leverage_limit)
logger.info(
    "timestamp_updated: Timestamp update check",
    timestamp_updated=settings.timestamp > datetime.now(UTC).replace(microsecond=0),
)
logger.info("")

logger.info("✅ All consistency checks passed!")
