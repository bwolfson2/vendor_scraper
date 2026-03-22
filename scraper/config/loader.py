"""Load per-state YAML configuration files."""

import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent / "states"


def load_state_config(state_abbr: str) -> dict:
    """Load a single state config by abbreviation."""
    config_path = CONFIG_DIR / f"{state_abbr.lower()}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"No config for state: {state_abbr} (expected {config_path})")
    with open(config_path) as f:
        return yaml.safe_load(f)


def load_all_configs() -> dict[str, dict]:
    """Load all state configs. Returns dict keyed by state abbreviation."""
    configs = {}
    if not CONFIG_DIR.exists():
        logger.warning(f"Config directory not found: {CONFIG_DIR}")
        return configs

    for path in sorted(CONFIG_DIR.glob("*.yaml")):
        try:
            with open(path) as f:
                config = yaml.safe_load(f)
            abbr = config.get("abbreviation", path.stem.upper())
            configs[abbr] = config
        except Exception as e:
            logger.error(f"Error loading {path}: {e}")

    logger.info(f"Loaded {len(configs)} state configs")
    return configs


def get_adapter_class(adapter_name: str):
    """Import and return the adapter class for a given adapter type."""
    adapters = {
        "socrata": ("scraper.adapters.socrata", "SocrataAdapter"),
        "aspnet": ("scraper.adapters.aspnet", "ASPNetAdapter"),
        "opengov": ("scraper.adapters.opengov", "OpenGovAdapter"),
        "rest_api": ("scraper.adapters.rest_api", "RESTAPIAdapter"),
        "tableau": ("scraper.adapters.tableau", "TableauAdapter"),
        "bulk_download": ("scraper.adapters.bulk_download", "BulkDownloadAdapter"),
        "playwright": ("scraper.adapters.playwright_scraper", "PlaywrightAdapter"),
        "ks_download": ("scraper.adapters.ks_download", "KSDownloadAdapter"),
        "spending_app": ("scraper.adapters.socrata_spending_app", "SocrataSpendingAppAdapter"),
    }

    if adapter_name not in adapters:
        raise ValueError(f"Unknown adapter: {adapter_name}. Available: {list(adapters.keys())}")

    module_path, class_name = adapters[adapter_name]
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)
