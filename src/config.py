"""
Configuration management for the sales data pipeline.
"""
import os
import yaml


DEFAULT_CONFIG = {
    "pipeline": {
        "name": "sales_data_pipeline",
        "version": "1.0.0",
    },
    "storage": {
        "raw_path": "/data/raw",
        "processed_path": "/data/processed",
        "delta_path": "/data/delta",
    },
    "spark": {
        "app_name": "SalesDataPipeline",
        "shuffle_partitions": 200,
    },
    "data": {
        "date_format": "yyyy-MM-dd",
        "timestamp_format": "yyyy-MM-dd HH:mm:ss",
    },
}


def load_config(config_path: str = None) -> dict:
    """Load configuration from a YAML file, falling back to defaults.

    Args:
        config_path: Path to the YAML configuration file. If None or the file
                     does not exist, the default configuration is returned.

    Returns:
        Configuration dictionary.
    """
    config = DEFAULT_CONFIG.copy()

    if config_path and os.path.exists(config_path):
        with open(config_path, "r") as f:
            file_config = yaml.safe_load(f) or {}
        config = _deep_merge(config, file_config)

    return config


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into *base*.

    Args:
        base: The base dictionary.
        override: Values that take precedence over *base*.

    Returns:
        Merged dictionary.
    """
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result
