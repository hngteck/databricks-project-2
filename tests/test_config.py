"""
Tests for src/config.py
"""
import os
import tempfile

import pytest
import yaml

from src.config import load_config, _deep_merge, DEFAULT_CONFIG


class TestDeepMerge:
    def test_simple_override(self):
        base = {"a": 1, "b": 2}
        override = {"b": 99, "c": 3}
        result = _deep_merge(base, override)
        assert result == {"a": 1, "b": 99, "c": 3}

    def test_nested_merge(self):
        base = {"spark": {"app_name": "base", "shuffle_partitions": 200}}
        override = {"spark": {"shuffle_partitions": 50}}
        result = _deep_merge(base, override)
        assert result["spark"]["app_name"] == "base"
        assert result["spark"]["shuffle_partitions"] == 50

    def test_base_not_mutated(self):
        base = {"a": {"x": 1}}
        override = {"a": {"x": 2}}
        _deep_merge(base, override)
        assert base["a"]["x"] == 1


class TestLoadConfig:
    def test_returns_defaults_when_no_file(self):
        config = load_config(None)
        assert config["pipeline"]["name"] == DEFAULT_CONFIG["pipeline"]["name"]
        assert config["spark"]["shuffle_partitions"] == 200

    def test_returns_defaults_when_file_missing(self):
        config = load_config("/nonexistent/path/settings.yaml")
        assert config["pipeline"]["name"] == DEFAULT_CONFIG["pipeline"]["name"]

    def test_loads_and_merges_yaml(self):
        custom = {"spark": {"shuffle_partitions": 100}}
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as tmp:
            yaml.dump(custom, tmp)
            tmp_path = tmp.name

        try:
            config = load_config(tmp_path)
            assert config["spark"]["shuffle_partitions"] == 100
            # Default values not in file should still be present
            assert config["pipeline"]["name"] == DEFAULT_CONFIG["pipeline"]["name"]
        finally:
            os.unlink(tmp_path)

    def test_empty_yaml_returns_defaults(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as tmp:
            tmp.write("")
            tmp_path = tmp.name
        try:
            config = load_config(tmp_path)
            assert config == DEFAULT_CONFIG
        finally:
            os.unlink(tmp_path)
