# config/config_reader.py

import os
import json

import yaml
from pathlib import Path


def read_properties(filepath):
    """
    Reads a .properties file and overlays environment variables if present.
    """
    config = {}
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Config file not found: {filepath}")
    with open(filepath, "r") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                if "=" in line:
                    parameter, value = line.split("=", 1)
                    env_key = parameter.upper().replace('.', '_')
                    config[parameter] = os.environ.get(env_key, value.strip())
    return config


def read_yaml(filepath):
    """
    Reads a YAML config file.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Config file not found: {filepath}")
    with open(filepath, "r") as fh:
        return yaml.safe_load(fh)


def read_json(filepath):
    """
    Reads a JSON config file.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Config file not found: {filepath}")
    with open(filepath, "r") as fh:
        return json.load(fh)
