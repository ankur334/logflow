# config/config_reader.py

import os
import json
import re
import logging

import yaml
from pathlib import Path

log = logging.getLogger(__name__)


def _substitute_env_vars(value: str, allow_missing: bool = False) -> str:
    """
    Substitute environment variables in the format ${VAR_NAME} or $VAR_NAME.
    
    Args:
        value: String that may contain environment variable placeholders
        allow_missing: If True, missing variables are left as-is instead of raising error
        
    Returns:
        String with environment variables substituted
        
    Raises:
        ValueError: If required environment variable is not set and allow_missing=False
    """
    if not isinstance(value, str):
        return value
    
    # Pattern to match ${VAR_NAME} or $VAR_NAME  
    pattern = r'\$\{([^}]+)\}|\$([A-Z_][A-Z0-9_]*)'
    
    def replace_var(match):
        # Get variable name from either ${VAR} or $VAR format
        var_name = match.group(1) or match.group(2)
        
        if var_name in os.environ:
            env_value = os.environ[var_name]
            log.debug(f"Substituted ${{{var_name}}} with environment value")
            return env_value
        elif allow_missing:
            log.warning(f"Environment variable '{var_name}' not set, keeping placeholder")
            return match.group(0)  # Return original placeholder
        else:
            raise ValueError(f"Required environment variable '{var_name}' is not set")
    
    try:
        return re.sub(pattern, replace_var, value)
    except ValueError as e:
        log.error(f"Environment variable substitution failed: {e}")
        raise


def read_properties(filepath, strict=True):
    """
    Reads a .properties file and substitutes environment variables.
    
    Supports both ${VAR_NAME} and direct environment variable lookup.
    Environment variables take precedence over file values.
    
    Args:
        filepath: Path to the .properties file
        strict: If True, raises error for missing env vars; if False, logs warning
        
    Returns:
        Dictionary with configuration values
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If required environment variables are missing (strict=True)
    """
    config = {}
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"Config file not found: {filepath}")
    
    log.info(f"Loading configuration from: {filepath} (strict={strict})")
    
    with open(filepath, "r") as fh:
        for line_num, line in enumerate(fh, 1):
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue
                
            if "=" not in line:
                log.warning(f"Skipping invalid line {line_num} in {filepath}: {line}")
                continue
                
            try:
                parameter, value = line.split("=", 1)
                parameter = parameter.strip()
                value = value.strip()
                
                # First check if there's a direct environment variable override
                env_key = parameter.upper().replace('.', '_').replace('-', '_')
                if env_key in os.environ:
                    config[parameter] = os.environ[env_key]
                    log.debug(f"Using environment override for {parameter}")
                else:
                    # Substitute any ${VAR} placeholders in the value
                    config[parameter] = _substitute_env_vars(value, allow_missing=not strict)
                    
            except ValueError as e:
                log.error(f"Error processing line {line_num} in {filepath}: {e}")
                if strict:
                    raise ValueError(f"Configuration error in {filepath} line {line_num}: {e}")
                else:
                    log.warning(f"Configuration warning in {filepath} line {line_num}: {e}")
                    continue
                
    log.info(f"Loaded {len(config)} configuration parameters")
    return config


def validate_kafka_config(config):
    """
    Validate that required Kafka configuration parameters are present and valid.
    
    Args:
        config: Dictionary with Kafka configuration
        
    Raises:
        ValueError: If required parameters are missing or invalid
    """
    required_params = ['bootstrap.servers', 'sasl.username', 'sasl.password']
    missing_params = []
    placeholder_params = []
    
    for param in required_params:
        if param not in config:
            missing_params.append(param)
        elif isinstance(config[param], str) and ('${' in config[param] or config[param].startswith('$')):
            placeholder_params.append(param)
    
    if missing_params:
        raise ValueError(f"Missing required Kafka configuration parameters: {missing_params}")
    
    if placeholder_params:
        raise ValueError(f"Kafka configuration contains unresolved placeholders: {placeholder_params}")
    
    # Validate bootstrap servers format
    bootstrap_servers = config.get('bootstrap.servers', '')
    if not bootstrap_servers or ':' not in bootstrap_servers:
        raise ValueError(f"Invalid bootstrap.servers format: {bootstrap_servers}")
    
    log.info("Kafka configuration validation passed")
    return True


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
