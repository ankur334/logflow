"""Environment variable loader utility

Loads environment variables from .env.dev file for development.
This centralizes environment configuration and keeps secrets out of code.
"""
import os
from pathlib import Path
from dotenv import load_dotenv


def load_dev_env():
    """Load environment variables from .env.dev file
    
    This function loads the development environment configuration
    from the .env.dev file in the project root.
    
    Returns:
        bool: True if .env.dev file was found and loaded, False otherwise
    """
    # Find project root (where .env.dev is located)
    project_root = Path(__file__).parent.parent
    env_file = project_root / ".env.dev"
    
    if env_file.exists():
        load_dotenv(env_file)
        return True
    else:
        print(f"Warning: {env_file} not found. Using system environment variables.")
        return False


def get_kafka_config():
    """Get Kafka configuration from environment variables
    
    Returns:
        dict: Kafka configuration dictionary with bootstrap servers, 
              SASL username, and SASL password
    """
    return {
        'bootstrap_servers': os.getenv('BOOTSTRAP_SERVERS'),
        'sasl_username': os.getenv('SASL_USERNAME'),  
        'sasl_password': os.getenv('SASL_PASSWORD')
    }


def get_default_topic():
    """Get default Kafka topic from environment
    
    Returns:
        str: Default Kafka topic name
    """
    return os.getenv('KAFKA_TOPIC', 'last9Topic')


def get_default_sink_path():
    """Get default sink path from environment
    
    Returns:
        str: Default sink path for output files
    """
    return os.getenv('SINK_PATH', 'file:///tmp/last9_parquet')


def get_raw_layer_path():
    """Get raw layer path for bronze/raw data
    
    Returns:
        str: Raw layer path for unprocessed data
    """
    return os.getenv('SINK_PATH_RAW', 'file:///tmp/last9_raw_layer')


def get_silver_layer_path():
    """Get silver layer path for processed/optimized data
    
    Returns:
        str: Silver layer path for optimized data
    """
    return os.getenv('SINK_PATH_SILVER', 'file:///tmp/last9_silver_layer')


# Load environment variables when module is imported
load_dev_env()