"""Test cases for environment loader utility

This file tests the environment variable loading functionality
to ensure credentials are properly loaded from .env.dev file.
"""
import pytest
from utils.env_loader import get_kafka_config, get_default_topic, get_default_sink_path


class TestEnvironmentLoader:
    """Test environment variable loading functionality"""
    
    def test_kafka_config_loaded(self):
        """Test that Kafka configuration is loaded from environment
        
        This verifies that the .env.dev file is properly loaded
        and Kafka credentials are available.
        """
        # WHEN: Getting Kafka configuration
        kafka_config = get_kafka_config()
        
        # THEN: Should have all required fields
        assert 'bootstrap_servers' in kafka_config
        assert 'sasl_username' in kafka_config  
        assert 'sasl_password' in kafka_config
        
        # Should have actual values (not None)
        assert kafka_config['bootstrap_servers'] is not None
        assert kafka_config['sasl_username'] is not None
        assert kafka_config['sasl_password'] is not None
        
        print("✅ Kafka configuration loaded successfully")
        print(f"   Bootstrap servers: {kafka_config['bootstrap_servers']}")
        print(f"   Username: {kafka_config['sasl_username'][:4]}****")
        
    def test_default_topic_loaded(self):
        """Test that default topic is loaded from environment"""
        # WHEN: Getting default topic
        topic = get_default_topic()
        
        # THEN: Should return the topic from environment
        assert topic == "last9Topic"
        print(f"✅ Default topic: {topic}")
        
    def test_default_sink_path_loaded(self):
        """Test that default sink path is loaded from environment"""  
        # WHEN: Getting default sink path
        sink_path = get_default_sink_path()
        
        # THEN: Should return the path from environment
        assert sink_path.startswith("file://")
        print(f"✅ Default sink path: {sink_path}")


if __name__ == "__main__":
    # Run basic tests to verify environment loading
    test = TestEnvironmentLoader()
    test.test_kafka_config_loaded()
    test.test_default_topic_loaded() 
    test.test_default_sink_path_loaded()
    print("\n🎯 All environment loading tests passed!")