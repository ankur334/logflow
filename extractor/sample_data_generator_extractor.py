"""Sample Data Generator Extractor - Symmetric to Kafka Producer

Generates the same data format as kafka_producer.py but as a batch extractor.
This maintains symmetry in the codebase architecture.
"""
import json
from datetime import datetime, timezone
import random
from typing import Iterator, Dict, Any
from extractor.base_extractor import AbstractExtractor


class SampleDataGeneratorExtractor(AbstractExtractor):
    """Generates sample log data matching kafka_producer format
    
    Creates data with the same structure as KafkaMessageProducer for
    testing and development purposes. Maintains symmetry with the
    existing Kafka producer implementation.
    """
    
    def __init__(self, count: int = 10, batch_size: int = 5):
        """Initialize sample data generator
        
        Args:
            count: Total number of messages to generate
            batch_size: Number of messages per batch (for streaming simulation)
        """
        self.count = count
        self.batch_size = batch_size
        
        # Same sample data as kafka_producer.py for consistency
        self.service_names = ["falcon-mec", "auth-service", "payment-gateway", "user-service"]
        self.severity_levels = ["INFO", "WARN", "ERROR", "DEBUG"]  
        self.urls = ["/auth/v3/getOtp", "/api/v1/users", "/payments/process", "/health/check"]
        self.hosts = ["ip-10-10-10-10", "ip-10-10-10-11", "ip-10-10-10-12"]
        self.envs = ["prod", "staging", "dev"]

    def generate_sample_message(self, index: int) -> Dict[str, Any]:
        """Generate a single sample message matching kafka_producer format
        
        Args:
            index: Message index for unique mobile numbers
            
        Returns:
            Dictionary with same structure as kafka_producer messages
        """
        mobile = f"98765{43210 + index:05d}"
        timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        
        return {
            "timestamp": timestamp,
            "serviceName": random.choice(self.service_names),
            "severityText": random.choice(self.severity_levels),
            "attributes": {
                "msg": "proxy-request", 
                "url": random.choice(self.urls),
                "mobile": mobile
            },
            "resources": {
                "host": random.choice(self.hosts),
                "env": random.choice(self.envs)
            },
            "body": json.dumps({"data": {"mobile": mobile}})
        }

    def extract(self) -> Iterator[Dict[str, Any]]:
        """Extract sample data in batches
        
        Yields batches of sample messages matching kafka_producer format.
        This maintains the same data structure for consistency.
        
        Yields:
            Batches of sample log messages
        """
        print(f"📊 Generating {self.count} sample messages in batches of {self.batch_size}...")
        
        for start_idx in range(0, self.count, self.batch_size):
            end_idx = min(start_idx + self.batch_size, self.count)
            batch = []
            
            for i in range(start_idx, end_idx):
                message = self.generate_sample_message(i)
                batch.append(message)
                
                print(f"📝 Generated message {i+1}/{self.count}:")
                print(f"   Service: {message['serviceName']}")
                print(f"   Mobile: {message['attributes']['mobile']}")
                print(f"   URL: {message['attributes']['url']}")
            
            print(f"📦 Yielding batch {start_idx//self.batch_size + 1} with {len(batch)} messages")
            yield batch
            
        print("✅ Data generation completed!")

    def register_in_flink(self, t_env) -> str:
        """Not implemented for sample data generator
        
        This extractor is designed for batch processing, not Flink streaming.
        Use FlinkKafkaJsonSource for Flink streaming pipelines.
        """
        raise NotImplementedError("Sample data generator is for batch processing only")