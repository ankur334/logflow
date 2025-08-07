import json
from datetime import datetime, timezone
from confluent_kafka import Producer
import random
import time
from utils.env_loader import get_kafka_config, get_default_topic

class KafkaMessageProducer:
    """Kafka message producer for sending test data to topics
    
    Uses environment configuration from .env.dev file for credentials.
    """
    
    def __init__(self, topic: str, bootstrap_servers=None, sasl_username=None, sasl_password=None):
        """Initialize Kafka producer with configuration
        
        Args:
            topic: Kafka topic to send messages to
            bootstrap_servers: Kafka brokers (uses env var if not provided)
            sasl_username: SASL username (uses env var if not provided)
            sasl_password: SASL password (uses env var if not provided)
        """
        self.topic = topic
        
        # Load Kafka configuration from environment
        kafka_config = get_kafka_config()
        
        self.bootstrap_servers = bootstrap_servers or kafka_config['bootstrap_servers']
        self.sasl_username = sasl_username or kafka_config['sasl_username']
        self.sasl_password = sasl_password or kafka_config['sasl_password']
        
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.sasl_username,
            'sasl.password': self.sasl_password,
            'client.id': 'python-producer'
        }
        
        self.producer = Producer(self.producer_config)
    
    def generate_sample_messages(self, count=10):
        """Generate sample log messages"""
        messages = []
        service_names = ["falcon-mec", "auth-service", "payment-gateway", "user-service"]
        severity_levels = ["INFO", "WARN", "ERROR", "DEBUG"]
        urls = ["/auth/v3/getOtp", "/api/v1/users", "/payments/process", "/health/check"]
        hosts = ["ip-10-10-10-10", "ip-10-10-10-11", "ip-10-10-10-12"]
        envs = ["prod", "staging", "dev"]
        
        for i in range(count):
            mobile = f"98765{43210 + i:05d}"
            timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            
            message = {
                "timestamp": timestamp,
                "serviceName": random.choice(service_names),
                "severityText": random.choice(severity_levels),
                "attributes": {
                    "msg": "proxy-request",
                    "url": random.choice(urls),
                    "mobile": mobile
                },
                "resources": {
                    "host": random.choice(hosts),
                    "env": random.choice(envs)
                },
                "body": json.dumps({"data": {"mobile": mobile}})
            }
            messages.append(message)
        
        return messages
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            print(f'❌ Message delivery failed: {err}')
        else:
            print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def produce_messages(self, messages):
        """Send messages to Kafka"""
        for i, message in enumerate(messages):
            try:
                # Convert message to JSON string
                message_json = json.dumps(message)
                
                # Produce message
                self.producer.produce(
                    self.topic,
                    key=str(i).encode('utf-8'),
                    value=message_json.encode('utf-8'),
                    callback=self.delivery_report
                )
                
                print(f"📤 Sending message {i+1}/{len(messages)}:")
                print(f"   Timestamp: {message['timestamp']}")
                print(f"   Service: {message['serviceName']}")
                print(f"   Mobile: {message['attributes']['mobile']}")
                
                # Poll for delivery reports
                self.producer.poll(0)
                
                # Small delay between messages
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ Error producing message {i}: {e}")
        
        # Wait for all messages to be delivered
        print("\n⏳ Flushing remaining messages...")
        self.producer.flush()
        print("✅ All messages sent!")
    
    def close(self):
        """Clean up producer"""
        self.producer.flush()

def main():
    """Main function to produce test messages"""
    topic = get_default_topic()
    producer = KafkaMessageProducer(topic)
    
    print(f"🚀 Kafka Producer starting...")
    print(f"📝 Topic: {topic}")
    print(f"🔗 Bootstrap servers: {producer.bootstrap_servers}")
    print("-" * 50)
    
    # Generate and send messages
    messages = producer.generate_sample_messages(10)
    producer.produce_messages(messages)
    
    producer.close()

if __name__ == "__main__":
    main()