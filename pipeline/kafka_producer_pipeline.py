from pipeline.base_pipeline import AbstractPipeline
from producer.kafka_producer import KafkaMessageProducer

class KafkaProducerPipeline(AbstractPipeline):
    """Pipeline to produce test messages to Kafka"""
    
    def __init__(self, topic: str, message_count: int = 10):
        self.topic = topic
        self.message_count = message_count
        self.producer = KafkaMessageProducer(topic)
    
    @classmethod
    def build(cls, topic="last9Topic", count=10, **kwargs):
        """Build the producer pipeline"""
        return cls(topic=topic, message_count=int(count))
    
    def run(self):
        """Execute the pipeline to send messages"""
        print(f"🚀 Kafka Producer Pipeline starting...")
        print(f"📝 Topic: {self.topic}")
        print(f"📊 Message count: {self.message_count}")
        print("-" * 50)
        
        # Generate and send messages
        messages = self.producer.generate_sample_messages(self.message_count)
        self.producer.produce_messages(messages)
        
        # Clean up
        self.producer.close()
        print("\n✅ Pipeline completed successfully!")