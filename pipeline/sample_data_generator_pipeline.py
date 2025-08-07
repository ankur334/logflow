"""Sample Data Generator Pipeline - Batch ETL for testing

Symmetric pipeline that generates sample data similar to Kafka producer,
transforms it, and writes to Parquet. Maintains architectural consistency
with Flink streaming pipelines but for batch processing.
"""
import os
from pipeline.base_pipeline import AbstractPipeline


class SampleDataGeneratorPipeline(AbstractPipeline):
    """Batch ETL pipeline: Generate → Transform → Parquet
    
    Maintains symmetry with Flink streaming pipelines but for batch processing.
    Generates data matching kafka_producer format for testing and development.
    """
    
    def __init__(self, extractor, transformer, sink):
        """Initialize batch pipeline with ETL components
        
        Args:
            extractor: Sample data generator (creates test data)
            transformer: Batch transformation logic
            sink: Kafka destination (writes processed data)
        """
        self.extractor = extractor      # Sample data generator
        self.transformer = transformer  # Batch transformations  
        self.sink = sink               # Kafka sink

    @classmethod
    def build(cls, count=50, topic=None, **_):
        """Factory method to create sample data pipeline
        
        Creates batch ETL components for sample data processing:
        - SampleDataGeneratorExtractor: Generates test data matching kafka_producer
        - SampleDataTransformer: Enriches data with quality flags and hot keys
        - SampleDataKafkaSink: Writes to Kafka topic
        
        Args:
            count: Number of sample messages to generate
            topic: Kafka topic to send messages to (uses default from env if not provided)
            
        Returns:
            Configured batch pipeline ready for execution
        """
        # Convert count to integer if it's a string (from CLI)
        if isinstance(count, str):
            count = int(count)
        
        # Import env loader to get default topic if not provided
        from utils.env_loader import get_default_topic
        
        # Default topic for sample data
        topic = topic or get_default_topic()
        
        print("🏗️  Building sample data generator pipeline...")
        print(f"   📊 Sample count: {count}")
        print(f"   📝 Kafka topic: {topic}")
        
        # Import batch processing components 
        from extractor.sample_data_generator_extractor import SampleDataGeneratorExtractor
        from transformer.sample_data_transformer import SampleDataTransformer
        from sink.sample_data_kafka_sink import SampleDataKafkaSink
        
        # Create batch ETL components
        extractor = SampleDataGeneratorExtractor(count=count, batch_size=10)
        transformer = SampleDataTransformer(
            add_processing_time=True,
            enrich_url_patterns=True
        )
        
        # Configure Kafka sink
        sink = SampleDataKafkaSink(topic=topic)
        
        return cls(extractor, transformer, sink)

    def run(self) -> None:
        """Execute the batch data generation pipeline
        
        BATCH EXECUTION FLOW:
        1. Generate sample data using extractor (batches)
        2. Apply transformations with enrichments
        3. Write transformed data to Parquet files  
        4. Display processing summary
        """
        print("🚀 Starting sample data generator pipeline...")
        print("=" * 60)
        
        try:
            # STEP 1: Generate sample data batches
            print("📊 STEP 1: Generating sample data...")
            raw_data = self.extractor.extract()
            
            # STEP 2: Apply transformations 
            print("\n⚙️  STEP 2: Applying transformations...")
            print("   🔥 Hot key promotion: msg, url, mobile → columns")
            print("   ✅ Data quality flags: isValidJSON, hasDataMobile") 
            print("   🎯 URL patterns: Pre-extracted for faster matching")
            print("   🕒 Time fields: Date/hour for partitioning")
            
            transformed_data = self.transformer.transform(raw_data)
            
            # STEP 3: Sink to Kafka
            print("\n📤 STEP 3: Writing to Kafka topic...")
            self.sink.sink(transformed_data)
            
            # STEP 4: Success summary
            print("\n" + "=" * 60)
            print("🎉 PIPELINE EXECUTION COMPLETED!")
            print("✅ Sample data generated and sent to Kafka")
            print(f"📝 Kafka topic: {self.sink.topic}")
            print("📊 Data includes same structure as kafka_producer for consistency")
            
            # Show architectural benefits
            print("\n🏗️  ARCHITECTURAL BENEFITS:")
            print("   📐 Maintains symmetry with Flink streaming pipelines")
            print("   🧪 Provides test data matching production format")
            print("   🔄 Demonstrates batch ETL patterns")
            print("   📦 Uses same abstract base classes as streaming pipelines")
            
        except Exception as e:
            print(f"\n❌ Pipeline execution failed: {e}")
            raise