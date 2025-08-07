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
            sink: Parquet destination (writes processed data)
        """
        self.extractor = extractor      # Sample data generator
        self.transformer = transformer  # Batch transformations  
        self.sink = sink               # Parquet sink

    @classmethod
    def build(cls, count=50, sink_path=None, partition_by_time=True, **_):
        """Factory method to create sample data pipeline
        
        Creates batch ETL components for sample data processing:
        - SampleDataGeneratorExtractor: Generates test data matching kafka_producer
        - SampleDataTransformer: Enriches data with quality flags and hot keys
        - SampleDataParquetSink: Writes to Parquet with optional partitioning
        
        Args:
            count: Number of sample messages to generate
            sink_path: Output path for Parquet files 
            partition_by_time: Whether to partition by date/hour
            
        Returns:
            Configured batch pipeline ready for execution
        """
        # Default sink path for sample data
        sink_path = sink_path or os.path.join(os.getcwd(), "sample_data_output")
        
        print("🏗️  Building sample data generator pipeline...")
        print(f"   📊 Sample count: {count}")
        print(f"   📁 Sink path: {sink_path}")
        print(f"   📅 Time partitioning: {partition_by_time}")
        
        # Import batch processing components 
        from extractor.sample_data_generator_extractor import SampleDataGeneratorExtractor
        from transformer.sample_data_transformer import SampleDataTransformer
        from sink.sample_data_parquet_sink import SampleDataParquetSink
        
        # Create batch ETL components
        extractor = SampleDataGeneratorExtractor(count=count, batch_size=10)
        transformer = SampleDataTransformer(
            add_processing_time=True,
            enrich_url_patterns=True
        )
        
        # Configure sink with optional partitioning
        partition_cols = ['log_date', 'log_hour'] if partition_by_time else []
        sink = SampleDataParquetSink(
            path=sink_path, 
            partition_cols=partition_cols,
            file_prefix="sample_data"
        )
        
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
            
            # STEP 3: Sink to Parquet
            print("\n💾 STEP 3: Writing to Parquet files...")
            self.sink.sink(transformed_data)
            
            # STEP 4: Success summary
            print("\n" + "=" * 60)
            print("🎉 PIPELINE EXECUTION COMPLETED!")
            print("✅ Sample data generated and written to Parquet")
            print(f"📁 Output location: {self.sink.path}")
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