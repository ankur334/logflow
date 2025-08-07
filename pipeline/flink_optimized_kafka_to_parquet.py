"""Optimized Flink Pipeline for Last9 Query Performance

This pipeline implements performance optimizations based on query analysis:
- Hot key promotion to reduce JSON parsing at query time
- Data quality flags for faster filtering  
- Time-based partitioning for efficient time range queries
- Pre-filtering to reduce data volume

Expected Performance Improvements:
- Query latency: 30s → <5s  
- Time range queries: 15s → <2s
- Ingest latency: <2.5s (p50), <5s (p99)
"""
import os
from pyflink.table import EnvironmentSettings, TableEnvironment
from pipeline.base_pipeline import AbstractPipeline
from utils.env_loader import get_default_topic, get_silver_layer_path


def _set(t_env, k, v): 
    """Helper to set Flink configuration properties"""
    t_env.get_config().get_configuration().set_string(k, v)


class FlinkOptimizedKafkaToParquetPipeline(AbstractPipeline):
    """Optimized Flink streaming pipeline for Last9 query performance
    
    Key Optimizations:
    1. Hot Key Promotion: Extract msg, url, mobile to columns
    2. Data Quality Flags: Pre-compute isValidJSON, hasDataMobile  
    3. Time Partitioning: Date/hour partitioning for range queries
    4. Pre-filtering: Apply common filters during ingestion
    5. Schema Optimization: Order columns by query frequency
    
    Query Performance Benefits:
    - Attribute filters: No JSON parsing needed at query time
    - Time range queries: Partition pruning reduces scan volume
    - Data quality checks: Pre-computed flags eliminate runtime validation
    - URL patterns: Pre-extracted patterns for faster regex matching
    """
    
    def __init__(self, extractor, transformer, sink,
                 checkpoint_interval: str = "30 s", pipeline_jars: str | None = None):
        """Initialize optimized pipeline with ETL components
        
        Args:
            extractor: Kafka source (reads streaming data)
            transformer: Optimized transformation logic with hot key promotion
            sink: Optimized Parquet sink with partitioning
            checkpoint_interval: Flink checkpointing frequency
            pipeline_jars: JAR dependencies for connectors
        """
        self.extractor = extractor        # Kafka source
        self.transformer = transformer    # Optimized transformations
        self.sink = sink                 # Optimized Parquet sink
        self.checkpoint_interval = checkpoint_interval
        self.pipeline_jars = pipeline_jars or os.environ.get("FLINK_PIPELINE_JARS")

    @classmethod
    def build(cls, topic=None, sink_path=None, **_):
        """Factory method to create optimized pipeline
        
        Creates specialized components for Last9 query optimization:
        - FlinkKafkaJsonSource: Standard Kafka connector
        - FlinkOptimizedLogTransform: Hot key promotion and quality flags
        - FlinkOptimizedParquetSink: Partitioned sink with optimized schema
        
        Args:
            topic: Kafka topic to read from (uses env default if None)
            sink_path: Output path for Parquet files (uses env default if None)
            
        Returns:
            Configured optimized pipeline ready for execution
        """
        # Use environment defaults - SILVER LAYER for optimized pipeline
        topic = topic or get_default_topic()
        sink_path = sink_path or get_silver_layer_path()
        
        print("🚀 Building optimized pipeline for Last9 query performance...")
        print(f"   📝 Topic: {topic}")
        print(f"   📁 Sink path: {sink_path}")
        
        # Import optimized components for Last9 query performance
        from extractor.flink_kafka_extractor import FlinkKafkaJsonSource
        from transformer.flink_optimized_log_transform import FlinkOptimizedLogTransform  # Optimized transformer
        from sink.flink_optimized_parquet_sink import FlinkOptimizedParquetSink          # Optimized sink
        
        # Create optimized ETL components
        extractor = FlinkKafkaJsonSource(topic=topic)                     # Standard Kafka source
        transformer = FlinkOptimizedLogTransform()                        # OPTIMIZED: Hot keys + quality flags
        sink = FlinkOptimizedParquetSink(path=sink_path)                  # OPTIMIZED: Partitioned sink
        
        return cls(extractor, transformer, sink)

    def run(self) -> None:
        """Execute the optimized Flink streaming pipeline
        
        OPTIMIZATION FLOW:
        1. Create TableEnvironment with optimized settings
        2. Load JAR dependencies for connectors
        3. Configure checkpointing for low latency and fault tolerance
        4. Register Kafka source table
        5. Apply optimized transformations (hot keys, quality flags)
        6. Register optimized Parquet sink with partitioning
        7. Execute streaming job with performance monitoring
        """
        print("⚡ Starting optimized Flink streaming pipeline...")
        
        # STEP 1: Create optimized TableEnvironment
        settings = EnvironmentSettings.in_streaming_mode()
        t_env = TableEnvironment.create(settings)
        
        # STEP 2: Load JAR dependencies
        jars_dir = os.path.abspath("jars")
        if os.path.exists(jars_dir):
            jar_files = [f for f in os.listdir(jars_dir) 
                        if f.endswith(".jar") and os.path.getsize(os.path.join(jars_dir, f)) > 1000]
            jar_urls = ";".join([f"file://{os.path.join(jars_dir, jar)}" for jar in jar_files])
            if jar_urls:
                _set(t_env, "pipeline.jars", jar_urls)
                
        # STEP 3: Configure optimized checkpointing
        # Faster checkpointing for low latency requirements (p50 < 2.5s, p99 < 5s)
        _set(t_env, "execution.checkpointing.interval", "5 s")            # More frequent checkpoints
        _set(t_env, "execution.checkpointing.min-pause", "1 s")           # Reduce pause between checkpoints
        _set(t_env, "execution.checkpointing.timeout", "30 s")            # Quick timeout for faster recovery
        
        # STEP 4: Register Kafka source
        print("📡 Registering Kafka source...")
        src_table = self.extractor.register_in_flink(t_env)
        
        # STEP 5: Apply optimized transformations
        print("⚙️  Applying optimized transformations...")
        print("   🔥 Hot key promotion: msg, url, mobile → columns")
        print("   ✅ Data quality flags: isValidJSON, hasDataMobile")
        print("   🕒 Time buckets: 10min, 1hr aggregation intervals")
        print("   🎯 URL patterns: Pre-extracted for faster matching")
        
        optimized_table = self.transformer.apply_in_flink(t_env, src_table)
        
        # STEP 6: Register optimized Parquet sink
        print("💾 Registering optimized Parquet sink...")
        self.sink.register_sink_in_flink(t_env)
        
        # STEP 7: Execute optimized streaming job via sink's INSERT method
        print("🚀 Executing optimized streaming pipeline...")
        
        # Delegate INSERT logic to the sink (proper separation of concerns)
        result = self.sink.insert_into_flink(t_env, optimized_table)
        
        # Display optimization summary
        print("🎯 OPTIMIZATION SUMMARY:")
        print("   📊 Schema: Hot keys promoted to columns")
        print("   🗓️  Partitioning: Date/hour for fast time range queries")
        print("   ⚡ Quality flags: Pre-computed for faster filtering")  
        print("   🔍 URL patterns: Pre-extracted for regex performance")
        print("   💾 File size: 128MB target for optimal query performance")
        print()
        print("📈 EXPECTED PERFORMANCE IMPROVEMENTS:")
        print("   🚀 Query latency: 30s → <5s")
        print("   ⏱️  Time range queries: 15s → <2s") 
        print("   🔥 Attribute queries: JSON parsing eliminated")
        print("   📅 Date filters: Partition pruning enabled")
        print()
        print(f"✅ Pipeline submitted. Job ID: {result.get_job_client().get_job_id()}")
        print("🔄 Streaming optimized data to partitioned Parquet files...")
        print("Press Ctrl+C to stop")
        
        # Keep the streaming job running
        result.wait()