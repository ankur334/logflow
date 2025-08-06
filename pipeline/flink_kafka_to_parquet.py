"""Flink Kafka to Parquet Pipeline - Main streaming ETL implementation

This pipeline demonstrates core Flink concepts:
- Table API for high-level stream processing
- Streaming execution with checkpointing
- Multiple sinks (print for debugging + parquet for storage)
- JAR dependency management for Kafka/Parquet connectors
"""
import os
from pyflink.table import EnvironmentSettings, TableEnvironment
from pipeline.base_pipeline import AbstractPipeline


def _set(t_env, k, v): 
    """Helper to set Flink configuration properties"""
    t_env.get_config().get_configuration().set_string(k, v)


class FlinkKafkaToParquetPipeline(AbstractPipeline):
    """Main Flink streaming pipeline: Kafka → Transform → Parquet
    
    Key Flink Concepts:
    - Uses Table API for SQL-like operations on streaming data
    - Implements checkpointing for fault tolerance
    - Supports multiple sinks for debugging and storage
    """
    
    def __init__(self, extractor, transformer, sink,
                 checkpoint_interval: str = "30 s", pipeline_jars: str | None = None):
        """Initialize pipeline with ETL components
        
        Args:
            extractor: Kafka source (reads streaming data)
            transformer: Data transformation logic
            sink: Parquet destination (writes processed data)
            checkpoint_interval: Flink checkpointing frequency
            pipeline_jars: JAR dependencies for connectors
        """
        self.extractor = extractor        # Kafka source
        self.transformer = transformer    # Data transformations
        self.sink = sink                 # Parquet sink
        self.checkpoint_interval = checkpoint_interval
        self.pipeline_jars = pipeline_jars or os.environ.get("FLINK_PIPELINE_JARS")

    @classmethod
    def build(cls, topic=None, sink_path=None, **_):
        """Factory method to create pipeline with Flink components
        
        FLINK CONCEPT: Component composition
        Creates specialized Flink implementations of abstract ETL components:
        - FlinkKafkaJsonSource: Kafka Table API source connector
        - FlinkLogPromoteTransform: SQL transformations for nested field extraction
        - FlinkFilesystemParquetSink: Filesystem connector with Parquet format
        
        Args:
            topic: Kafka topic to read from
            sink_path: Output path for Parquet files
            
        Returns:
            Configured pipeline ready for execution
        """
        print("Called Build with topic =", topic, "sink_path =", sink_path)
        
        # Create streaming TableEnvironment (not used here, but shows the pattern)
        settings = EnvironmentSettings.in_streaming_mode()
        t_env = TableEnvironment.create(settings)
        print(f"t_env = {t_env}")
        
        # Load environment defaults
        from utils.env_loader import get_default_topic, get_default_sink_path
        
        # Use provided values or environment defaults
        topic = topic or get_default_topic()
        sink_path = sink_path or get_default_sink_path()
        
        # Import Flink-specific implementations
        from extractor.flink_kafka_extractor import FlinkKafkaJsonSource
        from transformer.flink_log_transform import FlinkLogPromoteTransform
        from sink.flink_parquet_sink import FlinkFilesystemParquetSink
        
        # Create ETL components with Flink Table API integration
        extractor = FlinkKafkaJsonSource(topic=topic)              # Kafka source table
        transformer = FlinkLogPromoteTransform()                   # SQL transformation view
        sink = FlinkFilesystemParquetSink(path=sink_path)         # Parquet sink table
        
        return cls(extractor, transformer, sink)

    def run(self) -> None:
        """Execute the Flink streaming pipeline
        
        FLINK EXECUTION FLOW:
        1. Create TableEnvironment for streaming mode
        2. Load JAR dependencies (Kafka, Parquet connectors)
        3. Configure checkpointing for fault tolerance
        4. Register source table (Kafka)
        5. Create debug view and print sink
        6. Apply transformations
        7. Register destination sink (Parquet)
        8. Execute both print and parquet sinks simultaneously
        9. Wait for streaming job to run continuously
        """
        # STEP 1: Create Flink TableEnvironment for streaming execution
        settings = EnvironmentSettings.in_streaming_mode()
        t_env = TableEnvironment.create(settings)
        
        # STEP 2: Load JAR dependencies for Kafka and Parquet connectors
        # Flink needs external JARs for connector functionality
        jars_dir = os.path.abspath("jars")
        if os.path.exists(jars_dir):
            # Only include non-empty JAR files
            jar_files = [f for f in os.listdir(jars_dir) 
                        if f.endswith(".jar") and os.path.getsize(os.path.join(jars_dir, f)) > 1000]
            jar_urls = ";".join([f"file://{os.path.join(jars_dir, jar)}" for jar in jar_files])
            if jar_urls:
                _set(t_env, "pipeline.jars", jar_urls)
                
        # STEP 3: Configure checkpointing for fault tolerance
        # Checkpoints enable exactly-once processing and recovery
        _set(t_env, "execution.checkpointing.interval", "10 s")

        # STEP 4: Register Kafka source table
        # Creates a Table API table connected to Kafka topic
        src_table = self.extractor.register_in_flink(t_env)
        
        # STEP 5: Create debug view for monitoring data flow
        # This shows what data is coming from Kafka in real-time
        debug_view = "kafka_debug_view"
        t_env.execute_sql(f"""
            CREATE TEMPORARY VIEW {debug_view} AS
            SELECT 
                `timestamp`,
                serviceName,
                severityText,
                CAST(attributes AS STRING) as attributes_str,
                CAST(resources AS STRING) as resources_str,
                body
            FROM {src_table}
        """)
        
        # STEP 6: Create print sink for debugging
        # Flink's print connector outputs data to console for monitoring
        t_env.execute_sql(f"""
            CREATE TEMPORARY TABLE print_sink (
                `timestamp` STRING,
                serviceName STRING,
                severityText STRING,
                attributes_str STRING,
                resources_str STRING,
                body STRING
            ) WITH (
                'connector' = 'print',
                'print-identifier' = 'KafkaData>'
            )
        """)
        
        # Create statement set to run both print and file sink together
        statement_set = t_env.create_statement_set()
        
        # Add print sink
        statement_set.add_insert_sql(f"INSERT INTO print_sink SELECT * FROM {debug_view}")
        
        # Continue with normal pipeline
        tmp = self.transformer.apply_in_flink(t_env, src_table) or src_table
        self.sink.register_sink_in_flink(t_env)
        
        # Add parquet sink
        statement_set.add_insert_sql(f"INSERT INTO `{self.sink.table_name}` SELECT * FROM `{tmp}`")
        
        # Execute both sinks together
        result = statement_set.execute()
        print(f"Pipeline submitted. Job ID: {result.get_job_client().get_job_id()}")
        print("📨 Data printed to console as it arrives")
        print("📁 Files written to parquet_data/")
        print("Press Ctrl+C to stop")
        
        # Let it run continuously
        result.wait()
        # Job now runs; stop via Ctrl+C or job cancel in Flink UI
