"""Flink Kafka to Parquet Pipeline - Production streaming ETL implementation

This pipeline demonstrates core Flink concepts:
- Table API for high-level stream processing
- Streaming execution with checkpointing
- Multiple sinks (print for debugging + parquet for storage)
- JAR dependency management for Kafka/Parquet connectors
- Production-ready error handling and monitoring
"""
import os
import logging
from pyflink.table import EnvironmentSettings, TableEnvironment
from pipeline.base_pipeline import AbstractPipeline

log = logging.getLogger(__name__)


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
    def build(cls, topic=None, sink_path=None, checkpoint_interval="30 s", 
              validate_config=True, **_):
        """Factory method to create production-ready Flink pipeline
        
        FLINK CONCEPT: Component composition
        Creates specialized Flink implementations of abstract ETL components:
        - FlinkKafkaJsonSource: Kafka Table API source connector
        - FlinkLogPromoteTransform: SQL transformations for nested field extraction
        - FlinkFilesystemParquetSink: Filesystem connector with Parquet format
        
        Args:
            topic: Kafka topic to read from
            sink_path: Output path for Parquet files
            checkpoint_interval: Flink checkpointing frequency
            validate_config: Whether to validate Kafka configuration
            
        Returns:
            Configured pipeline ready for execution
        """
        log.info(f"Building Flink Kafka to Parquet pipeline")
        log.info(f"  Topic: {topic}")
        log.info(f"  Sink path: {sink_path}")
        log.info(f"  Checkpoint interval: {checkpoint_interval}")
        
        # Validate environment and configuration
        if validate_config:
            cls._validate_environment()
        
        # Load environment defaults - RAW LAYER for basic pipeline
        from utils.env_loader import get_default_topic, get_raw_layer_path
        
        # Use provided values or environment defaults (RAW layer for unprocessed data)
        topic = topic or get_default_topic()
        sink_path = sink_path or get_raw_layer_path()
        
        log.info(f"Final configuration:")
        log.info(f"  Topic: {topic}")
        log.info(f"  Sink path: {sink_path}")
        
        # Import Flink-specific implementations
        from extractor.flink_kafka_extractor import FlinkKafkaJsonSource
        from transformer.flink_log_transform import FlinkLogPromoteTransform
        from sink.flink_parquet_sink import FlinkFilesystemParquetSink
        
        # Create ETL components with Flink Table API integration
        extractor = FlinkKafkaJsonSource(topic=topic)              # Kafka source table
        transformer = FlinkLogPromoteTransform()                   # SQL transformation view
        sink = FlinkFilesystemParquetSink(path=sink_path)         # Parquet sink table
        
        log.info("Pipeline components created successfully")
        return cls(extractor, transformer, sink, checkpoint_interval=checkpoint_interval)
    
    @staticmethod
    def _validate_environment():
        """Validate that the environment is properly set up for Flink execution"""
        log.info("Validating Flink environment...")
        
        # Check JAR dependencies
        jars_dir = os.path.abspath("jars")
        if not os.path.exists(jars_dir):
            raise ValueError(f"JARs directory not found: {jars_dir}")
        
        required_jars = ["flink-sql-connector-kafka", "flink-sql-parquet"]
        available_jars = os.listdir(jars_dir)
        
        for required_jar in required_jars:
            if not any(required_jar in jar for jar in available_jars):
                raise ValueError(f"Required JAR not found: {required_jar}")
        
        log.info(f"Found {len(available_jars)} JAR files in {jars_dir}")
        
        # Validate Kafka configuration
        try:
            from utils.env_loader import get_kafka_config
            kafka_config = get_kafka_config()
            
            required_keys = ['bootstrap_servers', 'sasl_username', 'sasl_password']
            for key in required_keys:
                if not kafka_config.get(key):
                    raise ValueError(f"Missing required Kafka configuration: {key}")
                    
            log.info("Kafka configuration validation passed")
        except Exception as e:
            log.warning(f"Kafka configuration validation failed: {e}")
            raise ValueError(f"Kafka configuration error: {e}")
        
        log.info("Environment validation completed successfully")

    def run(self) -> None:
        """Execute the production-ready Flink streaming pipeline
        
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
        log.info("🚀 Starting Flink Kafka to Parquet pipeline execution")
        
        try:
            # STEP 1: Create Flink TableEnvironment for streaming execution
            log.info("Creating Flink streaming TableEnvironment")
            settings = EnvironmentSettings.in_streaming_mode()
            t_env = TableEnvironment.create(settings)
            
            # STEP 2: Load JAR dependencies for Kafka and Parquet connectors
            self._configure_jars(t_env)
            
            # STEP 3: Configure checkpointing and other Flink settings
            self._configure_flink_settings(t_env)
            
            # STEP 4: Register Kafka source table
            log.info("Registering Kafka source table")
            src_table = self.extractor.register_in_flink(t_env)
            log.info(f"Kafka source registered as table: {src_table}")
            
            # STEP 5: Apply transformations
            log.info("Applying data transformations")
            transformed_table = self.transformer.apply_in_flink(t_env, src_table) or src_table
            log.info(f"Transformation view created: {transformed_table}")
            
            # STEP 6: Register sinks
            log.info("Registering destination sinks")
            self._setup_debug_monitoring(t_env, src_table)
            sink_table = self.sink.register_sink_in_flink(t_env)
            log.info(f"Parquet sink registered as table: {sink_table}")
            
            # STEP 7: Execute pipeline
            self._execute_pipeline(t_env, transformed_table)
            
        except Exception as e:
            log.error(f"❌ Flink pipeline execution failed: {e}")
            raise
        finally:
            log.info("Flink pipeline execution finished")
    
    def _configure_jars(self, t_env):
        """Configure JAR dependencies for Flink execution"""
        jars_dir = os.path.abspath("jars")
        
        if not os.path.exists(jars_dir):
            log.warning(f"JARs directory not found: {jars_dir}")
            return
            
        # Only include non-empty JAR files
        jar_files = [f for f in os.listdir(jars_dir) 
                    if f.endswith(".jar") and os.path.getsize(os.path.join(jars_dir, f)) > 1000]
        
        if not jar_files:
            log.warning("No valid JAR files found")
            return
            
        jar_urls = ";".join([f"file://{os.path.join(jars_dir, jar)}" for jar in jar_files])
        _set(t_env, "pipeline.jars", jar_urls)
        log.info(f"Loaded {len(jar_files)} JAR dependencies")
        
    def _configure_flink_settings(self, t_env):
        """Configure Flink execution settings for production"""
        log.info(f"Configuring Flink settings (checkpoint interval: {self.checkpoint_interval})")
        
        # Checkpointing for fault tolerance
        _set(t_env, "execution.checkpointing.interval", self.checkpoint_interval)
        
        # Production-ready settings
        _set(t_env, "execution.checkpointing.mode", "EXACTLY_ONCE")
        _set(t_env, "execution.checkpointing.timeout", "10min")
        _set(t_env, "execution.checkpointing.max-concurrent-checkpoints", "1")
        _set(t_env, "execution.checkpointing.min-pause", "1s")
        
        # Restart strategy
        _set(t_env, "restart-strategy", "fixed-delay")
        _set(t_env, "restart-strategy.fixed-delay.attempts", "3")
        _set(t_env, "restart-strategy.fixed-delay.delay", "30s")
        
        # Performance tuning
        _set(t_env, "table.exec.mini-batch.enabled", "true")
        _set(t_env, "table.exec.mini-batch.allow-latency", "5s")
        _set(t_env, "table.exec.mini-batch.size", "5000")
        
        log.info("Flink settings configured for production")
        
    def _setup_debug_monitoring(self, t_env, src_table):
        """Setup debug monitoring and print sink"""
        log.info("Setting up debug monitoring")
        
        # Create debug view for monitoring data flow
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
        
        # Create print sink for debugging
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
        
        log.info("Debug monitoring configured")
        
    def _execute_pipeline(self, t_env, transformed_table):
        """Execute the streaming pipeline with both debug and production sinks"""
        log.info("Creating statement set for multi-sink execution")
        
        # Create statement set to run both print and file sink together
        statement_set = t_env.create_statement_set()
        
        # Add print sink for debugging
        statement_set.add_insert_sql("INSERT INTO print_sink SELECT * FROM kafka_debug_view")
        
        # Add parquet sink
        parquet_insert_sql = self.sink.get_insert_sql(transformed_table)
        statement_set.add_insert_sql(parquet_insert_sql)
        
        # Execute both sinks together
        log.info("Submitting Flink streaming job")
        result = statement_set.execute()
        
        job_id = result.get_job_client().get_job_id()
        log.info(f"✅ Pipeline submitted successfully. Job ID: {job_id}")
        
        print("=" * 60)
        print("🎉 FLINK STREAMING PIPELINE RUNNING")
        print(f"📊 Job ID: {job_id}")
        print("📨 Data printed to console as it arrives")
        print(f"📁 Parquet files written to: {self.sink.path}")
        print(f"⏱️  Checkpoint interval: {self.checkpoint_interval}")
        print("=" * 60)
        print("Press Ctrl+C to stop")
        
        # Wait for the job to run continuously
        try:
            result.wait()
        except KeyboardInterrupt:
            log.info("Received shutdown signal, stopping pipeline")
            print("\n🛑 Shutting down pipeline gracefully...")
        except Exception as e:
            log.error(f"Pipeline execution error: {e}")
            raise
