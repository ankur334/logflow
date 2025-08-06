import os
from pyflink.table import EnvironmentSettings, TableEnvironment
from pipeline.base_pipeline import AbstractPipeline


def _set(t_env, k, v): t_env.get_config().get_configuration().set_string(k, v)


class FlinkKafkaToParquetPipeline(AbstractPipeline):
    def __init__(self, extractor, transformer, sink,
                 checkpoint_interval: str = "30 s", pipeline_jars: str | None = None):
        self.extractor = extractor
        self.transformer = transformer
        self.sink = sink
        self.checkpoint_interval = checkpoint_interval
        self.pipeline_jars = pipeline_jars or os.environ.get("FLINK_PIPELINE_JARS")

    @classmethod
    def build(cls, topic="last9Topic", sink_path="file:///tmp/last9_parquet", **_):
        print("Called Build with topic =", topic, "sink_path =", sink_path)
        settings = EnvironmentSettings.in_streaming_mode()
        t_env = TableEnvironment.create(settings)
        print(f"t_env = {t_env}")
        # cfg = t_env.get_config().get_configuration()
        # print("[DEBUG] pipeline.jars =", cfg.set_string("pipeline.jars", ""))
        # Compose Flink-aware implementations that still satisfy your ABCs
        from extractor.flink_kafka_extractor import FlinkKafkaJsonSource
        from transformer.flink_log_transform import FlinkLogPromoteTransform
        from sink.flink_parquet_sink import FlinkFilesystemParquetSink
        extractor = FlinkKafkaJsonSource(topic=topic)
        transformer = FlinkLogPromoteTransform()
        sink = FlinkFilesystemParquetSink(path=sink_path)
        return cls(extractor, transformer, sink)

    def run(self) -> None:
        settings = EnvironmentSettings.in_streaming_mode()
        t_env = TableEnvironment.create(settings)
        # cfg = t_env.get_config().get_configuration()
        # print("[DEBUG] pipeline.jars =", cfg.get_string("pipeline.jars", ""))
        # Add JAR dependencies
        jars_dir = os.path.abspath("jars")
        if os.path.exists(jars_dir):
            jar_files = [f for f in os.listdir(jars_dir) if f.endswith(".jar") and os.path.getsize(os.path.join(jars_dir, f)) > 1000]
            jar_urls = ";".join([f"file://{os.path.join(jars_dir, jar)}" for jar in jar_files])
            if jar_urls:
                _set(t_env, "pipeline.jars", jar_urls)
        # Simple checkpointing for file writes
        _set(t_env, "execution.checkpointing.interval", "10 s")

        # Use the **same** Extractor/Transformer/Sink instances via their Flink hooks
        src_table = self.extractor.register_in_flink(t_env)
        
        # Create a view to print data for debugging
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
        
        # Add print sink to see the data
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
