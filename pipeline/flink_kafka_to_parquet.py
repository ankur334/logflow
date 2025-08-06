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
        _set(t_env, "execution.checkpointing.interval", self.checkpoint_interval)

        # Use the **same** Extractor/Transformer/Sink instances via their Flink hooks
        src_table = self.extractor.register_in_flink(t_env)
        tmp = self.transformer.apply_in_flink(t_env, src_table) or src_table
        self.sink.register_sink_in_flink(t_env)
        self.sink.insert_into_flink(t_env, tmp)
        # Job now runs; stop via Ctrl+C or job cancel in Flink UI
