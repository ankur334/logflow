import logging, signal
from utils.batching import BatchAccumulator
from pipeline.base_pipeline import AbstractPipeline
from extractor.kafka_extractor import KafkaExtractor
from transformer.log_enricher import LogPromoteTransformer
from sink.parquet_sink import ParquetSink
from config.config_reader import read_properties

log = logging.getLogger(__name__)


class KafkaToParquetPipeline(AbstractPipeline):
    def __init__(self, extractor, transformer, sink, batch_size: int = 1000):
        self.extractor = extractor
        self.transformer = transformer
        self.sink = sink
        self.batch = BatchAccumulator(batch_size, on_flush=self.sink.write)
        self._stopping = False

    @classmethod
    def build(cls, topic: str = "last9Topic", batch_size: int = 1000, **_):
        kafka_conf = read_properties("config/properties/confluent.properties")
        extractor = KafkaExtractor(kafka_conf, topic=topic, group_id="kafka-parquet", include_metadata=False)
        transformer = LogPromoteTransformer()
        sink = ParquetSink(base_path="parquet_data", compression="zstd")
        return cls(extractor, transformer, sink, batch_size=batch_size)

    def _graceful_stop(self, *_):
        if not self._stopping:
            log.info("Stopping pipeline …")
            self._stopping = True
            try:
                self.extractor.stop()
            except Exception:
                pass

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._graceful_stop)
        signal.signal(signal.SIGTERM, self._graceful_stop)
        try:
            for rec in self.extractor.extract():
                if self._stopping: break
                out = self.transformer.transform(rec)
                if out is not None:
                    self.batch.add(out)
        finally:
            self.batch.flush()
            try:
                self.sink.close()
            except Exception:
                pass
