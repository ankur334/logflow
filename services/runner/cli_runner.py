from services.pipeline.kafka_to_parquet import KafkaToParquetPipeline
from services.extractor.kafka_extractor import KafkaExtractor
from services.transformer.log_enricher import LogEnricher
from services.sink.parquet_sink import ParquetSink


def run_pipeline():
    extractor = KafkaExtractor(config={})
    transformer = LogEnricher()
    sink = ParquetSink(output_dir="output/")
    pipeline = KafkaToParquetPipeline(extractor, transformer, sink)
    pipeline.run()
