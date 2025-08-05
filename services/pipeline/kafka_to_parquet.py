from services.extractor.kafka_extractor import KafkaExtractor
from services.transformer.log_enricher import LogEnricher
from services.sink.parquet_sink import ParquetSink
from services.pipeline.base_pipeline import AbstractPipeline


class KafkaToParquetPipeline(AbstractPipeline):
    def __init__(self, extractor, transformer, sink):
        self.extractor = extractor
        self.transformer = transformer
        self.sink = sink

    def run(self):
        records = []
        for record in self.extractor.extract():
            transformed = self.transformer.transform(record)
            records.append(transformed)
            if len(records) >= 1000:
                self.sink.write(records)
                records.clear()
        if records:
            self.sink.write(records)