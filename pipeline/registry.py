from pipeline.flink_kafka_to_parquet import FlinkKafkaToParquetPipeline
from pipeline.kafka_to_parquet import KafkaToParquetPipeline
from pipeline.kafka_producer_pipeline import KafkaProducerPipeline

PIPELINE_REGISTRY = {
    "kafka_to_parquet": KafkaToParquetPipeline,
    "flink_kafka_to_parquet": FlinkKafkaToParquetPipeline,
    "kafka_producer": KafkaProducerPipeline
    # "sqs_to_parquet": SqsToOrcPipeline,
    # add more here
}