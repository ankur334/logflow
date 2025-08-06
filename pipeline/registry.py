from pipeline.flink_kafka_to_parquet import FlinkKafkaToParquetPipeline
from pipeline.kafka_to_parquet import KafkaToParquetPipeline

PIPELINE_REGISTRY = {
    "kafka_to_parquet": KafkaToParquetPipeline,
    "flink_kafka_to_parquet": FlinkKafkaToParquetPipeline
    # "sqs_to_parquet": SqsToOrcPipeline,
    # add more here
}