from pipeline.kafka_to_parquet import KafkaToParquetPipeline

PIPELINE_REGISTRY = {
    "kafka_to_parquet": KafkaToParquetPipeline,
    # "sqs_to_parquet": SqsToOrcPipeline,
    # add more here
}