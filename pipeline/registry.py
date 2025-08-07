from pipeline.flink_kafka_to_parquet import FlinkKafkaToParquetPipeline
from pipeline.flink_optimized_kafka_to_parquet import FlinkOptimizedKafkaToParquetPipeline
from pipeline.kafka_to_parquet import KafkaToParquetPipeline
from pipeline.kafka_producer_pipeline import KafkaProducerPipeline
from pipeline.sample_data_generator_pipeline import SampleDataGeneratorPipeline

PIPELINE_REGISTRY = {
    "kafka_to_parquet": KafkaToParquetPipeline,
    "flink_kafka_to_parquet": FlinkKafkaToParquetPipeline,
    "flink_optimized_kafka_to_parquet": FlinkOptimizedKafkaToParquetPipeline,
    "kafka_producer": KafkaProducerPipeline,
    "sample_data_generator": SampleDataGeneratorPipeline
    # "sqs_to_parquet": SqsToOrcPipeline,
    # add more here
}