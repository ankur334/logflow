import logging, signal
from utils.batching import BatchAccumulator
from pipeline.base_pipeline import AbstractPipeline
from extractor.kafka_extractor import KafkaExtractor
from transformer.log_enricher import LogPromoteTransformer
from sink.parquet_sink import ParquetSink
from config.config_reader import read_properties, validate_kafka_config

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
        # Load and validate Kafka configuration
        log.info("Loading Kafka configuration for kafka_to_parquet pipeline")
        
        try:
            # Try to load with strict validation first (production mode)
            kafka_conf = read_properties("config/properties/confluent.properties", strict=True)
            validate_kafka_config(kafka_conf)
        except (ValueError, FileNotFoundError) as e:
            log.warning(f"Strict configuration loading failed: {e}")
            log.info("Falling back to env_loader for configuration")
            
            # Fallback to env_loader approach for development
            from utils.env_loader import get_kafka_config
            kafka_conf = get_kafka_config()
            if not kafka_conf:
                raise ValueError("No valid Kafka configuration found. Check environment variables or config files.")
        
        # Convert batch_size to int if it's a string (from CLI)
        if isinstance(batch_size, str):
            batch_size = int(batch_size)
            
        log.info(f"Building kafka_to_parquet pipeline: topic={topic}, batch_size={batch_size}")
        
        extractor = KafkaExtractor(kafka_conf, topic=topic, group_id="kafka-parquet", include_metadata=False)
        transformer = LogPromoteTransformer(validate_timestamp=True)  # Enable validation
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
        """
        Execute the kafka_to_parquet pipeline with proper error handling.
        
        Processes messages from Kafka, transforms them, and writes to Parquet files
        with graceful shutdown handling.
        """
        signal.signal(signal.SIGINT, self._graceful_stop)
        signal.signal(signal.SIGTERM, self._graceful_stop)
        
        log.info("Starting kafka_to_parquet pipeline execution")
        processed_count = 0
        error_count = 0
        
        try:
            for rec in self.extractor.extract():
                if self._stopping: 
                    log.info("Graceful shutdown requested, stopping message processing")
                    break
                    
                try:
                    out = self.transformer.transform(rec)
                    if out is not None:
                        self.batch.add(out)
                        processed_count += 1
                    else:
                        # Record was filtered out by transformer validation
                        pass
                        
                except Exception as e:
                    error_count += 1
                    log.error(f"Error processing record {processed_count + 1}: {e}")
                    
                    # Log first few errors in detail for debugging
                    if error_count <= 3:
                        log.error(f"Problematic record: {rec}")
                    
                    # Stop processing if too many errors
                    if error_count > 100:
                        log.error("Too many processing errors, stopping pipeline")
                        break
                        
                # Log progress periodically
                if processed_count % 10000 == 0 and processed_count > 0:
                    log.info(f"Processed {processed_count} records, {error_count} errors")
                    
        except Exception as e:
            log.error(f"Fatal error in pipeline execution: {e}")
            raise
        finally:
            log.info("Flushing remaining batches and cleaning up")
            try:
                self.batch.flush()
            except Exception as e:
                log.error(f"Error during batch flush: {e}")
                
            try:
                self.sink.close()
            except Exception as e:
                log.error(f"Error during sink cleanup: {e}")
                
            # Log final statistics
            stats = getattr(self.transformer, 'get_stats', lambda: {})()
            log.info(f"Pipeline execution completed:")
            log.info(f"  - Messages processed: {processed_count}")
            log.info(f"  - Processing errors: {error_count}")
            if stats:
                log.info(f"  - Transformer stats: {stats}")
                
            log.info("kafka_to_parquet pipeline stopped")
