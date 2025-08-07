import logging
from pyflink.table import TableEnvironment
from extractor.base_extractor import AbstractExtractor
from utils.env_loader import get_kafka_config

log = logging.getLogger(__name__)


class FlinkKafkaJsonSource(AbstractExtractor):
    """Flink Kafka source for streaming JSON data
    
    Creates a Kafka source table in Flink's Table API with SASL authentication.
    Credentials are loaded from environment variables via .env.dev file.
    """
    
    def __init__(self, topic: str, startup_mode: str = "earliest-offset",
                 table_name: str = "last9Topic", watermark_delay_seconds: int = 5,
                 bootstrap=None, security_protocol=None, sasl_mechanism=None,
                 sasl_username=None, sasl_password=None):
        """Initialize Kafka source with configuration
        
        Args:
            topic: Kafka topic to read from
            startup_mode: How to start reading ('earliest-offset' or 'latest-offset')
            table_name: Name for the Flink table
            bootstrap: Kafka bootstrap servers (uses env var if not provided)
            sasl_username: SASL username (uses env var if not provided)  
            sasl_password: SASL password (uses env var if not provided)
        """
        self.topic = topic
        self.startup_mode = startup_mode
        self.table_name = table_name
        self.watermark_delay_seconds = int(watermark_delay_seconds)
        
        # Load Kafka configuration from environment
        kafka_config = get_kafka_config()
        
        # Use provided values or fall back to environment configuration
        self.bootstrap = bootstrap or kafka_config['bootstrap_servers']
        self.security_protocol = security_protocol or "SASL_SSL"
        self.sasl_mechanism = sasl_mechanism or "PLAIN"
        self.sasl_username = sasl_username or kafka_config['sasl_username']
        self.sasl_password = sasl_password or kafka_config['sasl_password']

    # Not used in Flink path; satisfy ABC
    def extract(self):
        return iter(())  # empty iterator

    def register_in_flink(self, t_env: TableEnvironment) -> str:
        """Register Kafka source table in Flink with production-ready configuration"""
        log.info(f"Registering Kafka source table: {self.table_name}")
        log.info(f"  Topic: {self.topic}")
        log.info(f"  Bootstrap servers: {self.bootstrap}")
        log.info(f"  Startup mode: {self.startup_mode}")
        
        try:
            # Validate required configuration
            if not self.bootstrap:
                raise ValueError("Bootstrap servers not configured")
            if not self.sasl_username or not self.sasl_password:
                raise ValueError("SASL credentials not configured")
                
            # Create JAAS configuration for authentication
            jaas = (
                f'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
                f'username="{self.sasl_username}" password="{self.sasl_password}";'
            )
            
            # Create Kafka source table with production settings
            ddl_sql = f"""
                CREATE TEMPORARY TABLE `{self.table_name}` (
                    `timestamp`   STRING,
                    serviceName   STRING,
                    severityText  STRING,
                    attributes    MAP<STRING, STRING>,
                    resources     MAP<STRING, STRING>,
                    body          STRING
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = '{self.topic}',
                    'properties.bootstrap.servers' = '{self.bootstrap}',
                    'properties.security.protocol' = '{self.security_protocol}',
                    'properties.sasl.mechanism' = '{self.sasl_mechanism}',
                    'properties.sasl.jaas.config' = '{jaas}',
                    'scan.startup.mode' = '{self.startup_mode}',
                    'format' = 'json',
                    'json.ignore-parse-errors' = 'true',
                    'json.timestamp-format.standard' = 'ISO-8601',
                    'properties.group.id' = 'flink-kafka-to-parquet',
                    'properties.auto.offset.reset' = 'earliest',
                    'properties.enable.auto.commit' = 'false',
                    'properties.max.poll.records' = '1000',
                    'properties.session.timeout.ms' = '45000',
                    'properties.heartbeat.interval.ms' = '3000'
                )
            """
            
            log.debug(f"Executing Kafka source DDL: {ddl_sql}")
            t_env.execute_sql(ddl_sql)
            
            log.info(f"✅ Kafka source table registered successfully: {self.table_name}")
            return self.table_name
            
        except Exception as e:
            log.error(f"❌ Failed to register Kafka source table: {e}")
            raise ValueError(f"Kafka source registration failed: {e}")
