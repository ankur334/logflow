from pyflink.table import TableEnvironment
from extractor.base_extractor import AbstractExtractor
from utils.env_loader import get_kafka_config


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
        jaas = (
            f'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{self.sasl_username}" password="{self.sasl_password}";'
        )
        t_env.execute_sql(f"""
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
                'json.timestamp-format.standard' = 'ISO-8601'
            )
        """)
        return self.table_name
