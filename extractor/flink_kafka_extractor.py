import os
from pyflink.table import TableEnvironment
from extractor.base_extractor import AbstractExtractor


class FlinkKafkaJsonSource(AbstractExtractor):
    def __init__(self, topic: str, startup_mode: str = "earliest-offset",
                 table_name: str = "last9Topic", watermark_delay_seconds: int = 5,
                 bootstrap=None, security_protocol=None, sasl_mechanism=None,
                 sasl_username=None, sasl_password=None):
        self.topic = topic
        self.startup_mode = startup_mode
        self.table_name = table_name
        self.watermark_delay_seconds = int(watermark_delay_seconds)
        self.bootstrap = bootstrap or os.environ.get("BOOTSTRAP_SERVERS", "pkc-921jm.us-east-2.aws.confluent.cloud:9092")
        self.security_protocol = security_protocol or os.environ.get("SECURITY_PROTOCOL", "SASL_SSL")
        self.sasl_mechanism = sasl_mechanism or os.environ.get("SASL_MECHANISMS", "PLAIN")
        self.sasl_username = sasl_username or os.environ.get("SASL_USERNAME", "P37ULBCFLIIPDCU2")
        self.sasl_password = sasl_password or os.environ.get("SASL_PASSWORD", "cfltOxgqyf8cCulWd67dwVlZelj+UrsAp+4niX/OvGWkBYf0BxDoLlwybQeqweOQ")

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
