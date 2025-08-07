from sink.base_sink import AbstractSink


class FlinkFilesystemParquetSinkSimple(AbstractSink):
    def __init__(self, path: str, table_name: str = "logs_parquet_simple",
                 rolling_file_size="500B", rollover_interval="5 s"):
        self.path = path
        self.table_name = table_name
        self.rolling_file_size = rolling_file_size
        self.rollover_interval = rollover_interval

    # Not used in Flink path; satisfy ABC
    def write(self, records): return None

    def flush(self): return None

    def close(self): return None

    def register_sink_in_flink(self, t_env):
        # Create non-partitioned sink for faster testing
        t_env.execute_sql(f"""
            CREATE TEMPORARY TABLE `{self.table_name}` (
                ts            TIMESTAMP_LTZ(3),
                serviceName   STRING,
                severityText  STRING,
                msg           STRING,
                url           STRING,
                mobile        STRING,
                attributes    MAP<STRING, STRING>,
                resources     MAP<STRING, STRING>,
                body          STRING,
                dt            STRING,
                hr            STRING
            ) WITH (
                'connector' = 'filesystem',
                'path' = '{self.path}',
                'format' = 'parquet',
                'sink.rolling-policy.file-size' = '{self.rolling_file_size}',
                'sink.rolling-policy.rollover-interval' = '{self.rollover_interval}'
            )
        """)
        return self.table_name

    def insert_into_flink(self, t_env, from_table: str) -> None:
        t_env.execute_sql(f"INSERT INTO `{self.table_name}` SELECT * FROM `{from_table}`")