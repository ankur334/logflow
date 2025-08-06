from sink.base_sink import AbstractSink


class FlinkFilesystemParquetSink(AbstractSink):
    def __init__(self, path: str, table_name: str = "logs_parquet",
                 rolling_file_size="512MB", rollover_interval="5 min",
                 rolling_check_interval="1 min", partition_commit_delay="1 min"):
        self.path = path
        self.table_name = table_name
        self.rolling_file_size = rolling_file_size
        self.rollover_interval = rollover_interval
        self.rolling_check_interval = rolling_check_interval
        self.partition_commit_delay = partition_commit_delay

    # Not used in Flink path; satisfy ABC
    def write(self, records): return None

    def flush(self): return None

    def close(self): return None

    def register_sink_in_flink(self, t_env):
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
            )
            PARTITIONED BY (dt, hr)
            WITH (
                'connector' = 'filesystem',
                'path' = '{self.path}',
                'format' = 'parquet',
                'sink.rolling-policy.file-size' = '{self.rolling_file_size}',
                'sink.rolling-policy.rollover-interval' = '{self.rollover_interval}',
                'sink.rolling-policy.check-interval' = '{self.rolling_check_interval}',
                'sink.partition-commit.trigger' = 'process-time',
                'sink.partition-commit.delay'   = '{self.partition_commit_delay}',
                'sink.partition-commit.policy.kind' = 'success-file'
            )
        """)
        return self.table_name

    def insert_into_flink(self, t_env, from_table: str) -> None:
        t_env.execute_sql(f"INSERT INTO `{self.table_name}` SELECT * FROM `{from_table}`")
