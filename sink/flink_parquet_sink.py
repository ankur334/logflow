from sink.base_sink import AbstractSink


class FlinkFilesystemParquetSink(AbstractSink):
    def __init__(self, path: str, table_name: str = "logs_parquet",
                 rolling_file_size="1KB", rollover_interval="10 s",
                 rolling_check_interval="5 s", partition_commit_delay="5 s"):
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
                `timestamp`   STRING,
                serviceName   STRING,
                severityText  STRING,
                msg           STRING,
                url           STRING,
                mobile        STRING,
                attributes    MAP<STRING, STRING>,
                resources     MAP<STRING, STRING>,
                body          STRING
            ) WITH (
                'connector' = 'filesystem',
                'path' = '{self.path}',
                'format' = 'parquet',
                'sink.rolling-policy.file-size' = '{self.rolling_file_size}',
                'sink.rolling-policy.rollover-interval' = '{self.rollover_interval}',
                'sink.rolling-policy.check-interval' = '{self.rolling_check_interval}',
                'auto-compaction' = 'true',
                'compaction.file-size' = '128MB'
            )
        """)
        return self.table_name

    def insert_into_flink(self, t_env, from_table: str):
        """Execute INSERT to write data to Parquet files
        
        Args:
            t_env: Flink TableEnvironment
            from_table: Source table/view name to read from
            
        Returns:
            Flink execution result
        """
        print(f"📤 Executing INSERT from {from_table} to {self.table_name}")
        
        # INSERT statement for basic schema (hot keys promoted)
        insert_sql = f"""
            INSERT INTO `{self.table_name}`
            SELECT 
                `timestamp`,
                serviceName,
                severityText,
                msg,
                url,
                mobile,
                attributes,
                resources,
                body
            FROM `{from_table}`
        """
        
        print("🚀 Starting Parquet writing job...")
        result = t_env.execute_sql(insert_sql)
        
        print(f"✅ Parquet writing job submitted")
        print(f"   📊 Hot keys: msg, url, mobile promoted to columns")
        
        return result
    
    def get_insert_sql(self, from_table: str) -> str:
        """Get INSERT SQL for use in statement sets
        
        Args:
            from_table: Source table/view name
            
        Returns:
            INSERT SQL string
        """
        return f"""
            INSERT INTO `{self.table_name}`
            SELECT 
                `timestamp`,
                serviceName,
                severityText,
                msg,
                url,
                mobile,
                attributes,
                resources,
                body
            FROM `{from_table}`
        """
