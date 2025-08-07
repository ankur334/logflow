import logging
from sink.base_sink import AbstractSink

log = logging.getLogger(__name__)


class FlinkFilesystemParquetSink(AbstractSink):
    def __init__(self, path: str, table_name: str = "logs_parquet",
                 rolling_file_size="128MB", rollover_interval="5min",
                 rolling_check_interval="1min", partition_commit_delay="1min",
                 enable_compaction=True):
        """Initialize Flink Parquet sink with production-ready settings
        
        Args:
            path: Output path for Parquet files
            table_name: Name for the Flink sink table
            rolling_file_size: File size threshold for rolling (production: 128MB)
            rollover_interval: Time threshold for rolling (production: 5min)
            rolling_check_interval: How often to check for rolling (production: 1min)
            partition_commit_delay: Delay before committing partitions (production: 1min)
            enable_compaction: Whether to enable automatic compaction
        """
        self.path = path
        self.table_name = table_name
        self.rolling_file_size = rolling_file_size
        self.rollover_interval = rollover_interval
        self.rolling_check_interval = rolling_check_interval
        self.partition_commit_delay = partition_commit_delay
        self.enable_compaction = enable_compaction
        
        log.info(f"Configured Parquet sink:")
        log.info(f"  Path: {self.path}")
        log.info(f"  Rolling file size: {self.rolling_file_size}")
        log.info(f"  Rollover interval: {self.rollover_interval}")
        log.info(f"  Compaction enabled: {self.enable_compaction}")

    # Not used in Flink path; satisfy ABC
    def write(self, records): return None

    def flush(self): return None

    def close(self): return None

    def register_sink_in_flink(self, t_env):
        """Register Parquet sink table in Flink with production configuration"""
        log.info(f"Registering Parquet sink table: {self.table_name}")
        log.info(f"  Output path: {self.path}")
        
        try:
            # Build WITH clause based on configuration
            compaction_settings = ""
            if self.enable_compaction:
                compaction_settings = f"""
                    'auto-compaction' = 'true',
                    'compaction.file-size' = '256MB',"""
            
            ddl_sql = f"""
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
                    'sink.rolling-policy.check-interval' = '{self.rolling_check_interval}',{compaction_settings}
                    'sink.partition-commit.delay' = '{self.partition_commit_delay}',
                    'sink.partition-commit.policy.kind' = 'success-file',
                    'parquet.compression' = 'SNAPPY',
                    'parquet.block.size' = '134217728',
                    'parquet.page.size' = '1048576'
                )
            """
            
            log.debug(f"Executing Parquet sink DDL: {ddl_sql}")
            t_env.execute_sql(ddl_sql)
            
            log.info(f"✅ Parquet sink table registered successfully: {self.table_name}")
            return self.table_name
            
        except Exception as e:
            log.error(f"❌ Failed to register Parquet sink table: {e}")
            raise ValueError(f"Parquet sink registration failed: {e}")

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
