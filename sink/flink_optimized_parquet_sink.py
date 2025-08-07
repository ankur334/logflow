"""Optimized Parquet Sink for Last9 Query Performance

This sink implements schema and partitioning optimizations:
1. Date/Hour partitioning for fast time range queries
2. Optimized column ordering (filters first, then data)
3. Proper data types for performance
4. File size optimization for query engines
"""
from sink.base_sink import AbstractSink


class FlinkOptimizedParquetSink(AbstractSink):
    """Optimized Parquet sink for Last9 query patterns
    
    Key optimizations:
    - Date partitioning: log_date=YYYY-MM-DD for fast time filtering
    - Hour sub-partitioning: log_hour=HH for smaller file sizes
    - Column ordering: Filter columns first for better compression
    - File sizing: Optimized for query engine performance
    """
    
    def __init__(self, path: str, table_name: str = "optimized_parquet_sink"):
        """Initialize optimized Parquet sink
        
        Args:
            path: Base output path for Parquet files
            table_name: Name for the Flink sink table
        """
        self.path = path
        self.table_name = table_name
        
    def write_batch(self, records):
        """Not used in Flink streaming path"""
        pass
    
    def write(self, record):
        """Not used in Flink streaming path"""
        pass
        
    def flush(self):
        """Not used in Flink streaming path"""
        pass
        
    def close(self):
        """Not used in Flink streaming path"""
        pass
        
    def register_sink_in_flink(self, t_env):
        """Register optimized Parquet sink with partitioning
        
        Schema optimization:
        - Filter columns (timestamp, service, severity) first
        - Hot keys (msg, url, mobile) as separate columns  
        - Data quality flags for fast filtering
        - Time buckets for aggregation
        - Original data last for fallback queries
        
        Partitioning strategy:
        - Primary: log_date (YYYY-MM-DD) for date range queries
        - Secondary: log_hour (0-23) to control file sizes
        
        File optimization:
        - Target 128MB files for optimal query performance
        - 10-second rolling for streaming freshness
        """
        t_env.execute_sql(f"""
            CREATE TEMPORARY TABLE `{self.table_name}` (
                -- TIMESTAMP COLUMNS (for partitioning and filtering)
                `timestamp` STRING,
                log_date STRING,                                          -- Partition column: YYYY-MM-DD
                log_hour BIGINT,                                          -- Partition column: 0-23
                
                -- PRIMARY FILTER COLUMNS (most selective first)
                serviceName STRING,                                       -- High selectivity filter
                severityText STRING,                                      -- Medium selectivity filter
                
                -- HOT KEY COLUMNS (promoted for fast access)
                msg STRING,                                               -- attributes['msg'] - frequently filtered
                url STRING,                                               -- attributes['url'] - regex patterns
                mobile STRING,                                            -- mobile extraction - business critical
                
                -- DATA QUALITY FLAGS (boolean-like for fast filtering) 
                is_valid_json BIGINT,                                     -- 0/1: JSON validation flag
                has_data_mobile BIGINT,                                   -- 0/1: data.mobile existence flag
                is_getotp_url BIGINT,                                     -- 0/1: URL pattern flag
                
                -- TIME BUCKET COLUMNS (for aggregation queries)
                time_bucket_10min BIGINT,                                 -- 10-minute intervals
                time_bucket_1hr BIGINT,                                   -- 1-hour intervals
                
                -- ORIGINAL DATA (for comprehensive queries)
                attributes MAP<STRING, STRING>,                           -- Full attributes map
                resources MAP<STRING, STRING>,                            -- Full resources map
                body STRING                                               -- Complete body for complex parsing
                
            ) PARTITIONED BY (log_date, log_hour) WITH (
                'connector' = 'filesystem',
                'path' = '{self.path}',
                'format' = 'parquet',
                
                -- PARTITIONING CONFIGURATION
                'partition.time-extractor.timestamp-pattern' = '$log_date $log_hour:00:00',
                'sink.partition-commit.delay' = '10 s',                   -- Fast commit for streaming
                'sink.partition-commit.trigger' = 'process-time',
                'sink.partition-commit.policy.kind' = 'success-file',
                
                -- FILE OPTIMIZATION
                'sink.rolling-policy.file-size' = '128MB',                -- Optimal for query engines
                'sink.rolling-policy.rollover-interval' = '10 s',        -- Balance freshness vs file count
                'sink.rolling-policy.check-interval' = '5 s',            -- Check interval for rolling
                
                -- COMPRESSION OPTIMIZATION
                'parquet.compression' = 'SNAPPY',                         -- Fast decompression for queries
                'parquet.block.size' = '134217728',                       -- 128MB blocks
                'parquet.page.size' = '1048576',                          -- 1MB pages
                
                -- WRITE OPTIMIZATION
                'sink.parallelism' = '4'                                  -- Parallel writes for throughput
            )
        """)
        
        print(f"✅ Optimized Parquet sink registered: {self.table_name}")
        print(f"   📁 Path: {self.path}")
        print(f"   🗓️  Partitioning: log_date/log_hour")  
        print(f"   📊 Schema: Hot keys promoted, quality flags added")
        print(f"   ⚡ File size: 128MB target for query performance")
        
    def insert_into_flink(self, t_env, from_table: str):
        """Execute INSERT to write optimized data to Parquet files
        
        This method encapsulates the specific INSERT logic for the optimized sink,
        including all performance columns and proper column ordering.
        
        Args:
            t_env: Flink TableEnvironment
            from_table: Source table/view name to read from
            
        Returns:
            Flink execution result
        """
        print(f"📤 Executing optimized INSERT from {from_table} to {self.table_name}")
        
        # INSERT statement for optimized schema with all performance columns
        insert_sql = f"""
            INSERT INTO `{self.table_name}`
            SELECT 
                `timestamp`,
                log_date,
                log_hour,
                serviceName,
                severityText,
                msg,
                url,
                mobile,
                is_valid_json,
                has_data_mobile,
                is_getotp_url,
                time_bucket_10min,
                time_bucket_1hr,
                attributes,
                resources,
                body
            FROM `{from_table}`
        """
        
        print("🚀 Starting optimized streaming job with performance columns...")
        result = t_env.execute_sql(insert_sql)
        
        print(f"✅ Optimized Parquet writing job submitted")
        print(f"   📊 Hot keys: msg, url, mobile promoted to columns")
        print(f"   🏷️  Quality flags: is_valid_json, has_data_mobile, is_getotp_url")
        print(f"   📅 Partitioning: log_date/log_hour for fast time queries")
        print(f"   ⏱️  Time buckets: 10min/1hr intervals for aggregation")
        
        return result
        
    def get_insert_sql(self, from_table: str) -> str:
        """Get INSERT SQL for use in statement sets
        
        Args:
            from_table: Source table/view name
            
        Returns:
            INSERT SQL string for optimized schema
        """
        return f"""
            INSERT INTO `{self.table_name}`
            SELECT 
                `timestamp`,
                log_date,
                log_hour,
                serviceName,
                severityText,
                msg,
                url,
                mobile,
                is_valid_json,
                has_data_mobile,
                is_getotp_url,
                time_bucket_10min,
                time_bucket_1hr,
                attributes,
                resources,
                body
            FROM `{from_table}`
        """