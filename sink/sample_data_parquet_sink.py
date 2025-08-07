"""Sample Data Parquet Sink - Batch processing sink

Writes sample data to Parquet files using pandas/pyarrow.
Maintains symmetry with Flink sinks but for batch processing.
"""
import os
import json
from pathlib import Path
from typing import Iterator, List, Dict, Any
import pandas as pd
from sink.base_sink import AbstractSink


class SampleDataParquetSink(AbstractSink):
    """Batch Parquet sink for sample data
    
    Writes transformed sample data to Parquet files using pandas.
    Maintains architectural symmetry with Flink sinks.
    """
    
    def __init__(self, path: str, partition_cols: List[str] = None, 
                 file_prefix: str = "sample_data", max_records_per_file: int = 1000):
        """Initialize Parquet sink for sample data
        
        Args:
            path: Output directory path for Parquet files
            partition_cols: Columns to partition by (e.g., ['log_date', 'log_hour'])
            file_prefix: Prefix for output files
            max_records_per_file: Maximum records per Parquet file
        """
        self.path = path
        self.partition_cols = partition_cols or []
        self.file_prefix = file_prefix
        self.max_records_per_file = max_records_per_file
        
        # Ensure output directory exists
        Path(self.path).mkdir(parents=True, exist_ok=True)
        print(f"📁 Sample data sink configured:")
        print(f"   Path: {self.path}")
        print(f"   Partitions: {self.partition_cols}")
        print(f"   Max records per file: {self.max_records_per_file}")

    def flatten_nested_data(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Flatten nested JSON structures for Parquet compatibility
        
        Args:
            records: List of records with potentially nested data
            
        Returns:
            Flattened records suitable for Parquet storage
        """
        flattened_records = []
        
        for record in records:
            flattened = {}
            
            # Copy simple fields
            for key, value in record.items():
                if isinstance(value, (str, int, float, bool)) or value is None:
                    flattened[key] = value
                elif isinstance(value, dict):
                    # Convert dict to JSON string
                    flattened[f"{key}_json"] = json.dumps(value)
                else:
                    # Convert other types to string
                    flattened[f"{key}_str"] = str(value)
            
            flattened_records.append(flattened)
        
        return flattened_records

    def write_parquet_batch(self, records: List[Dict[str, Any]], batch_num: int) -> str:
        """Write a batch of records to Parquet file
        
        Args:
            records: Batch of transformed records
            batch_num: Batch number for file naming
            
        Returns:
            Path to the written Parquet file
        """
        if not records:
            print("⚠️  Empty batch, skipping Parquet write")
            return ""
        
        # Flatten nested data for Parquet compatibility
        flattened_records = self.flatten_nested_data(records)
        
        # Create DataFrame
        df = pd.DataFrame(flattened_records)
        
        # Generate filename
        filename = f"{self.file_prefix}_batch_{batch_num:03d}.parquet"
        
        if self.partition_cols:
            # Write with partitioning
            output_path = os.path.join(self.path, "partitioned")
            df.to_parquet(
                output_path, 
                partition_cols=self.partition_cols,
                index=False,
                engine='pyarrow'
            )
            print(f"📄 Written {len(records)} records to partitioned Parquet: {output_path}")
            return output_path
        else:
            # Write single file
            file_path = os.path.join(self.path, filename)
            df.to_parquet(file_path, index=False, engine='pyarrow')
            print(f"📄 Written {len(records)} records to Parquet: {file_path}")
            return file_path

    def sink(self, data: Iterator[List[Dict[str, Any]]]) -> None:
        """Sink transformed data batches to Parquet files
        
        Args:
            data: Iterator of transformed message batches
        """
        print("💾 Starting batch Parquet sink...")
        
        batch_count = 0
        total_records = 0
        written_files = []
        
        for batch in data:
            batch_count += 1
            
            if not batch:
                print(f"⚠️  Batch {batch_count} is empty, skipping...")
                continue
                
            print(f"💾 Sinking batch {batch_count} with {len(batch)} records...")
            
            try:
                file_path = self.write_parquet_batch(batch, batch_count)
                if file_path:
                    written_files.append(file_path)
                    total_records += len(batch)
                    
                    # Show sample of what was written
                    sample_record = batch[0]
                    print(f"   📋 Sample record: {sample_record.get('serviceName', 'N/A')} | {sample_record.get('mobile', 'N/A')}")
                    
            except Exception as e:
                print(f"❌ Error writing batch {batch_count}: {e}")
                continue
        
        # Summary
        print(f"\n📊 SINK SUMMARY:")
        print(f"   📦 Batches processed: {batch_count}")
        print(f"   📝 Total records written: {total_records}")
        print(f"   📁 Files created: {len(written_files)}")
        print(f"   📂 Output directory: {self.path}")
        
        if written_files:
            print(f"   📄 Sample files:")
            for file_path in written_files[:3]:  # Show first 3 files
                print(f"      - {file_path}")
        
        print("✅ Batch Parquet sink completed!")

    def register_sink_in_flink(self, t_env) -> None:
        """Not implemented for batch sink
        
        This sink is designed for batch processing, not Flink streaming.
        Use Flink*Sink classes for Flink streaming pipelines.
        """
        raise NotImplementedError("Sample data sink is for batch processing only")
        
    def insert_into_flink(self, t_env, from_table: str):
        """Not implemented for batch sink
        
        This sink is designed for batch processing, not Flink streaming.
        Use Flink*Sink classes for Flink streaming pipelines.  
        """
        raise NotImplementedError("Sample data sink is for batch processing only")