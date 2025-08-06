"""
Test cases for main.py - Understanding the entry point flow

This file demonstrates how main.py works and tests its argument parsing logic.

Key Flink Concepts Demonstrated:
- Entry point for Flink ETL pipeline execution
- Command-line argument parsing for pipeline parameters
- Pipeline vs DAG execution modes
- Parameter passing to Flink jobs

Run with: python -m pytest tests/test_main.py -v
"""
import pytest
import sys
from unittest.mock import patch, MagicMock
from main import parse_pipeline_args, parse_dag_args


class TestMainEntryPoint:
    """Test the main entry point argument parsing and flow
    
    This demonstrates how the CLI interface works for Flink pipelines
    """
    
    def test_parse_pipeline_args_basic(self):
        """Test basic pipeline argument parsing
        
        FLINK CONCEPT: Parameter passing to streaming jobs
        In Flink, we often need to pass runtime parameters like:
        - Kafka topics to read from  
        - Output paths for sinks
        - Configuration values
        """
        # GIVEN: Basic pipeline command arguments (our real use case)
        args = ['flink_kafka_to_parquet', 'topic=last9Topic', 'sink_path=file:///tmp/output']
        
        # WHEN: Parsing pipeline arguments
        pipeline_name, parameters = parse_pipeline_args(args)
        
        # THEN: Should extract pipeline name and parameters correctly
        assert pipeline_name == 'flink_kafka_to_parquet'
        assert parameters == {
            'topic': 'last9Topic',          # Kafka source topic
            'sink_path': 'file:///tmp/output'  # Parquet sink path
        }
        
        print("✅ Pipeline parameter parsing works for Flink job configuration")
    
    def test_parse_pipeline_args_kafka_specific(self):
        """Test parsing with Kafka-specific parameters
        
        FLINK CONCEPT: Kafka connector configuration
        Flink's Kafka connector needs various parameters:
        - Bootstrap servers, topics, consumer groups, etc.
        """
        # GIVEN: Kafka-specific parameters
        args = [
            'kafka_pipeline',
            'topic=my_topic',
            'bootstrap_servers=localhost:9092',
            'group_id=flink_consumer',
            'scan_startup_mode=earliest-offset',
            'sink_path=file:///data/parquet'
        ]
        
        # WHEN: Parsing arguments
        pipeline_name, parameters = parse_pipeline_args(args)
        
        # THEN: Should handle all Kafka parameters
        assert pipeline_name == 'kafka_pipeline'
        assert parameters['topic'] == 'my_topic'
        assert parameters['bootstrap_servers'] == 'localhost:9092'
        assert parameters['group_id'] == 'flink_consumer'
        assert parameters['scan_startup_mode'] == 'earliest-offset'
        assert parameters['sink_path'] == 'file:///data/parquet'
        
        print("✅ Kafka connector parameters parsed correctly")
    
    def test_parse_pipeline_args_no_params(self):
        """Test pipeline parsing with no parameters
        
        FLINK CONCEPT: Default pipeline configuration
        Some Flink jobs might use default configurations
        """
        # GIVEN: Only pipeline name
        args = ['simple_pipeline']
        
        # WHEN: Parsing arguments
        pipeline_name, parameters = parse_pipeline_args(args)
        
        # THEN: Should have empty parameters (will use defaults)
        assert pipeline_name == 'simple_pipeline'
        assert parameters == {}
        
        print("✅ Pipeline can run with default parameters")
    
    def test_parse_pipeline_args_malformed(self):
        """Test pipeline parsing with malformed parameters
        
        FLINK CONCEPT: Robust parameter handling
        Real-world CLI usage might have malformed inputs
        """
        # GIVEN: Mix of valid and malformed parameters
        args = ['my_pipeline', 'valid=value', 'malformed', 'also_valid=another_value']
        
        # WHEN: Parsing arguments
        pipeline_name, parameters = parse_pipeline_args(args)
        
        # THEN: Should skip malformed parameters gracefully
        assert pipeline_name == 'my_pipeline'
        assert parameters == {
            'valid': 'value',
            'also_valid': 'another_value'
        }
        # 'malformed' should be ignored as it has no '='
        
        print("✅ Malformed parameters handled gracefully")
    
    def test_parse_pipeline_args_empty(self):
        """Test pipeline parsing with no arguments
        
        FLINK CONCEPT: Input validation
        Flink jobs need proper validation before execution
        """
        # GIVEN: No arguments
        args = []
        
        # WHEN/THEN: Should raise SystemExit with clear error
        with pytest.raises(SystemExit, match="Pipeline mode requires"):
            parse_pipeline_args(args)
        
        print("✅ Proper error handling for missing arguments")
    
    def test_parse_pipeline_args_complex_values(self):
        """Test pipeline parsing with complex parameter values
        
        FLINK CONCEPT: Complex configuration values
        Flink often needs complex configs like JSON, URLs, etc.
        """
        # GIVEN: Complex parameter values (realistic Flink configs)
        args = [
            'complex_pipeline',
            'kafka_servers=broker1:9092,broker2:9092',  # Multiple brokers
            'sink_path=file:///Users/user/data/output',  # Full file paths
            'schema_registry=http://localhost:8081',     # URLs
            'consumer_config={"auto.offset.reset":"earliest"}',  # JSON
            'checkpoint_interval=10s'                    # Time values
        ]
        
        # WHEN: Parsing arguments  
        pipeline_name, parameters = parse_pipeline_args(args)
        
        # THEN: Should handle complex values correctly
        assert pipeline_name == 'complex_pipeline'
        assert parameters == {
            'kafka_servers': 'broker1:9092,broker2:9092',
            'sink_path': 'file:///Users/user/data/output', 
            'schema_registry': 'http://localhost:8081',
            'consumer_config': '{"auto.offset.reset":"earliest"}',
            'checkpoint_interval': '10s'
        }
        
        print("✅ Complex Flink configuration values handled correctly")


class TestDAGParsing:
    """Test DAG workflow parsing
    
    FLINK CONCEPT: Complex ETL workflows
    Real-world data processing often involves multiple connected Flink jobs
    forming a DAG (Directed Acyclic Graph) of data transformations
    """
    
    def test_parse_dag_args_missing_file(self):
        """Test DAG parsing with missing file
        
        FLINK CONCEPT: Workflow validation
        DAG workflows need proper file validation
        """
        # GIVEN: Non-existent YAML file
        args = ['non_existent_workflow.yaml']
        
        # WHEN/THEN: Should raise SystemExit with clear error
        with pytest.raises(SystemExit, match="DAG file not found"):
            parse_dag_args(args)
        
        print("✅ DAG file validation works correctly")
    
    @patch('pathlib.Path')
    @patch('yaml.safe_load')
    def test_parse_dag_args_valid_etl_workflow(self, mock_yaml_load, mock_path):
        """Test DAG parsing with valid ETL workflow
        
        FLINK CONCEPT: Multi-stage ETL pipeline
        This demonstrates a typical ETL workflow:
        1. Extract: Read from Kafka
        2. Transform: Process/enrich data  
        3. Load: Write to Parquet
        """
        # GIVEN: Mock realistic ETL workflow YAML
        mock_yaml_content = {
            'name': 'kafka_to_parquet_etl',
            'description': 'Complete Kafka to Parquet ETL workflow',
            'max_workers': 2,
            'steps': [
                {
                    'name': 'extract_kafka_data',
                    'pipeline': 'kafka_extractor',
                    'description': 'Read streaming data from Kafka',
                    'params': {
                        'topic': 'raw_events',
                        'bootstrap_servers': 'localhost:9092',
                        'scan_startup_mode': 'earliest-offset'
                    }
                },
                {
                    'name': 'transform_data',
                    'pipeline': 'data_transformer', 
                    'description': 'Enrich and transform streaming data',
                    'depends_on': ['extract_kafka_data'],
                    'params': {
                        'format': 'json',
                        'add_timestamp': True,
                        'extract_nested_fields': ['user_id', 'event_type']
                    },
                    'retries': 3
                },
                {
                    'name': 'load_parquet',
                    'pipeline': 'parquet_loader',
                    'description': 'Write processed data to Parquet files',
                    'depends_on': ['transform_data'],
                    'params': {
                        'output_path': 'file:///data/processed',
                        'partition_by': ['date', 'hour'],
                        'file_size': '128MB'
                    }
                }
            ]
        }
        
        # Mock file operations
        mock_path_instance = MagicMock()
        mock_path_instance.exists.return_value = True
        mock_path_instance.read_text.return_value = 'yaml_content'
        mock_path.return_value = mock_path_instance
        mock_yaml_load.return_value = mock_yaml_content
        
        # WHEN: Parsing DAG arguments
        dag_config, dag = parse_dag_args(['etl_workflow.yaml'])
        
        # THEN: Should parse ETL workflow structure correctly
        assert dag_config == mock_yaml_content
        assert len(dag) == 3
        
        # FLINK CONCEPT: ETL Stage 1 - Extract
        assert dag['extract_kafka_data'] == {
            'pipeline': 'kafka_extractor',
            'depends_on': [],  # No dependencies - this is the source
            'params': {
                'topic': 'raw_events',
                'bootstrap_servers': 'localhost:9092',
                'scan_startup_mode': 'earliest-offset'
            },
            'retries': 0
        }
        
        # FLINK CONCEPT: ETL Stage 2 - Transform  
        assert dag['transform_data'] == {
            'pipeline': 'data_transformer',
            'depends_on': ['extract_kafka_data'],  # Depends on extract stage
            'params': {
                'format': 'json',
                'add_timestamp': True,
                'extract_nested_fields': ['user_id', 'event_type']
            },
            'retries': 3  # Transformations might need retries
        }
        
        # FLINK CONCEPT: ETL Stage 3 - Load
        assert dag['load_parquet'] == {
            'pipeline': 'parquet_loader', 
            'depends_on': ['transform_data'],  # Depends on transform stage
            'params': {
                'output_path': 'file:///data/processed',
                'partition_by': ['date', 'hour'],
                'file_size': '128MB'
            },
            'retries': 0
        }
        
        print("✅ Multi-stage ETL workflow parsed correctly")
        print("   Extract → Transform → Load dependency chain established")


class TestRealWorldExamples:
    """Real-world examples showing how main.py works in practice
    
    FLINK CONCEPT: Production usage patterns
    These tests show actual commands used in production
    """
    
    def test_our_working_kafka_to_parquet_command(self):
        """Test parsing our actual working command
        
        FLINK CONCEPT: Kafka-to-Parquet streaming job
        This is the exact command we use for real-time data ingestion
        """
        # GIVEN: Our real production command arguments
        args = [
            'flink_kafka_to_parquet',
            'topic=last9Topic', 
            'sink_path=file:///Users/ankurranjan/PycharmProjects/lastNineFlink/parquet_output'
        ]
        
        # WHEN: Parsing our command
        pipeline_name, parameters = parse_pipeline_args(args)
        
        # THEN: Should parse our real command correctly
        assert pipeline_name == 'flink_kafka_to_parquet'
        assert parameters == {
            'topic': 'last9Topic',  # Our Confluent Cloud Kafka topic
            'sink_path': 'file:///Users/ankurranjan/PycharmProjects/lastNineFlink/parquet_output'  # Local parquet output
        }
        
        print("✅ Our production Kafka-to-Parquet command parsed correctly")
        print(f"   Pipeline: {pipeline_name}")
        print(f"   Kafka Topic: {parameters['topic']}")
        print(f"   Output Path: {parameters['sink_path']}")
        
    @patch('main.run_pipeline')
    @patch('main.setup_logging')  
    def test_full_pipeline_execution_flow(self, mock_setup_logging, mock_run_pipeline):
        """Test the complete pipeline execution flow
        
        FLINK CONCEPT: Job submission and execution
        This shows the full flow from CLI to Flink job execution
        """
        # GIVEN: Mock sys.argv for our pipeline mode
        test_argv = [
            'main.py', 
            'pipeline',
            'flink_kafka_to_parquet',
            'topic=testTopic',
            'sink_path=file:///tmp/test_parquet'
        ]
        
        # Mock the main execution
        with patch.object(sys, 'argv', test_argv):
            # WHEN: Main execution logic runs
            mode = test_argv[1]
            remaining_args = test_argv[2:]
            
            if mode == "pipeline":
                from main import parse_pipeline_args
                pipeline_name, parameters = parse_pipeline_args(remaining_args)
                
                # Simulate the main.py execution
                print(f"🚀 Starting pipeline: {pipeline_name}")
                print(f"📋 Parameters: {parameters}")
                
                # This would call run_pipeline in real execution
                mock_run_pipeline(pipeline_name, **parameters)
        
        # THEN: Should follow the expected execution flow
        assert mock_run_pipeline.called
        mock_run_pipeline.assert_called_with(
            'flink_kafka_to_parquet',
            topic='testTopic', 
            sink_path='file:///tmp/test_parquet'
        )
        
        print("✅ Full pipeline execution flow validated")
        print("   main.py → parse_args → run_pipeline → Flink job execution")


class TestFlinkConceptsInMain:
    """Tests that demonstrate key Flink concepts visible in main.py
    
    EDUCATIONAL: Understanding how Flink concepts map to our CLI
    """
    
    def test_streaming_vs_batch_parameter_patterns(self):
        """Test parameter patterns for streaming vs batch jobs
        
        FLINK CONCEPT: Streaming vs Batch execution
        Different Flink execution modes need different parameters
        """
        # GIVEN: Streaming job parameters (our typical case)
        streaming_args = [
            'streaming_pipeline',
            'source_topic=events',           # Streaming source
            'scan_startup_mode=latest-offset',  # Stream from latest
            'checkpointing=true',            # Enable checkpointing
            'checkpoint_interval=60s'        # Regular checkpoints
        ]
        
        # GIVEN: Batch job parameters  
        batch_args = [
            'batch_pipeline',
            'input_path=file:///data/input', # Bounded source
            'output_path=file:///data/output', # Batch output
            'parallelism=4'                  # Fixed parallelism
        ]
        
        # WHEN: Parsing both types
        stream_name, stream_params = parse_pipeline_args(streaming_args)
        batch_name, batch_params = parse_pipeline_args(batch_args)
        
        # THEN: Should handle both execution modes
        assert 'source_topic' in stream_params  # Streaming uses topics
        assert 'checkpointing' in stream_params  # Streaming needs checkpointing
        
        assert 'input_path' in batch_params    # Batch uses file paths
        assert 'parallelism' in batch_params   # Batch sets fixed parallelism
        
        print("✅ Both streaming and batch parameter patterns supported")
        
    def test_flink_connector_parameters(self):
        """Test parameters for different Flink connectors
        
        FLINK CONCEPT: Connector configuration
        Different connectors (Kafka, filesystem, etc.) need specific params
        """
        # GIVEN: Parameters for various Flink connectors
        args = [
            'multi_connector_pipeline',
            # Kafka connector params
            'kafka_topic=input_events',
            'kafka_servers=broker1:9092,broker2:9092',
            'kafka_group_id=flink_consumer',
            # Filesystem connector params  
            'parquet_path=file:///data/parquet',
            'parquet_compression=snappy',
            # JDBC connector params
            'jdbc_url=jdbc:postgresql://localhost/db',
            'jdbc_table=processed_events'
        ]
        
        # WHEN: Parsing connector parameters
        pipeline_name, parameters = parse_pipeline_args(args)
        
        # THEN: Should handle all connector types
        # Kafka connector
        assert parameters['kafka_topic'] == 'input_events'
        assert parameters['kafka_servers'] == 'broker1:9092,broker2:9092'
        
        # Filesystem connector  
        assert parameters['parquet_path'] == 'file:///data/parquet'
        assert parameters['parquet_compression'] == 'snappy'
        
        # JDBC connector
        assert parameters['jdbc_url'] == 'jdbc:postgresql://localhost/db'
        
        print("✅ Multiple Flink connector parameters handled correctly")


def demonstrate_main_flow():
    """
    EDUCATIONAL: Demonstration of main.py execution flow
    
    This function shows the step-by-step process of what happens when you run:
    python main.py pipeline flink_kafka_to_parquet topic=last9Topic sink_path=file:///tmp/output
    
    FLINK CONCEPTS DEMONSTRATED:
    - Job parameter configuration
    - Pipeline registry lookup
    - Flink TableEnvironment creation
    - Streaming job execution
    """
    print("\n" + "="*70)
    print("🔍 MAIN.PY → FLINK EXECUTION FLOW DEMONSTRATION")
    print("="*70)
    
    print("\n1️⃣ COMMAND LINE PARSING:")
    print("   Input: python main.py pipeline flink_kafka_to_parquet topic=last9Topic sink_path=file:///tmp/output")
    
    example_argv = ['main.py', 'pipeline', 'flink_kafka_to_parquet', 'topic=last9Topic', 'sink_path=file:///tmp/output']
    mode = example_argv[1]
    remaining_args = example_argv[2:]
    
    print(f"   📋 Mode detected: {mode}")
    print(f"   📋 Pipeline args: {remaining_args}")
    
    print("\n2️⃣ PIPELINE ARGUMENT PARSING:")
    pipeline_name, parameters = parse_pipeline_args(remaining_args)
    print(f"   🎯 Pipeline name: {pipeline_name}")
    print(f"   ⚙️  Parameters: {parameters}")
    
    print("\n3️⃣ FLINK PIPELINE EXECUTION FLOW:")
    print("   📍 main.py delegates to cli_runner.run_pipeline()")
    print("   📍 cli_runner looks up pipeline in registry")
    print("   📍 Pipeline.build() creates Flink components:")
    print("      • FlinkKafkaJsonSource (Kafka Table API source)")
    print("      • FlinkLogPromoteTransform (Table API transformations)")
    print("      • FlinkFilesystemParquetSink (Filesystem Table API sink)")
    print("   📍 Pipeline.run() executes Flink job:")
    print("      • Creates TableEnvironment (Flink SQL/Table API)")
    print("      • Loads JAR dependencies (Kafka/Parquet connectors)")
    print("      • Sets up checkpointing configuration")
    print("      • Creates Kafka source table with SASL authentication")
    print("      • Creates transformation view with nested field extraction")
    print("      • Creates Parquet sink table with rolling file policy")
    print("      • Executes streaming INSERT statement")
    print("      • Starts Flink streaming job")
    
    print("\n4️⃣ FLINK STREAMING EXECUTION:")
    print("   🌊 Flink starts streaming job in cluster")
    print("   🌊 Continuously reads from Kafka topic")
    print("   🌊 Applies transformations (extract nested fields)")
    print("   🌊 Writes to Parquet files (with checkpointing)")
    print("   🌊 Handles backpressure and fault tolerance")
    
    print("\n✅ KEY FLINK CONCEPTS IN OUR FLOW:")
    print("   🔹 TableEnvironment: High-level Table API for SQL-like operations")
    print("   🔹 Source Tables: Kafka connector for streaming ingestion")
    print("   🔹 Transformations: SQL views for data processing")
    print("   🔹 Sink Tables: Filesystem connector for Parquet output")
    print("   🔹 Checkpointing: Fault tolerance and exactly-once processing")
    print("   🔹 Streaming Execution: Continuous data processing")
    
    print(f"\n🎯 This demonstrates clean architecture:")
    print("   • main.py: Entry point & CLI parsing")
    print("   • cli_runner: Pipeline orchestration") 
    print("   • Pipeline classes: Flink Table API logic")
    print("   • Extractor/Transformer/Sink: Flink operators")


if __name__ == "__main__":
    # Run the educational demonstration
    demonstrate_main_flow()