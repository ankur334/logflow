"""
Main entry point for the Flink ETL Pipeline System

This system provides two execution modes:
1. PIPELINE mode: Run individual ETL pipelines (our main use case)
2. DAG mode: Run multiple pipelines as a directed acyclic graph

Example usage:
- Single pipeline: python main.py pipeline flink_kafka_to_parquet topic=myTopic sink_path=file:///path/to/output
- DAG workflow: python main.py dag workflow.yaml
"""
import sys

from runner.cli_runner import run_pipeline
from runner.dag_runner import run_dag
from utils.log import setup_logging


def parse_pipeline_args(args):
    """
    Parse command line arguments for pipeline mode
    
    Args:
        args: List of command line arguments
        
    Returns:
        tuple: (pipeline_name, parameters_dict)
        
    Example:
        ['flink_kafka_to_parquet', 'topic=myTopic', 'sink_path=file:///tmp/output']
        -> ('flink_kafka_to_parquet', {'topic': 'myTopic', 'sink_path': 'file:///tmp/output'})
    """
    if len(args) < 1:
        raise SystemExit("Pipeline mode requires: python main.py pipeline <name> [k=v]...")
    
    pipeline_name = args[0]
    # Parse key=value parameters (skip malformed ones)
    parameters = dict(arg.split("=", 1) for arg in args[1:] if "=" in arg)
    
    return pipeline_name, parameters


def parse_dag_args(args):
    """
    Parse command line arguments for DAG mode
    
    Args:
        args: List of command line arguments containing YAML file path
        
    Returns:
        dict: Parsed DAG configuration
    """
    if len(args) < 1:
        raise SystemExit("DAG mode requires: python main.py dag <yaml-path>")
    
    import yaml
    import pathlib
    
    yaml_path = pathlib.Path(args[0])
    if not yaml_path.exists():
        raise SystemExit(f"DAG file not found: {yaml_path}")
    
    # Load and parse YAML configuration
    dag_config = yaml.safe_load(yaml_path.read_text())
    
    # Transform into internal DAG representation
    # Each step becomes: {name: {pipeline, depends_on, params, retries}}
    dag = {}
    for step in dag_config["steps"]:
        dag[step["name"]] = {
            "pipeline": step["pipeline"],
            "depends_on": step.get("depends_on", []),
            "params": step.get("params", {}),
            "retries": step.get("retries", 0)
        }
    
    return dag_config, dag


if __name__ == "__main__":
    # Initialize logging system for all pipeline operations
    setup_logging()
    
    # Validate minimum command line arguments
    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage:\n"
            "  python main.py pipeline <name> [k=v]...  # Run single pipeline\n"
            "  python main.py dag <yaml-path>           # Run DAG workflow\n"
            "\n"
            "Examples:\n"
            "  python main.py pipeline flink_kafka_to_parquet topic=myTopic sink_path=file:///tmp/output\n"
            "  python main.py dag workflows/my_dag.yaml"
        )
    
    # Determine execution mode from first argument
    mode = sys.argv[1].lower()
    remaining_args = sys.argv[2:]
    
    if mode == "pipeline":
        # PIPELINE MODE: Execute a single ETL pipeline
        # This is our primary use case for Kafka -> Parquet processing
        pipeline_name, parameters = parse_pipeline_args(remaining_args)
        
        print(f"🚀 Starting pipeline: {pipeline_name}")
        print(f"📋 Parameters: {parameters}")
        
        # Delegate to CLI runner which will:
        # 1. Look up pipeline in registry
        # 2. Build pipeline with parameters  
        # 3. Execute Flink job
        run_pipeline(pipeline_name, **parameters)
        
    elif mode == "dag":
        # DAG MODE: Execute multiple pipelines with dependencies
        # Useful for complex workflows with multiple ETL steps
        dag_config, dag = parse_dag_args(remaining_args)
        
        print(f"🔄 Running DAG workflow")
        print(f"📋 Configuration: {dag_config}")
        print(f"🏗️  Dependency graph: {dag}")
        
        # Delegate to DAG runner which will:
        # 1. Resolve pipeline dependencies
        # 2. Execute pipelines in correct order
        # 3. Handle retries and failures
        max_workers = dag_config.get("max_workers", 4)
        run_dag(dag, max_workers=max_workers)
        
    else:
        raise SystemExit(f"❌ Unknown mode: {mode}. Use 'pipeline' or 'dag'")
    
    print("✅ Execution completed")