"""CLI Runner - Bridge between command line and Flink pipeline execution

This module receives pipeline name and parameters from main.py,
looks up the pipeline class, and executes the Flink job.
"""
import logging
from pipeline.registry import PIPELINE_REGISTRY
from utils.log import setup_logging


def run_pipeline(pipeline_name: str, **params):
    """Execute a Flink pipeline by name with given parameters.
    
    Flow:
    1. Setup logging for pipeline execution
    2. Validate pipeline exists in registry
    3. Build pipeline instance with parameters
    4. Execute the Flink job
    
    Args:
        pipeline_name: Registered pipeline name (e.g. 'flink_kafka_to_parquet')
        **params: Pipeline parameters (e.g. topic='myTopic', sink_path='file:///tmp')
        
    Raises:
        ValueError: If pipeline_name not found in registry
    """
    # Initialize logging for this pipeline run
    setup_logging()
    
    # Log pipeline startup
    logging.getLogger(__name__).info("Starting pipeline '%s' with %s", pipeline_name, params)
    
    # Validate pipeline exists
    if pipeline_name not in PIPELINE_REGISTRY:
        raise ValueError(f"Unknown pipeline: {pipeline_name}")
    
    # Get pipeline class from registry
    pipeline_cls = PIPELINE_REGISTRY[pipeline_name]
    
    # Build pipeline instance with parameters
    # This creates Flink TableEnvironment and configures sources/sinks
    pipeline = pipeline_cls.build(**params)
    
    # Execute the Flink streaming job
    # This starts the actual data processing
    pipeline.run()
