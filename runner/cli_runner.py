import logging
from pipeline.registry import PIPELINE_REGISTRY
from utils.log import setup_logging


def run_pipeline(pipeline_name: str, **params):
    setup_logging()
    logging.getLogger(__name__).info("Starting pipeline '%s' with %s", pipeline_name, params)
    if pipeline_name not in PIPELINE_REGISTRY:
        raise ValueError(f"Unknown pipeline: {pipeline_name}")
    pipeline_cls = PIPELINE_REGISTRY[pipeline_name]
    pipeline = pipeline_cls.build(**params)
    pipeline.run()
