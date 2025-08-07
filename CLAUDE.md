# Claude Code Configuration

## Project Overview
Apache Flink ETL Pipeline project that streams data from Kafka to Parquet format using PyFlink.

## Key Commands
```bash
# Install dependencies
pip install -r requirements.txt

# Download Flink JARs
python download_jars.py

# Run tests
python -m pytest tests/ -v

# Run main pipeline
python main.py pipeline flink_kafka_to_parquet topic=last9Topic sink_path=file://$(pwd)/parquet_output

# Run with coverage
python -m pytest tests/ --cov=. --cov-report=html
```

## Project Structure
- `main.py` - CLI entry point for pipeline execution
- `pipeline/` - Pipeline implementations (ETL orchestration)
- `extractor/` - Data source components (Kafka, etc.)
- `transformer/` - Data transformation logic
- `sink/` - Data output components (Parquet, etc.)
- `config/` - Configuration files for different environments
- `tests/` - Test suite
- `utils/` - Shared utilities
- `runner/` - Execution runners (CLI, DAG, Airflow)

## Dependencies
- Python 3.11+
- Apache Flink 1.20.0
- Kafka client libraries
- PyArrow for Parquet support

## Environment Setup
Configuration uses `.env.dev` for development settings (not committed).
Kafka credentials configured in `config/properties/` files.

## Testing Strategy
Uses pytest with coverage reporting. Tests organized by component type.