# Apache Flink ETL Pipeline - Kafka to Parquet

A production-ready Apache Flink streaming ETL pipeline that reads from Kafka, transforms data, and writes to Parquet format. Built with PyFlink Table API for high-level stream processing.

## 🚀 Quick Start

```bash
# Install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run the pipeline
python main.py pipeline flink_kafka_to_parquet \
    topic=last9Topic \
    sink_path=file://$(pwd)/parquet_output
```

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Main Components](#main-components)
- [Installation](#installation)
- [Usage](#usage)
- [Flink Concepts](#flink-concepts)
- [Testing](#testing)
- [Configuration](#configuration)

## 🏗️ Architecture Overview

```
CLI Entry Point (main.py)
    ↓
Pipeline Registry (cli_runner.py)  
    ↓
Pipeline Classes (flink_kafka_to_parquet.py)
    ├── Extractor (Kafka Source)
    ├── Transformer (Nested Field Extraction)  
    └── Sink (Parquet Writer)
    ↓
Flink TableEnvironment (Streaming Execution)
```

## 📦 Main Components

### 1. **main.py** - CLI Entry Point

The main entry point provides two execution modes:

#### Pipeline Mode (Primary Use Case)
```bash
python main.py pipeline <pipeline_name> [parameters...]
```

**Key Functions:**
- `parse_pipeline_args()`: Parses CLI arguments into pipeline name and parameters
- Delegates to `cli_runner` for pipeline execution

**Example:**
```bash
python main.py pipeline flink_kafka_to_parquet \
    topic=myTopic \
    sink_path=file:///tmp/output
```

#### DAG Mode (Complex Workflows)
```bash
python main.py dag <workflow.yaml>
```
Executes multiple pipelines with dependencies.

### 2. **Pipeline System**

#### Base Pipeline (`pipeline/base_pipeline.py`)
Abstract base class defining the ETL pipeline interface:
```python
class AbstractPipeline(ABC):
    @abstractmethod
    def run(self) -> None: pass
    
    @classmethod
    @abstractmethod  
    def build(cls, **kwargs): pass
```

#### Flink Kafka to Parquet Pipeline (`pipeline/flink_kafka_to_parquet.py`)
Main streaming pipeline implementation using Flink Table API:
- Creates `TableEnvironment` for streaming mode
- Configures checkpointing for fault tolerance
- Orchestrates Extractor → Transformer → Sink flow

### 3. **Extractors** (Data Sources)

#### Flink Kafka Source (`extractor/flink_kafka_extractor.py`)
Creates Kafka source table with:
- SASL/SSL authentication for Confluent Cloud
- JSON format parsing
- Configurable scan modes (earliest/latest offset)

**Flink SQL DDL Example:**
```sql
CREATE TABLE kafka_logs (
    `timestamp` STRING,
    serviceName STRING,
    severityText STRING,
    attributes MAP<STRING, STRING>,
    resources MAP<STRING, STRING>,
    body STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'last9Topic',
    'properties.bootstrap.servers' = 'broker:9092',
    'format' = 'json'
)
```

### 4. **Transformers** (Data Processing)

#### Log Transform (`transformer/flink_log_transform.py`)
Creates SQL view for data transformation:
- Extracts nested JSON fields
- Creates computed columns
- Handles null values with COALESCE

**Transformation SQL:**
```sql
CREATE VIEW logs_enriched AS
SELECT
    `timestamp`,
    serviceName,
    severityText,
    attributes['msg'] AS msg,
    attributes['url'] AS url,
    COALESCE(JSON_VALUE(body, '$.data.mobile'), attributes['mobile']) AS mobile,
    attributes,
    resources,
    body
FROM kafka_logs
```

### 5. **Sinks** (Data Destinations)

#### Parquet Sink (`sink/flink_parquet_sink.py`)
Creates filesystem sink with:
- Parquet format output
- Configurable rolling policies
- File size and time-based rolling

**Sink Configuration:**
```sql
CREATE TABLE parquet_sink (
    -- columns
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///path/to/output',
    'format' = 'parquet',
    'sink.rolling-policy.file-size' = '1KB',
    'sink.rolling-policy.rollover-interval' = '10s'
)
```

## 🛠️ Installation

### Prerequisites
- **Python**: 3.11.9 (tested version)
- **Java**: 11 or higher (required by Apache Flink)
- **Memory**: Minimum 4GB RAM recommended
- **Kafka Access**: Confluent Cloud, AWS MSK, or RedPanda cluster

### Setup Steps

1. **Create Virtual Environment:**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. **Install Python Dependencies:**
```bash
pip install -r requirements.txt
```

**Exact Python Dependencies:**
```
apache-flink==1.20.0          # PyFlink framework
confluent-kafka==2.11.0       # Kafka client library
python-dotenv==1.1.1          # Environment configuration
PyYAML==6.0.2                 # YAML configuration support
pyarrow==11.0.0               # Parquet file format support
pandas==2.0.3                 # Data manipulation
pytest==8.4.1                 # Testing framework (dev dependency)
```

3. **Download Required JAR Files:**
```bash
python download_jars.py
```

**Complete JAR Dependencies with Exact Versions:**

| Component | JAR File | Version | Purpose |
|-----------|----------|---------|---------|
| **Flink Connectors** |
| Kafka Connector | `flink-sql-connector-kafka.jar` | 3.3.0-1.20 | Kafka source/sink |
| Parquet Connector | `flink-sql-parquet.jar` | 1.20.0 | Parquet file format |
| **Hadoop Ecosystem** |
| Hadoop Common | `hadoop-common.jar` | 3.3.4 | HDFS/filesystem operations |
| Hadoop MapReduce | `hadoop-mapreduce-client-core.jar` | 3.3.4 | MapReduce client |
| Hadoop Shaded Guava | `hadoop-shaded-guava.jar` | 1.1.1 | Guava for Hadoop |
| **Supporting Libraries** |
| Google Guava | `guava.jar` | 31.1.0 | Core utilities |
| Commons Configuration | `commons-configuration2.jar` | 2.8.0 | Configuration management |
| Commons Text | `commons-text.jar` | 1.9 | Text processing utilities |
| Stax2 API | `stax2-api.jar` | 4.2.1 | XML processing API |
| Woodstox Core | `woodstox-core.jar` | 6.4.0 | XML processing implementation |

**Total JAR Size**: ~30MB (10 JAR files)

4. **Verify Installation:**
```bash
# Check Python and dependencies
python --version  # Should show 3.11.9
python -c "import pyflink; print(pyflink.__version__)"  # Should show 1.20.0

# Check JAR files
ls -la jars/  # Should list 10 JAR files

# Run tests to verify setup
python -m pytest tests/ -v
```

## 📊 Flink Concepts Explained

### Table API & SQL
Our pipeline uses Flink's Table API, which provides:
- **High-level abstraction** over DataStream API
- **SQL-like operations** for data processing
- **Unified batch/streaming** processing model

### Streaming Execution
```python
# Create streaming environment
settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(settings)

# Configure checkpointing for fault tolerance
t_env.get_config().get_configuration().set_string(
    "execution.checkpointing.interval", "10 s"
)
```

### Connectors
Flink uses connectors to interact with external systems:

**Kafka Connector:**
- Reads unbounded streams from Kafka topics
- Supports exactly-once semantics with checkpointing
- Handles authentication (SASL/SSL)

**Filesystem Connector:**
- Writes to various file formats (Parquet, CSV, JSON)
- Implements rolling file policies
- Supports partitioned writes

### Checkpointing & Fault Tolerance
- **Checkpoints**: Periodic snapshots of streaming state
- **Recovery**: Automatic recovery from failures
- **Exactly-once**: Guarantees no data loss or duplication

## 🧪 Testing

Run tests with pytest:
```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_main.py -v

# Run with coverage
python -m pytest tests/ --cov=. --cov-report=html
```

Test organization:
- `tests/test_main.py` - Entry point and CLI tests
- `tests/test_pipeline.py` - Pipeline logic tests
- `tests/test_extractors.py` - Source connector tests
- `tests/test_transformers.py` - Transformation logic tests
- `tests/test_sinks.py` - Sink connector tests

## ⚙️ Configuration

### Environment Configuration
Configure credentials in `.env.dev` file (for development):
```bash
# Development Environment Configuration for Kafka
BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-2.aws.confluent.cloud:9092
SASL_USERNAME=YOUR_API_KEY
SASL_PASSWORD=YOUR_API_SECRET
KAFKA_TOPIC=last9Topic
SINK_PATH=file:///tmp/last9_parquet
```

**Security Notes:**
- `.env.dev` is already in `.gitignore` and won't be committed
- Never commit credentials to version control
- Use different `.env` files for different environments (dev, staging, prod)

### Kafka Configuration
The `config/properties/confluent.properties` file now uses placeholders:
```properties
bootstrap.servers=${BOOTSTRAP_SERVERS}
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=${SASL_USERNAME}
sasl.password=${SASL_PASSWORD}
```

### Pipeline Parameters
Common parameters for `flink_kafka_to_parquet`:
- `topic` - Kafka topic to consume
- `sink_path` - Output path for Parquet files
- `bootstrap_servers` - Kafka brokers (optional, uses config)
- `scan_startup_mode` - `earliest-offset` or `latest-offset`

## 📝 Usage Examples

### Basic Kafka to Parquet
```bash
python main.py pipeline flink_kafka_to_parquet \
    topic=events \
    sink_path=file:///data/events_parquet
```

### With Custom Configuration
```bash
python main.py pipeline flink_kafka_to_parquet \
    topic=logs \
    sink_path=file:///data/logs \
    scan_startup_mode=latest-offset \
    checkpoint_interval=30s
```

### DAG Workflow
Create `workflow.yaml`:
```yaml
max_workers: 2
steps:
  - name: extract_data
    pipeline: kafka_extractor
    params:
      topic: raw_events
  - name: transform_data  
    pipeline: data_transformer
    depends_on: [extract_data]
  - name: load_data
    pipeline: parquet_loader
    depends_on: [transform_data]
```

Run workflow:
```bash
python main.py dag workflow.yaml
```

## 🔍 Monitoring & Debugging

### Console Output
The pipeline prints data flow information:
```
🚀 Starting pipeline: flink_kafka_to_parquet
📋 Parameters: {'topic': 'last9Topic', 'sink_path': 'file:///tmp/output'}
KafkaData>:6> +I[2025-08-06T09:12:30, user-service, DEBUG, {...}]
```

### Output Files
Parquet files are created with rolling policies:
```
parquet_output/
├── part-xxx-4-0 (3.4KB)
├── part-xxx-5-0 (4.4KB)
└── part-xxx-6-0 (3.9KB)
```

### Checkpoints
Monitor checkpointing in `/tmp/flink-checkpoints/`

## 🚀 Production Deployment

### Best Practices
1. **Resource Allocation**: Set appropriate parallelism
2. **Checkpointing**: Configure based on data volume
3. **Monitoring**: Use Flink metrics and logging
4. **Error Handling**: Implement proper retry logic
5. **Schema Evolution**: Plan for data schema changes

### Performance Tuning
- Adjust `checkpoint_interval` based on throughput
- Configure `rolling_policy` for optimal file sizes
- Set appropriate `watermark_delay` for late data

## 📚 Learn More

- [Apache Flink Documentation](https://flink.apache.org/docs/)
- [PyFlink Table API Guide](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/table_api_tutorial/)
- [Confluent Kafka Documentation](https://docs.confluent.io/)

## 🔧 Troubleshooting

### Version Compatibility Issues

**JAR Version Mismatches:**
```bash
# If you see ClassNotFoundException or NoSuchMethodError
# Verify all JARs are compatible with Flink 1.20
ls -la jars/
python download_jars.py  # Re-download if needed
```

**Python Version Issues:**
```bash
# If PyFlink fails to import
python --version  # Must be 3.11.9 or compatible
pip install --upgrade apache-flink==1.20.0
```

**Memory Issues:**
```bash
# If Flink jobs fail with OutOfMemoryError
export FLINK_ENV_JAVA_OPTS="-Xmx2g -Xms1g"
```

### Version Verification Commands

```bash
# Quick health check
python -c "
import sys
print(f'Python: {sys.version}')
import pyflink
print(f'PyFlink: {pyflink.__version__}')
import pandas as pd
print(f'Pandas: {pd.__version__}')
import pyarrow as pa
print(f'PyArrow: {pa.__version__}')
"

# JAR verification
find jars -name "*.jar" -exec basename {} \; | sort
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License.