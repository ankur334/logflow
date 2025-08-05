# config/

This directory contains all configuration files and helpers for running and managing your data pipelines.

**⚠️ Never commit real secrets, credentials, or tokens to this directory or to version control. Always use placeholders, blanks, or environment variables for sensitive values.**

---

## Folder Layout

```
config/
├── properties/      # All .properties config files (e.g., Kafka, RedPanda)
│   ├── client.properties
│   ├── redpanda.properties
│   └── sqs.properties
├── yaml/            # All YAML pipeline/workflow configs
│   ├── pipeline.yaml
│   └── other_pipeline.yaml
├── json/            # All JSON-based configs (optional)
│   └── example_config.json
├── config_reader.py # Python module for loading config files
├── config_context.py# Python context manager(s) for config/secrets (optional)
├── .env.example     # Example environment variable file (NEVER commit real .env)
```

---

## Guidelines

- **DO NOT COMMIT any actual secrets or credentials.**
    - Use placeholder values or leave sensitive fields blank.
    - Supply secrets at runtime via environment variables, secret managers, or CI/CD pipelines.
- Each folder contains a single type of config for clarity and extensibility.

---

## How to Use

- **.properties:**  
  Place Kafka/RedPanda/SQS client config files in `properties/`.  
  Example:  
- `config/properties/client.properties`  
  ```properties
  bootstrap.servers=localhost:9092
  security.protocol=PLAINTEXT
  ```
  
*(Fill secrets via environment variables, not in this file.)*

- **.yaml:**  
Place pipeline definitions or workflow configs in `yaml/`.
- Good for specifying extractors, transformers, sinks, and pipeline parameters.

- **.json:**  
Use for configs that need to interoperate with APIs, or for structured, non-hierarchical data.

---

## Loading Configs in Code

Use the shared loaders provided in `config/config_reader.py`, for example:

```python
from config.config_reader import read_properties, read_yaml, read_json

kafka_config = read_properties("config/properties/client.properties")
pipeline_config = read_yaml("config/yaml/pipeline.yaml")
```


## Environment Variables

Use environment variables for all sensitive values, such as passwords, tokens, and keys.
Maintain an example file, .env.example, with required variable names.


## Adding New Configurations
- Place your new .properties, .yaml, or .json file in the correct subfolder.
- Never commit real secrets—use placeholders or blanks.

