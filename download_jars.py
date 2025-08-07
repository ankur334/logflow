#!/usr/bin/env python3
"""Download required Flink connector JARs"""

import os
import urllib.request
from pathlib import Path

FLINK_VERSION = "1.20"
KAFKA_VERSION = "3.3.0-1.20"
PARQUET_VERSION = "1.20.0"
JARS_DIR = Path("jars")

JARS = [
    {
        "name": "flink-sql-connector-kafka",
        "url": f"https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/{KAFKA_VERSION}/flink-sql-connector-kafka-{KAFKA_VERSION}.jar"
    },
    {
        "name": "flink-sql-parquet",
        "url": f"https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-parquet/{PARQUET_VERSION}/flink-sql-parquet-{PARQUET_VERSION}.jar"
    }
]

def download_jars():
    JARS_DIR.mkdir(exist_ok=True)
    
    for jar in JARS:
        filename = jar["url"].split("/")[-1]
        filepath = JARS_DIR / filename
        
        if filepath.exists():
            print(f"✓ {filename} already exists")
        else:
            print(f"Downloading {filename}...")
            urllib.request.urlretrieve(jar["url"], filepath)
            print(f"✓ Downloaded {filename}")
    
    print(f"\nAll JARs downloaded to {JARS_DIR.absolute()}")
    print(f"Set environment variable: export FLINK_PIPELINE_JARS=\"{JARS_DIR.absolute()}/*.jar\"")

if __name__ == "__main__":
    download_jars()