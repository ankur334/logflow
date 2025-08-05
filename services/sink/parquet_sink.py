from parquet_writer import flatten_event
from .base_sink import AbstractSink
import pyarrow as pa
import pyarrow.parquet as pq


class ParquetSink(AbstractSink):
    def __init__(self, output_dir):
        self.output_dir = output_dir

    def write(self, records):
        # flatten records, create table, write parquet
        # (reuse flatten_event from earlier)
        table = pa.Table.from_pylist([flatten_event(r) for r in records])
        filename = ...  # build path dynamically
        pq.write_table(table, filename)
