import os, logging
from datetime import datetime
from typing import Dict, Any, List
import pyarrow as pa
import pyarrow.parquet as pq
from .base_sink import AbstractSink

log = logging.getLogger(__name__)


def _flatten(event: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "timestamp": event.get("timestamp"),
        "serviceName": event.get("serviceName"),
        "severityText": event.get("severityText"),
        "msg": event.get("msg"),
        "url": event.get("url"),
        "mobile": event.get("mobile"),
        "body": event.get("body"),
    }
    for k, v in (event.get("attributes") or {}).items():
        out[f"attr_{k}"] = v
    for k, v in (event.get("resources") or {}).items():
        out[f"res_{k}"] = v
    return out


class ParquetSink(AbstractSink):
    """
    Partitioned by date/hour directories. Rolls files per batch.
    For byte-accurate rolling, add a size estimator; batch-based is often sufficient.
    """

    def __init__(self, base_path: str = "parquet_data", compression: str = "zstd"):
        self.base_path = base_path.rstrip("/")
        self.compression = compression
        os.makedirs(self.base_path, exist_ok=True)

    def _path_for(self, dt: str, hr: str) -> str:
        path = f"{self.base_path}/dt={dt}/hr={hr}"
        os.makedirs(path, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        return f"{path}/logs_{ts}.parquet"

    def write(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        flat = [_flatten(r) for r in records]
        # derive dt/hr from timestamp string if available
        dt = "unknown"
        hr = "00"
        ts_any = next((r.get("timestamp") for r in records if r.get("timestamp")), None)
        if ts_any and len(ts_any) >= 13:
            # ts like 2025-08-04T12:31:...Z
            dt = ts_any[0:10]
            hr = ts_any[11:13]
        path = self._path_for(dt, hr)
        table = pa.Table.from_pylist(flat)
        pq.write_table(table, path, compression=self.compression)
        log.info("Wrote %d rows -> %s", len(records), path)

    def flush(self) -> None:
        ...

    def close(self) -> None:
        ...
