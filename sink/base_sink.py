from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class AbstractSink(ABC):
    @abstractmethod
    def write(self, records: List[Dict[str, Any]]):
        """Write an iterable of records to the destination sink."""
        pass

    # --- Optional Flink hooks (default no-op) ---
    def register_sink_in_flink(self, t_env) -> Optional[str]:
        """If implemented, create/register a Flink sink table and return its name."""
        return None

    def insert_into_flink(self, t_env, from_table: str) -> None:
        """If implemented, submit INSERT INTO sink SELECT … FROM from_table."""
        raise NotImplementedError

    @abstractmethod
    def flush(self):
        pass

    @abstractmethod
    def close(self):
        pass
