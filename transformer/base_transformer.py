from abc import ABC, abstractmethod
from typing import Optional


class AbstractTransformer(ABC):
    @abstractmethod
    def transform(self, record):
        """Transform a single record (dict) and return the transformed record."""
        pass

    # --- Optional Flink hook (default passthrough) ---
    def apply_in_flink(self, t_env, source_table: str) -> Optional[str]:
        """
        If implemented, create a Flink view/table derived from source_table and return its name.
        Return None to signal 'use source_table as-is'.
        """
        return None
