from abc import ABC, abstractmethod
from typing import Optional


class AbstractExtractor(ABC):
    @abstractmethod
    def extract(self):
        """Yield or return data records (dicts/rows/events) from the source."""
        pass

    # --- Optional Flink hook (default no-op) ---
    def register_in_flink(self, t_env) -> Optional[str]:
        """
        If implemented, create/register a Flink source table and return its name.
        Non-Flink extractors can ignore this (return None).
        """
        return None
