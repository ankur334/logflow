from abc import ABC, abstractmethod
from typing import List, Dict, Any


class AbstractSink(ABC):
    @abstractmethod
    def write(self, records: List[Dict[str, Any]]):
        """Write an iterable of records to the destination sink."""
        pass

    @abstractmethod
    def flush(self):
        pass

    @abstractmethod
    def close(self):
        pass
