from abc import ABC, abstractmethod


class AbstractExtractor(ABC):
    @abstractmethod
    def extract(self):
        """Yield or return data records (dicts/rows/events) from the source."""
        pass
