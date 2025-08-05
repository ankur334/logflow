from abc import ABC, abstractmethod


class AbstractSink(ABC):
    @abstractmethod
    def write(self, records):
        """Write an iterable of records to the destination sink."""
        pass
