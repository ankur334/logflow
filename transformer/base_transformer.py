from abc import ABC, abstractmethod


class AbstractTransformer(ABC):
    @abstractmethod
    def transform(self, record):
        """Transform a single record (dict) and return the transformed record."""
        pass
