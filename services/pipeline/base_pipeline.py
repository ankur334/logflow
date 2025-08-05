from abc import ABC, abstractmethod


class AbstractPipeline(ABC):
    @abstractmethod
    def run(self):
        """Run the full extract-transform-load (ETL) pipeline."""
        pass
