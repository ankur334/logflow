from abc import ABC

from services.transformer.base_transformer import AbstractTransformer


class LogEnricher(AbstractTransformer, ABC):
    def transform(self, record):
        """
        Enrich the log record with additional metadata.
        This is a placeholder implementation that adds a 'processed' key.
        """
        # Example enrichment logic
        record['processed'] = True
        return record
