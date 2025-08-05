# config/config_context.py

from contextlib import contextmanager


@contextmanager
def config_context(config):
    """
    Context manager for using a config in a safe block.
    You can expand this to fetch/rotate secrets, handle temp files, etc.
    """
    try:
        # Setup (if needed)
        yield config
    finally:
        # Cleanup if needed (for future use)
        pass
