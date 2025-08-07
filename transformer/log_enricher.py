import json, logging
from datetime import datetime
from typing import Dict, Any, Optional
from .base_transformer import AbstractTransformer

log = logging.getLogger(__name__)


class LogPromoteTransformer(AbstractTransformer):
    """
    Promotes hot keys: attributes.msg/url/mobile and JSON body.data.mobile
    Keeps original attributes/resources/body.
    
    Optional timestamp validation for production environments.
    """

    def __init__(self, validate_timestamp: bool = False, require_mobile: bool = False):
        """
        Initialize the transformer with optional validation.
        
        Args:
            validate_timestamp: If True, drops records without valid timestamps
            require_mobile: If True, drops records without mobile numbers
        """
        self.validate_timestamp = validate_timestamp
        self.require_mobile = require_mobile
        self.processed_count = 0
        self.dropped_count = 0
        
    def _is_valid_timestamp(self, timestamp: str) -> bool:
        """
        Validate if timestamp is in expected ISO format.
        
        Args:
            timestamp: Timestamp string to validate
            
        Returns:
            True if timestamp is valid, False otherwise
        """
        if not timestamp or not isinstance(timestamp, str):
            return False
            
        # Check for basic ISO format patterns
        if len(timestamp) < 19:  # Minimum for YYYY-MM-DDTHH:MM:SS
            return False
            
        try:
            # Try to parse various ISO formats
            for fmt in [
                "%Y-%m-%dT%H:%M:%S.%fZ",  # 2025-08-07T06:30:45.123Z
                "%Y-%m-%dT%H:%M:%SZ",     # 2025-08-07T06:30:45Z
                "%Y-%m-%dT%H:%M:%S",      # 2025-08-07T06:30:45
                "%Y-%m-%d %H:%M:%S"       # 2025-08-07 06:30:45
            ]:
                try:
                    datetime.strptime(timestamp, fmt)
                    return True
                except ValueError:
                    continue
            return False
        except Exception:
            return False

    def transform(self, r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transform log record by promoting hot keys and applying validation.
        
        Args:
            r: Input log record
            
        Returns:
            Transformed record or None if validation fails
        """
        self.processed_count += 1
        
        # Extract attributes and body
        attrs = r.get("attributes") or {}
        body = r.get("body")
        
        # Extract mobile from JSON body if available
        mobile_from_body = None
        if isinstance(body, str):
            try:
                j = json.loads(body)
                mobile_from_body = (((j or {}).get("data") or {}).get("mobile"))
            except json.JSONDecodeError:
                log.debug("Failed to parse JSON body, continuing without mobile extraction")
            except Exception as e:
                log.warning(f"Unexpected error parsing JSON body: {e}")

        # Promote hot keys to top level
        r["msg"] = attrs.get("msg")
        r["url"] = attrs.get("url")
        r["mobile"] = mobile_from_body or attrs.get("mobile")
        
        # Apply validation if enabled
        if self.validate_timestamp:
            timestamp = r.get("timestamp")
            if not self._is_valid_timestamp(timestamp):
                log.debug(f"Dropping record {self.processed_count}: invalid timestamp '{timestamp}'")
                self.dropped_count += 1
                return None
                
        if self.require_mobile:
            mobile = r.get("mobile")
            if not mobile or not str(mobile).strip():
                log.debug(f"Dropping record {self.processed_count}: missing mobile number")
                self.dropped_count += 1
                return None
        
        # Log progress periodically
        if self.processed_count % 1000 == 0:
            log.info(f"Processed {self.processed_count} records, dropped {self.dropped_count}")
            
        return r
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get transformation statistics.
        
        Returns:
            Dictionary with processing statistics
        """
        return {
            "processed_count": self.processed_count,
            "dropped_count": self.dropped_count,
            "success_rate": (self.processed_count - self.dropped_count) / max(1, self.processed_count)
        }
