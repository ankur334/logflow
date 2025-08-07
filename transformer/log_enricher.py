import json, logging
from typing import Dict, Any, Optional
from .base_transformer import AbstractTransformer

log = logging.getLogger(__name__)


class LogPromoteTransformer(AbstractTransformer):
    """
    Promotes hot keys: attributes.msg/url/mobile and JSON body.data.mobile
    Keeps original attributes/resources/body.
    """

    def transform(self, r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        attrs = r.get("attributes") or {}
        body = r.get("body")
        mobile_from_body = None
        if isinstance(body, str):
            try:
                j = json.loads(body)
                mobile_from_body = (((j or {}).get("data") or {}).get("mobile"))
            except Exception:
                pass

        r["msg"] = attrs.get("msg")
        r["url"] = attrs.get("url")
        r["mobile"] = mobile_from_body or attrs.get("mobile")
        # Optional: drop if essential fields missing
        # if not r.get("timestamp"): return None
        return r
