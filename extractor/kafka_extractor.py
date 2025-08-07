from __future__ import annotations
import json, time, logging
from typing import Dict, Any, Iterable, Optional
from confluent_kafka import Consumer, KafkaException, KafkaError
from .base_extractor import AbstractExtractor

log = logging.getLogger(__name__)


class KafkaExtractor(AbstractExtractor):
    def __init__(
            self,
            kafka_config: Dict[str, Any],
            topic: str = "last9Topic",
            group_id: str = "py-pipeline-consumer",
            auto_offset_reset: str = "earliest",
            poll_timeout_s: float = 1.0,
            enable_auto_commit: bool = False,
            commit_every_n: int = 1000,
            commit_interval_s: float = 5.0,
            include_metadata: bool = False,
    ):
        conf = dict(kafka_config or {})
        conf["group.id"] = group_id
        conf["auto.offset.reset"] = auto_offset_reset
        conf["enable.auto.commit"] = enable_auto_commit

        self._c = Consumer(conf)
        self._topic = topic
        self._poll = poll_timeout_s
        self._auto = enable_auto_commit
        self._n = max(1, int(commit_every_n))
        self._secs = max(0.5, float(commit_interval_s))
        self._meta = include_metadata
        self._stopped = False

    def stop(self) -> None:
        self._stopped = True

    def close(self) -> None:
        try:
            self._c.close()
        except Exception:
            pass

    def _commit(self, async_=True):
        try:
            self._c.commit(asynchronous=async_)
        except Exception as e:
            log.warning("Commit failed: %s", e)

    def extract(self) -> Iterable[Dict[str, Any]]:
        self._c.subscribe([self._topic])
        in_batch, last_commit = 0, time.time()
        try:
            while not self._stopped:
                msg = self._c.poll(self._poll)
                if msg is None:
                    if not self._auto and in_batch and time.time() - last_commit >= self._secs:
                        self._commit()
                        in_batch, last_commit = 0, time.time()
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    rec = json.loads((msg.value() or b"").decode("utf-8"))
                    if not isinstance(rec, dict):
                        rec = {"value": rec}
                except Exception as e:
                    log.warning("Bad JSON; skipping: %s", e)
                    continue

                if self._meta:
                    ts_type, ts_val = msg.timestamp()
                    rec["_meta"] = {
                        "topic": msg.topic(), "partition": msg.partition(),
                        "offset": msg.offset(), "ts_type": ts_type, "ts": ts_val
                    }

                in_batch += 1
                if not self._auto and in_batch >= self._n:
                    self._commit()
                    in_batch, last_commit = 0, time.time()

                yield rec
        finally:
            if not self._auto and in_batch:
                self._commit()
            self.close()
