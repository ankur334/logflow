from typing import List, Callable, Iterable, TypeVar

T = TypeVar("T")


class BatchAccumulator:
    def __init__(self, batch_size: int, on_flush: Callable[[List[T]], None]):
        self.batch_size = max(1, int(batch_size))
        self.on_flush = on_flush
        self._buf: List[T] = []

    def add(self, item: T):
        self._buf.append(item)
        if len(self._buf) >= self.batch_size:
            self.flush()

    def flush(self):
        if self._buf:
            self.on_flush(self._buf)
            self._buf.clear()

    def drain(self, it: Iterable[T]):
        for x in it:
            self.add(x)
        self.flush()
