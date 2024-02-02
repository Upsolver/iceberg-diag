from __future__ import annotations

import gzip
from abc import abstractmethod, ABC
from typing import Callable

from pyiceberg.io import InputStream

GZIP = "gzip"
UTF8 = 'utf-8'


class Compressor(ABC):
    @staticmethod
    def get_compressor(location: str) -> Compressor:
        return GzipCompressor() if location.endswith(".gz.metadata.json") else NOOP_COMPRESSOR

    @abstractmethod
    def stream_decompressor(self, inp: InputStream) -> InputStream:
        """Return a stream decompressor.

        Args:
            inp: The input stream that needs decompressing.

        Returns:
            The wrapped stream
        """

    @abstractmethod
    def bytes_compressor(self) -> Callable[[bytes], bytes]:
        """Return a function to compress bytes.

        Returns:
            A function that can be used to compress bytes.
        """


class NoopCompressor(Compressor):
    def stream_decompressor(self, inp: InputStream) -> InputStream:
        return inp

    def bytes_compressor(self) -> Callable[[bytes], bytes]:
        return lambda b: b


NOOP_COMPRESSOR = NoopCompressor()


class GzipCompressor(Compressor):
    def stream_decompressor(self, inp: InputStream) -> InputStream:
        return gzip.open(inp)

    def bytes_compressor(self) -> Callable[[bytes], bytes]:
        return gzip.compress


