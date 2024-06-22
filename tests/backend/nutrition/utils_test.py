import gzip
import random
from collections.abc import Generator, Iterator
from typing import Optional

import pytest
import requests_mock
import requests_mock.adapter

from valens.nutrition.utils import DownloadError, decompress_by_line, download


class Request:
    def __init__(self, data: bytes, level: int = 9) -> None:
        self._pos = 0
        self._chunk_size = 0
        self._content = gzip.compress(data, compresslevel=level)
        self.headers = {"Content-Length": len(self._content)}

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        if self._pos >= len(self._content):
            raise StopIteration
        actual_chunk_size = random.randint(0, min(self._chunk_size, len(self._content) - self._pos))
        result = self._content[self._pos : self._pos + actual_chunk_size]
        self._pos += actual_chunk_size
        return result

    def raise_for_status(self) -> None:
        pass

    def iter_content(self, chunk_size: int) -> Iterator[bytes]:
        self._chunk_size = chunk_size
        return self


@pytest.mark.parametrize(
    ("data"),
    [
        "",
        "short single line",
        1000 * "long single line",
        10000 * "very long single line",
        5 * "\n",
        1000 * "\n",
        10 * "few short lines\n",
        1000 * "many short lines\n",
        500 * "many short lines\n" + 500 * ((1000 * "many long lines") + "\n"),
        1000 * ((1000 * "many long lines") + "\n"),
    ],
    ids=range(10),
)
def test_decompress_by_line(data: str) -> None:

    def download(
        url: str,  # noqa: ARG001
        chunk_size: int = 1024 * 1024,  # noqa: ARG001
    ) -> Generator[tuple[bytes, Optional[float]], None, None]:
        yield gzip.compress(data.encode()), None

    assert [
        l for l, _ in decompress_by_line(download=download(url="dummy"))
    ] == data.encode().split(b"\n")


@pytest.mark.parametrize("chunk_size", [1, 100, 1000, 1024, 1000000, 1024 * 1024])
@pytest.mark.parametrize(
    ("data"),
    [
        "",
        "short single line",
        1000 * "long single line",
        10000 * "very long single line",
        5 * "\n",
        1000 * "\n",
        10 * "few short lines\n",
        1000 * "many short lines\n",
        500 * "many short lines\n" + 500 * ((1000 * "many long lines") + "\n"),
        1000 * ((1000 * "many long lines") + "\n"),
    ],
    ids=range(10),
)
def test_download(data: str, chunk_size: int) -> None:
    url = "http://example.com"
    with requests_mock.Mocker() as m:
        m.get(url, text=data)
        m.head(url, text=data, headers={"Content-Length": f"{len(data)}"})
        chunks = [data for data, _ in download(url=url, chunk_size=chunk_size)]
        assert b"".join(chunks).decode() == data


def test_download_no_content_length() -> None:
    url = "http://example.com"
    with requests_mock.Mocker() as m:
        m.head(url)
        with pytest.raises(DownloadError, match="^No content length found$"):
            list(download(url=url))
