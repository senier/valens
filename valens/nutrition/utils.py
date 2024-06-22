import zlib
from collections.abc import Generator
from typing import Optional

import requests
import requests.adapters
import urllib3.util


class DownloadError(Exception):
    pass


def download(
    url: str,
    chunk_size: int = 1024 * 1024,
) -> Generator[tuple[bytes, Optional[float]], None, None]:

    session = requests.Session()

    adapter = requests.adapters.HTTPAdapter(
        max_retries=urllib3.util.Retry(
            total=50,
            redirect=5,
            status_forcelist=[413, 429, 500, 502, 503, 504],
            backoff_factor=1,
        )
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    response = session.head(url)

    if "Content-Length" not in response.headers:
        raise DownloadError("No content length found")

    length = int(response.headers["Content-Length"])
    position = 0

    while position < length:
        response = session.get(
            url, headers={"Range": f"bytes={position}-{position + chunk_size - 1}"}
        )
        response.raise_for_status()

        position += len(response.content)
        progress = position / length if length else None

        yield response.content, progress


def decompress_by_line(
    download: Generator[tuple[bytes, Optional[float]], None, None]
) -> Generator[tuple[bytes, Optional[float]], None, None]:

    partial_input: bytes = b""
    partial_output: Optional[bytes] = None

    decomp = zlib.decompressobj(32 + zlib.MAX_WBITS)

    for chunk, progress in download:
        # 42 seems to be the minimum input block size
        if len(partial_input) + len(chunk) < 42:
            partial_input = partial_input + chunk
            continue

        result = decomp.decompress(partial_input + chunk)
        partial_input = b""

        lines = result.split(b"\n")
        if len(lines) > 1:
            yield (lines[0] if partial_output is None else partial_output + lines[0]), progress
            partial_output = None

        partial_output = lines[-1] if partial_output is None else partial_output + lines[-1]
        for line in lines[1:-1]:
            yield line, progress

    if len(partial_input) > 0:
        lines = decomp.decompress(partial_input).split(b"\n")
        if len(lines) > 1:
            yield lines[0] if partial_output is None else partial_output + lines[0], progress
            partial_output = None
        partial_output = lines[-1] if partial_output is None else partial_output + lines[-1]
        for line in lines[1:-1]:
            yield line, progress

    assert partial_output is not None
    yield partial_output, progress
