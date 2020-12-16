import httpx
import aiometer
import aiofiles
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv
import os

from typing import (
    Union,
    Iterable,
    Optional,
    DefaultDict,
    List,
)


def merra2_file_from_url(url: str) -> str:
    return url.split("/")[-1].split(".nc4?")[0]


class AsyncDownloader(object):
    def __init__(
        self,
        directory: Union[str, Path],
        max_at_once: int = 10,
        max_per_second: float = 3.0,
        timeout: float = 30.0,
        retries: int = 1,
    ) -> None:
        """Class to download many files concurrently. Hardcoded for MERRA-2 reanalysis data.

        Parameters
        ----------
        directory : Union[str, Path]
            Path to output directory. Will be created if needed.
        max_at_once : int, optional
            Max concurrent connections set by aiometer, by default 10
        max_per_second : float, optional
            Max rate of connections set by aiometer. This sleeps between function calls, so unless the function takes 0 seconds to execute, the actual rate will be slower. By default 3.0
        timeout : float, optional
            Max seconds to wait for server response, by default 30.0
        retries : int, optional
            How many times to retry failed connections, by default 1


        Example
        -------
        urls = (f'fake_MERRA_URL.com/{i}' for i in range(100))
        downloader = AsyncDownloader(Path('./data/), max_at_once=5)
        asyncio.run(downloader.download(urls))
        """
        self.directory = Path(directory)
        self.failed_downloads: DefaultDict[Union[int, str], List[str]] = defaultdict(
            list
        )
        self._max_at_once = max_at_once
        self._max_per_second = max_per_second
        self._timeout = timeout
        self._retries = retries

        self._client: Optional[httpx.AsyncClient] = None

        # merra config
        load_dotenv()
        self._auth = (os.getenv("MERRA2_USER"), os.getenv("MERRA2_PASS"))

    def _get_filepath(self, url: str) -> Path:
        return self.directory / merra2_file_from_url(url)

    async def _write_async(self, response: httpx.Response) -> None:
        filepath = self._get_filepath(response.url.path)
        async with aiofiles.open(filepath, "wb") as f:
            async for chunk in response.aiter_bytes():  # httpx doesn't yet support chunk_size arg
                if chunk:
                    await f.write(chunk)

    async def _download_url(self, url: str) -> None:
        try:
            async with self._client.stream("GET", url) as resp:
                try:
                    resp.raise_for_status()
                    await self._write_async(resp)
                    return
                except httpx.HTTPError:
                    print(f"Status: {resp.status_code}\nURL: {url}\n")
                    self.failed_downloads[resp.status_code].append(url)
        except httpx.TimeoutException:
            print(f"Timeout\nURL: {url}\n")
            self.failed_downloads["timeout"].append(url)
        except httpx.TooManyRedirects:
            print(f"Too many redirects\nURL: {url}\n")
            self.failed_downloads["too_many_redirects"].append(url)

    def _log_failures(self) -> None:
        n = len(list(self.directory.glob("fails_*.txt")))
        filepath = self.directory / f"fails_{n}.txt"
        with open(filepath, "w") as f:
            for reason, url_list in self.failed_downloads.items():
                print(f"Reason: {reason}", file=f)
                print(*url_list, sep="\n", file=f)
        return

    async def download(self, urls: Iterable[str]) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)

        # run first_url to completion before starting concurrent download
        # This ensures authentication and cookie gathering is done only once
        url_iterator = iter(urls)  # makes compatible with lists and generators
        first_url = next(url_iterator)

        async with httpx.AsyncClient(auth=self._auth, timeout=self._timeout) as client:  # type: ignore
            self._client = client
            await self._download_url(first_url)
            await aiometer.run_on_each(
                self._download_url,
                url_iterator,
                max_at_once=self._max_at_once,
                max_per_second=self._max_per_second,
            )

            for i in range(self._retries):
                if not self.failed_downloads:
                    return
                # flatten and reset
                retry_urls = [
                    url for sublist in self.failed_downloads.values() for url in sublist
                ]
                self.failed_downloads = defaultdict(list)
                await aiometer.run_on_each(
                    self._download_url,
                    retry_urls,
                    max_at_once=self._max_at_once,
                    max_per_second=self._max_per_second,
                )

            if self.failed_downloads:
                print(
                    f"After {self._retries + 1} tries, there were still failed downloads."
                )
                for reason, url_list in self.failed_downloads.items():
                    print(f"{len(url_list)}\tfailures due to: {reason}")
                print("Failed URLs will be output to fails_X.txt")
                self._log_failures()
