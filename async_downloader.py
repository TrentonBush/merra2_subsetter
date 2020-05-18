import httpx
import aiometer
import aiofiles
from pathlib import Path
from collections import defaultdict

from typing import (
    Union,
    Callable,
    Iterable,
    Optional,
    Tuple,
    DefaultDict,
    List,
    Coroutine,
    Any,
)


def merra2_file_from_url(url: str) -> str:
    return url.split("/")[-1].split(".nc4?")[0]


class AsyncDownloader(object):
    def __init__(
        self,
        directory: Union[str, Path],
        auth: Optional[Tuple[str, str]] = None,
        merra: bool = True,
        max_at_once: int = 10,
        max_per_second: float = 3.0,
        timeout: float = 30.0,
        retries: int = 1,
    ) -> None:
        """Class to download many files concurrently. Motivated by reanalysis data.

        Parameters
        ----------
        directory : Union[str, Path]
            Path to output directory. Will be created if needed.
        filename_from_url : Optional[Callable[[str], str]], optional
            function to extract a filename from a URL string. Set by config. By default None
        auth : Optional[Tuple[str, str]], optional
            Tuple of (username, password), set by config. By default None
        merra : bool, optional
            If true, loads merra config. Otherwise ERA-5 (not yet implemented). By default True
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
        urls = (f'google.com/{i}' for i in range(100))
        downloader = AsyncDownloader(Path('./data/), max_at_once=5)
        asyncio.run(downloader.download(urls))
        """
        self.directory = Path(directory)
        self.failed_downloads: DefaultDict[Union[int, str], List[str]] = defaultdict(
            list
        )

        self._auth = auth
        self._filename_from_url: Callable[[str], str] = merra2_file_from_url
        self._max_at_once = max_at_once
        self._max_per_second = max_per_second
        self._timeout = timeout
        self._retries = retries

        self._client: Optional[httpx.AsyncClient] = None
        self._current_operation: Callable[
            [httpx.Response], Coroutine[Any, Any, Any]
        ] = self._write_async
        if merra:
            self._merra_config()

    def _merra_config(self) -> None:
        from dotenv import load_dotenv
        import os

        load_dotenv()
        self._auth = (os.getenv("MERRA2_USER"), os.getenv("MERRA2_PASS"))
        self._filename_from_url = merra2_file_from_url

    def _get_filepath(self, url: str) -> Path:
        return self.directory / self._filename_from_url(url)

    async def _write_async(self, response: httpx.Response) -> None:
        filepath = self._get_filepath(response.url.path)
        async with aiofiles.open(filepath, "wb") as f:
            async for chunk in response.aiter_bytes():  # httpx doesn't yet support chunk_size arg
                if chunk:
                    await f.write(chunk)

    async def _write(self, response: httpx.Response) -> None:
        filepath = self._get_filepath(response.url.path)
        with open(filepath, "wb") as fd:
            async for chunk in response.aiter_bytes():
                if chunk:
                    fd.write(chunk)

    async def _download_url(self, url: str) -> None:
        try:
            async with self._client.stream("GET", url) as resp:
                try:
                    resp.raise_for_status()
                    # TODO: this only works if self._current_operation is AWAITABLE. Not if Callable
                    await self._current_operation(resp)
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
        self._current_operation = self._write_async

        # run first_url to completion before starting concurrent download
        # This ensures authentication and cookie gathering is done only once
        url_iterator = iter(urls)  # makes compatible with lists and generators
        first_url = next(url_iterator)

        async with httpx.AsyncClient(auth=self._auth, timeout=self._timeout) as client:
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
