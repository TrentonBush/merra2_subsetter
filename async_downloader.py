import asyncio
import httpx
import aiofiles
from pathlib import Path
from collections import defaultdict

from typing import Union, Callable, Iterable, Optional, Tuple

class AsyncDownloader(object):
    def __init__(self, directory: Union[str, Path], filename_from_url: Callable[[str], str]=None, auth: Optional[Tuple[str, str]]=None, merra: bool=True) -> None:
        self.directory = Path(directory)
        self.failed_downloads = defaultdict(list)
        self._auth = auth
        self._filename_from_url = filename_from_url
        if merra:
            self._merra_config()
        elif filename_from_url is None:
            raise ValueError('filename_from_url cannot be None if merra=False')
    
    def _merra_config(self) -> None:
        from dotenv import load_dotenv
        import os
        load_dotenv()
        self._auth = (os.getenv("MERRA2_USER"), os.getenv("MERRA2_PASS"))
        def merra2_file_from_url(url:str) -> str:
            return url.split("/")[-1].split(".nc4?")[0]
        self._filename_from_url = merra2_file_from_url


    def _get_filepath(self, url: str) -> Path:
        return self.directory / self._filename_from_url(url)


    async def _write_async(self, response: httpx.Response, filepath: Path) -> None:
        async with aiofiles.open(filepath, "wb") as f:
            async for chunk in response.aiter_bytes(): # httpx doesn't yet support chunk_size arg
                if chunk:
                    await f.write(chunk)


    async def _write(self, response: httpx.Response, filepath: Path) -> None:
        with open(filepath, "wb") as fd:
            async for chunk in response.aiter_bytes():
                if chunk:
                    fd.write(chunk)

    
    async def _download_url(self, session: httpx.AsyncClient, url: str, operation: Callable[[httpx.Response, Path], None]=None) -> None:
        if operation is None:
            operation = self._write_async
        filepath = self._get_filepath(url)
        try:
            async with session.stream('GET', url) as resp:
                try:
                    resp.raise_for_status()
                    await operation(resp, filepath)
                    return
                except httpx.HTTPError:
                    print(f'Status: {resp.status_code}\nURL: {url}\n')
                    self.failed_downloads[resp.status_code].append(url)
        except httpx.TimeoutException:
            print(f'Timeout\nURL: {url}\n')
            self.failed_downloads['timeout'].append(url)
        except httpx.TooManyRedirects:
            print(f'Too many redirects\nURL: {url}\n')
            self.failed_downloads['too_many_redirects'].append(url)

    
    async def _download_url_rate_limited(self, semaphore: asyncio.Semaphore, *args) -> None:
        async with semaphore:
            await self._download_url(*args)


    async def _retry(self, session: httpx.AsyncClient, semaphore: asyncio.Semaphore) -> None:
        # flatten
        urls = [url for sublist in self.failed_downloads.values() for url in sublist]
        # reset
        self.failed_downloads = defaultdict(list)
        tasks = (self._download_url_rate_limited(semaphore, session, url) for url in urls)
        await asyncio.gather(*tasks)

        if self.failed_downloads:
            print('After two tries, there were still failed downloads.')
            print('Failed URLs will be output to fails_X.txt')
            for reason, url_list in self.failed_downloads.items():
                print(f'Failures due to {reason}:\t\t{len(url_list)}')
            self._write_failures()
    

    def _write_failures(self) -> None:
        n = len(list(self.directory.glob('fails_*.txt')))
        filepath = self.directory / f'fails_{n}.txt'
        with open(filepath, 'w') as f:
            for reason, url_list in self.failed_downloads.items():
                print(f'Reason: {reason}', file=f)
                print(*url_list, sep='\n', file=f)
        return


    async def download(self, urls: Iterable[str], max_connects: int=8, retry=True) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        sem = asyncio.Semaphore(max_connects)

        # run first_url to completion before starting concurrent download
        # This ensures authentication and cookie gathering is done only once
        url_iterator = iter(urls) # makes compatible with lists and generators
        first_url = next(url_iterator)

        async with httpx.AsyncClient(auth=self._auth, timeout=20) as client:
            tasks = (self._download_url_rate_limited(sem, client, url) for url in url_iterator)
            await self._download_url(client, first_url)
            await asyncio.gather(*tasks)

            if retry and self.failed_downloads:
                await self._retry(client, sem)
