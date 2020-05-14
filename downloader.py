import asyncio
import aiohttp
import aiofiles
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
import os


def filename_from_url(url):
    return url.split("/")[-1].split(".nc4?")[0]


async def download_async(
    session, directory: Path, url, write_async=False, chunk_size=512 * 1024
):
    fname = directory / filename_from_url(url)
    async with session.get(url) as resp:
        # assert resp.status == 200
        if write_async:  # try async write with aiofiles
            async with aiofiles.open(fname, "wb") as fd:
                while True:
                    chunk = await resp.content.read(chunk_size)
                    if not chunk:
                        break
                    await fd.write(chunk)
        else:  # synchronous write
            with open(fname, "wb") as fd:
                while True:
                    chunk = await resp.content.read(chunk_size)
                    if not chunk:
                        break
                    fd.write(chunk)


async def download_with_limit(sem, session, directory, url, **kwargs):
    async with sem:
        await download_async(session, directory, url, **kwargs)


async def login(session):
    data = aiohttp.FormData(
        {"username": os.getenv("MERRA2_USER"), "password": os.getenv("MERRA2_PASS")}
    )
    await session.post("https://urs.earthdata.nasa.gov/login", data=data)


async def main(urls, directory: Path, session, max_connects=8, **kwargs):
    # auth=aiohttp.BasicAuth(os.getenv("MERRA2_USER"), os.getenv("MERRA2_PASS"))

    sem = asyncio.Semaphore(max_connects)
    # aiohttp.ClientSession(auth=auth)
    async with session:
        return await asyncio.gather(
            *(
                download_with_limit(sem, session, directory, url, **kwargs)
                for url in urls
            )
        )
