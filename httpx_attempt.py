import httpx
import asyncio
import aiofiles
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()
import os

# The following is a first attempt to download MERRA data asynchronously using HTTPX.

TEST_URL = "https://goldsmr4.gesdisc.eosdis.nasa.gov/opendap/MERRA2/M2T1NXSLV.5.12.4/2020/03/MERRA2_400.tavg1_2d_slv_Nx.20200331.nc4.nc4?PS[0:23][232:254][117:139],T10M[0:23][232:254][117:139],U50M[0:23][232:254][117:139],V50M[0:23][232:254][117:139],time,lat[232:254],lon[117:139]"

def filename_from_url(url):
    return url.split("/")[-1].split(".nc4?")[0]

async def download(session, url, directory: Path, write_async = True):
    filename = directory / filename_from_url(url)
    async with session.stream('GET', url) as resp:
        resp.raise_for_status()
        if write_async:  # testing out async write with aiofiles
            async with aiofiles.open(filename, "wb") as f:
                async for data in resp.aiter_bytes():
                    if data:
                        await f.write(data)
        else:  # synchronous write
            with open(filename, "wb") as fd:
                async for data in resp.aiter_bytes():
                    if data:
                        fd.write(chunk)
        
    print('downloaded')

async def main():
    auth = (os.getenv("MERRA2_USER"), os.getenv("MERRA2_PASS"))
    async with httpx.AsyncClient(auth=auth, timeout=15) as client:
        await download(client, TEST_URL, Path('./'))
        

asyncio.run(main())
