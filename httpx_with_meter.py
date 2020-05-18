import httpx
import asyncio
import aiometer
import aiofiles
from pathlib import Path
from functools import partial
import datetime

from dotenv import load_dotenv
load_dotenv()
import os

# The following is a first attempt to use aiometer to throttle HTTPX requests.

TEST_URLS = [
    f"https://goldsmr4.gesdisc.eosdis.nasa.gov/opendap/MERRA2/M2T1NXSLV.5.12.4/2020/03/MERRA2_400.tavg1_2d_slv_Nx.202003{i}.nc4.nc4?PS[0:23][232:254][117:139],T10M[0:23][232:254][117:139],U50M[0:23][232:254][117:139],V50M[0:23][232:254][117:139],time,lat[232:254],lon[117:139]"
    for i in range(26,32)
]


def filename_from_url(url):
    return url.split("/")[-1].split(".nc4?")[0]

async def download(client, directory: Path, url):
    filename = directory / filename_from_url(url)
    async with client.stream('GET', url) as resp:
        resp.raise_for_status()
        
        async with aiofiles.open(filename, "wb") as f:
            async for data in resp.aiter_bytes():
                if data:
                    await f.write(data)
    print(f'Done with {filename.name} at {datetime.datetime.now().time()}')

async def main(urls: list, directory: Path):
    auth = (os.getenv("MERRA2_USER"), os.getenv("MERRA2_PASS"))
    async with httpx.AsyncClient(auth=auth, timeout=45) as client:
        await partial(download, client, directory)(urls[0])
        await aiometer.run_on_each(partial(download, client, directory), urls[1:], max_at_once=3, max_per_second=1)

asyncio.run(main(TEST_URLS, Path('./data/')))

# Success! Output for (max_at_once=3, max_per_second=1) is:
# Done with MERRA2_400.tavg1_2d_slv_Nx.20200326.nc4 at 22:16:55.308589
# Done with MERRA2_400.tavg1_2d_slv_Nx.20200327.nc4 at 22:17:02.676457
# Done with MERRA2_400.tavg1_2d_slv_Nx.20200328.nc4 at 22:17:04.062810
# Done with MERRA2_400.tavg1_2d_slv_Nx.20200329.nc4 at 22:17:05.333768
# Done with MERRA2_400.tavg1_2d_slv_Nx.20200330.nc4 at 22:17:10.259354
# Done with MERRA2_400.tavg1_2d_slv_Nx.20200331.nc4 at 22:17:12.098484