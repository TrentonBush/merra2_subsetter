import asyncio
from datetime import datetime
import merra_urls
import async_downloader
from pathlib import Path

urls = merra_urls.url_generator(
    time_interval=(datetime(2020, 3, 1), datetime(2020, 3, 31)),
    lat_interval=(26, 37),
    lon_interval=(-107, -93),
    collections=[
        {
            "collection": "tavg1_2d_slv_Nx",
            "short_name": "M2T1NXSLV",
            "fields": ["PS", "T10M", "U50M", "V50M"],
        }
    ],
)

dl = async_downloader.AsyncDownloader(Path('./data/'))

asyncio.run(dl.download(urls))
