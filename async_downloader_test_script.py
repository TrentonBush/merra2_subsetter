import asyncio
from datetime import datetime
from pathlib import Path

import merra_urls
import async_downloader


urls = merra_urls.url_generator(
    time_interval=(datetime(2014, 1, 1), datetime(2019, 1, 1)),
    lat_interval=(26, 37),
    lon_interval=(-107, -93),
    collections=[
        {
            "collection": "tavg1_2d_slv_Nx",
            "short_name": "M2T1NXSLV",
            "fields": ["PS", "TS", "T10M", "U50M", "V50M"],
        },
        {
            "collection": "tavg1_2d_flx_Nx",
            "short_name": "M2T1NXFLX",
            "fields": ["PRECTOTCORR", "RHOA", "RISFC"],
        },
        {
            "collection": "tavg1_2d_lnd_Nx",
            "short_name": "M2T1NXLND",
            "fields": ["GHLAND"],
        },
    ],
)

dl = async_downloader.AsyncDownloader(Path("/mnt/c/data/merra_texas"))

asyncio.run(dl.download(urls))
