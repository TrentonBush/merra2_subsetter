import asyncio
from datetime import datetime
import merra_urls
import async_downloader
from pathlib import Path

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
            "collection": "inst1_2d_asm_Nx",
            "short_name": "M2I1NXASM",
            "fields": ["U50M", "V50M"],
        },
        {
            "collection": "tavg1_2d_flx_Nx",
            "short_name": "M2T1NXFLX",
            "fields": ["PRECTOTCORR", "RHOA", "RISFC", "Z0M"],
        },
        {
            "collection": "tavg1_2d_lnd_Nx",
            "short_name": "M2T1NXLND",
            "fields": ["GHLAND"],
        },
    ],
)

dl = async_downloader.AsyncDownloader(Path("~/c/data/merra_texas"))

asyncio.run(dl.download(urls))
