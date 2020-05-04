from datetime import datetime
import merra_urls
import asyncio
import downloader
from pathlib import Path

urls = merra_urls.url_generator(
        time_interval=(datetime(2020, 3, 31), datetime(2020, 4, 1)),
        lat_interval=(26, 37),
        lon_interval=(-107, -93),
        collections=[{"collection": "tavg1_2d_slv_Nx",
        "short_name": "M2T1NXSLV",
        "fields": ["PS", "T10M", "U50M", "V50M"]}])

directory = Path.cwd() / "sample_downloads/"
if not directory.exists():
    Path.mkdir(Path.cwd() / "sample_downloads/")

asyncio.run(downloader.main(urls, directory))
