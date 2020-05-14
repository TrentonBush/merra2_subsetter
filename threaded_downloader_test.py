from datetime import datetime
import merra_urls
import threaded_downloader

urls = merra_urls.url_generator(
    time_interval=(datetime(2019, 3, 1), datetime(2020, 3, 1)),
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

dl = threaded_downloader.DownloadManager()
dl.download_path = "./sample_downloads/"
dl.download_urls = list(urls)
dl.start_download()
