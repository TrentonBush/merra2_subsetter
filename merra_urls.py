import datetime

BASE_URL = "https://goldsmr4.gesdisc.eosdis.nasa.gov/opendap/MERRA2/"


def url_generator(
    time_interval=(None, None),
    lat_interval=(-90, 90),
    lon_interval=(-180, 180),
    collections=None,
):
    # URLs have form root + short_name + const + date + stream + collection + date + const + field_params
    # collections: list of dicts {'collection': 'tavg1_2d_slv_Nx', 'short_name': 'M2T1NXSLV', 'fields': ['U50M', 'V50M']}
    hour_str = "[0:23]"
    lat_str = (
        f"[{lat_to_index_num(lat_interval[0])}:{lat_to_index_num(lat_interval[1])}]"
    )
    lon_str = (
        f"[{lon_to_index_num(lon_interval[0])}:{lon_to_index_num(lon_interval[1])}]"
    )
    for collection in collections:
        date = time_interval[0]
        date_inc = datetime.timedelta(days=1)
        param_str = f"{hour_str}{lat_str}{lon_str},"
        query_str = (
            param_str.join(collection["fields"])
            + f"{param_str}time,lat{lat_str},lon{lon_str}"
        )

        while date < time_interval[1]:
            short_name = collection["short_name"]
            year_month = date.strftime("%Y/%m")
            stream_num = production_stream(date.year)
            collec_name = collection["collection"]
            date_str = date.strftime("%Y%m%d")
            url = f"{BASE_URL}{short_name}.5.12.4/{year_month}/MERRA2_{stream_num}.{collec_name}.{date_str}.nc4.nc4?{query_str}"
            yield url
            date += date_inc


def lat_to_index_num(lat):
    """Input latitude in [-90, 90].
    MERRA-2 latitude is 0.5 degree resolution, indexed [0:360]"""
    if lat < -90 or lat > 90:
        raise ValueError(f"latitude outside bounds [-90, 90]; given {lat}")
    return round((lat + 90) / 0.5)


def lon_to_index_num(lon):
    """input longitude in [-180, 179.6875).
    MERRA-2 longitude is 0.625 degree resolution, indexed [0:575]"""
    if lon < -180 or lon >= 179.6875:  # 180 - (0.625 / 2); can't wrap
        raise ValueError(f"longitude outside bounds [-180, 179.6875); given {lon}")
    return round((lon + 180) / 0.625)


def production_stream(year):
    """MERRA-2 is/was produced in 4 batches ('streams'), depending on timeframe"""
    if year < 1980 or year > datetime.datetime.now().year:
        raise ValueError(
            f"Year {year} out of range. must be in [1980, {datetime.datetime.now().year}]"
        )
    elif year < 1992:
        return "100"
    elif year < 2001:
        return "200"
    elif year < 2011:
        return "300"
    else:
        return "400"
