import merra_urls
from datetime import datetime


def test_lat_to_index_num():
    cases = [-90, -10.3, -10.2, -0.3, -0.2, 0, 0.2, 0.3, 10.2, 10.3, 90]
    index_output = [0, 159, 160, 179, 180, 180, 180, 181, 200, 201, 360]
    assert list(map(merra_urls.lat_to_index_num, cases)) == index_output


def test_lon_to_index_num():
    cases = [-180, -10.3, -10.2, -2, 0, 2, 179.6]
    index_output = [0, 272, 272, 285, 288, 291, 575]
    assert list(map(merra_urls.lon_to_index_num, cases)) == index_output


def test_url_generator():
    time_interval = (datetime(2020, 3, 31), datetime(2020, 4, 1))
    lat_interval = (-2, 2)
    lon_interval = (-2, 2)
    collections = [
        {
            "collection": "inst1_2d_lfo_Nx",
            "short_name": "M2I1NXLFO",
            "fields": ["PS", "SPEEDLML"],
        }
    ]
    out = list(
        merra_urls.url_generator(
            time_interval=time_interval,
            lat_interval=lat_interval,
            lon_interval=lon_interval,
            collections=collections,
        )
    )
    assert len(out) == 1
    assert (
        out[0]
        == "https://goldsmr4.gesdisc.eosdis.nasa.gov/opendap/MERRA2/M2I1NXLFO.5.12.4/2020/03/MERRA2_400.inst1_2d_lfo_Nx.20200331.nc4.nc4?PS[0:23][176:184][285:291],SPEEDLML[0:23][176:184][285:291],time,lat[176:184],lon[285:291]"
    )

