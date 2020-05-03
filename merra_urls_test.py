import merra_urls


def test_lat_to_index_num():
    cases = [-90, -10.3, -10.2, -0.3, -0.2, 0, 0.2, 0.3, 10.2, 10.3, 90]
    index_output = [0, 159, 160, 179, 180, 180, 180, 181, 200, 201, 360]
    assert list(map(merra_urls.lat_to_index_num, cases)) == index_output


def test_lon_to_index_num():
    cases = [-180, -10.3, -10.2, -2, 0, 2, 179.6]
    index_output = [0, 272, 272, 285, 288, 291, 575]
    assert list(map(merra_urls.lon_to_index_num, cases)) == index_output
