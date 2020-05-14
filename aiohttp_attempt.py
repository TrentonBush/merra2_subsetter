import asyncio
import aiohttp
from pathlib import Path
from yarl import URL

from dotenv import load_dotenv
load_dotenv()
import os

# The following is an attempt to download MERRA data asynchronously, using aiohttp.
# The authentication() and login() functions seem to work, successfully storing 3 cookies.
# But when I try to use them in the actual download() GET request, I get redirected back to oauth.
# So I can't download anything.

# The multi-threaded requests-based downloader, which works, seems to have the exact same cookies at that point.
# It picks up a session cookie when the first download is successful.
# This matches what I see in Chrome's network log when I download manually.

# The download() GET request is identical to the authentication() GET request, except for the cookie.

TEST_URL = "https://goldsmr4.gesdisc.eosdis.nasa.gov/opendap/MERRA2/M2T1NXSLV.5.12.4/2020/03/MERRA2_400.tavg1_2d_slv_Nx.20200331.nc4.nc4?PS[0:23][232:254][117:139],T10M[0:23][232:254][117:139],U50M[0:23][232:254][117:139],V50M[0:23][232:254][117:139],time,lat[232:254],lon[117:139]"
auth=aiohttp.BasicAuth(os.getenv("MERRA2_USER"), os.getenv("MERRA2_PASS"))
cookie_jar = aiohttp.CookieJar()

CHROME_HEADERS={'Host' : 'goldsmr4.gesdisc.eosdis.nasa.gov',
'Connection' : 'keep-alive',
'Cache-Control' : 'max-age=0',
'DNT' : '1',
'Upgrade-Insecure-Requests' : '1',
'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
'Sec-Fetch-Site' : 'none',
'Sec-Fetch-Mode' : 'navigate',
'Sec-Fetch-User' : '?1',
'Sec-Fetch-Dest' : 'document',
'Accept-Encoding' : 'gzip, deflate, br',
'Accept-Language' : 'en-US,en;q=0.9'}

def set_cookies(response, cookie_jar, url):
    """aiohttp can't recieve cookies because NASA uses an obsolete date format in its cookies (timezone as -0000 instead of GMT). Failure point is python's http.cookies.BaseCookie.load() fails to read the old timezones. Unfortunately this means I can't follow redirects"""
    for hdr in response.headers.getall(aiohttp.hdrs.SET_COOKIE, ()):
        response.cookies.load(hdr.replace(' -0000', ' GMT'))
        cookie_jar.update_cookies(response.cookies, url)

async def login(session, url):
    r = await session.get(url, auth=auth, allow_redirects=False)
    set_cookies(r, cookie_jar, url)
    redirect_url = URL(r.headers.get(aiohttp.hdrs.LOCATION), encoded=not session._requote_redirect_url)
    r = await session.get(redirect_url, allow_redirects=False)
    set_cookies(r, cookie_jar, redirect_url)
    return URL(r.headers.get(aiohttp.hdrs.LOCATION), encoded=not session._requote_redirect_url)

def filename_from_url(url):
    return str(url).split("/")[-1].split(".nc4?")[0]

async def download(session, url, directory: Path, write_async=False, chunk_size=512 * 1024):
    fname = directory / filename_from_url(url)
    async with session.get(url, allow_redirects=False) as resp:
        set_cookies(resp, cookie_jar, url)
        with open(fname, "wb") as fd:
            while True:
                chunk = await resp.content.read(chunk_size)
                if not chunk:
                    break
                fd.write(chunk)
    print('downloaded')

async def authentication(session, url):
    async with session.get(url) as resp:
        if resp.status == 401:
            new_url = await login(session, resp.url)
            old_url = URL(url, encoded=not session._requote_redirect_url)
            if old_url == new_url:
                return new_url
            else:
                raise Exception(f'Login process has not returned original URL.\nOriginal: {str(old_url)}\nReturned: {str(new_url)}')

        else:
            return URL(url, encoded=not session._requote_redirect_url)

async def main():
    async with aiohttp.ClientSession(cookie_jar=cookie_jar) as client:
        repeat_url = await authentication(client, URL(TEST_URL))
        await download(client, repeat_url, Path('./'))

async def on_request_end(session, trace_config_ctx, params):
    print("\nEnding %s request for %s. I sent: %s" % (params.method, params.url, params.headers))
    print('\nSent headers: %s' % params.response.request_info.headers)

async def dl(url):
    #trace_config = aiohttp.TraceConfig()
    #trace_config.on_request_end.append(on_request_end)
    #async with aiohttp.ClientSession(cookie_jar=cookie_jar, trace_configs=[trace_config]) as client:
    async with aiohttp.ClientSession(cookie_jar=cookie_jar) as client:
        await download(client, first_url, Path(r'./'))

asyncio.run(main())
