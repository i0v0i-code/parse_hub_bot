import re
from urllib.parse import urlsplit
import httpx
from parsehub.utils.helpers import UA,match_url


def identify_profile(url):
    parts=urlsplit(url);host=(parts.hostname or '').lower()
    if parts.scheme not in {'http','https'}:return None
    if host == 'space.bilibili.com':
        match=re.fullmatch(r'/([1-9][0-9]*)(?:/(?:upload/)?video)?/?',parts.path)
        if match:return 'bilibili',match[1],'https://space.bilibili.com/'+match[1]
    return None


async def resolve_profile(text):
    url=match_url(text) or text
    if result:=identify_profile(url):return result
    host=urlsplit(url).hostname or ''
    if host != 'b23.tv':return None
    async with httpx.AsyncClient(timeout=20,follow_redirects=True,headers={'User-Agent':UA}) as client:
        response=await client.get(url)
        for hop in [*response.history,response]:
            if result:=identify_profile(str(hop.url)):return result
    return None
