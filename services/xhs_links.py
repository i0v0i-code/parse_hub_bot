"""Resolve the share redirect without visiting a note, login or captcha page."""
import re
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx
from parsehub.types import ParseError
from parsehub.utils.helpers import UA, match_url

SHORT_HOSTS = {'xhslink.cn', 'xhslink.com'}
NOTE_HOSTS = {'www.xiaohongshu.com', 'xiaohongshu.com'}


def note_url(url):
    parts = urlsplit(url)
    if parts.hostname not in NOTE_HOSTS:
        return url
    match = re.fullmatch(r'/(?:explore|discovery/item)/([a-fA-F0-9]{24})/?', parts.path)
    if match:
        return urlunsplit(('https', 'www.xiaohongshu.com', '/explore/'+match[1], parts.query, ''))
    return url


async def resolve_share_url(text, *, proxy=None, headers=None):
    url = match_url(text) or text
    if urlsplit(url).hostname not in SHORT_HOSTS:
        return note_url(url)
    async with httpx.AsyncClient(proxy=proxy, timeout=20, follow_redirects=False) as client:
        for _ in range(5):
            parts = urlsplit(url)
            if parts.scheme not in ('http', 'https') or parts.username or parts.password:
                raise ParseError('小红书分享链接跳转地址无效')
            if parts.hostname in NOTE_HOSTS:
                if re.fullmatch(r'/(?:explore|discovery/item|user/profile)/[a-fA-F0-9]{24}/?', parts.path):
                    return note_url(url)
                raise ParseError('小红书分享链接没有返回笔记或主页地址，请重新从 App 分享')
            if parts.hostname not in SHORT_HOSTS:
                raise ParseError('小红书分享链接跳转到了非小红书地址')
            try:
                response = await client.get(url, headers={'User-Agent': UA} if headers is None else headers)
                response.raise_for_status() if not response.is_redirect else None
            except httpx.HTTPError:
                raise ParseError('小红书分享链接跳转请求失败，请稍后重试') from None
            location = response.headers.get('location')
            if not response.is_redirect or not location:
                raise ParseError('小红书分享链接没有返回有效跳转，请重新从 App 分享')
            url = urljoin(url, location)
    raise ParseError('小红书分享链接跳转次数过多')
