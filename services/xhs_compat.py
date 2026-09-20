"""XHS authenticated redirects and selective SSR note extraction.

Credentials remain in a read-only browser export, never in platform config.
Only XHS methods are changed; no JavaScript from the remote page is executed.
"""
import json
import re
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

import httpx
from bs4 import BeautifulSoup
from parsehub.parsers.parser.xhs import XHSParser
from parsehub.provider_api.xhs import XHSAPI
from parsehub.utils.helpers import SecretCookie, UA
from parsehub.types import ParseError

_ORIGINAL_RAW = XHSParser.get_raw_url
_ORIGINAL_PARSE = XHSAPI._XHSAPI__parse
_JS_TOKEN = re.compile(r'"(?:\\.|[^"\\])*"|\bundefined\b')


def load_cookie(value):
    if value and len(value) == 1:
        key, val = next(iter(value.items()))
        if key.startswith('/') and not val:
            records = json.loads(Path(key).read_text())
            return {item['name']: item['value'] for item in records
                    if item['domain'].lstrip('.') == 'xiaohongshu.com'
                    or item['domain'].endswith('.xiaohongshu.com')}
    return value


async def get_raw_url(self, url, *, clean_all=False, headers=None):
    # SecretCookie deliberately accepts a configured file path as its lone key.
    self.cookie = SecretCookie(load_cookie(self.cookie.get_value()))
    from services.xhs_links import resolve_share_url
    url = await resolve_share_url(url, proxy=self.proxy, headers=headers)
    if 'undertake_note_error' in parse_qs(urlsplit(url).query):
        raise ParseError('小红书返回该内容暂时无法查看，请检查笔记权限或更新登录 Cookie')
    return await _ORIGINAL_RAW(self, url, clean_all=clean_all, headers=headers)


async def extract_data(html):
    soup = BeautifulSoup(html, 'lxml')
    script = next((s.text for s in soup.find_all('script')
                   if s.text.lstrip().startswith('window.__INITIAL_STATE__')), None)
    if script is None:
        raise ValueError('小红书页面未返回笔记数据，请检查登录状态或访问限制')
    # Only decode the note subtree. Unrelated state may contain JS constructors
    # (e.g. new Map([])); do not eval it or regex-rewrite user text.
    decoder = json.JSONDecoder()
    for match in re.finditer(r'"note"\s*:\s*(?=\{)', script):
        fragment = script[match.end():]
        fragment = _JS_TOKEN.sub(lambda m: 'null' if m[0] == 'undefined' else m[0], fragment)
        try:
            note, _ = decoder.raw_decode(fragment)
        except json.JSONDecodeError:
            continue
        if isinstance(note, dict) and 'firstNoteId' in note and 'noteDetailMap' in note:
            return {'note': note}
    raise ValueError('小红书页面笔记数据格式不兼容或内容不可见')


def parse_data(self, data):
    note = data.get('note') or {}
    first_id = note.get('firstNoteId')
    entry = (note.get('noteDetailMap') or {}).get(first_id) or {}
    if not first_id or not entry.get('note'):
        raise ValueError('小红书未返回可见笔记，请检查笔记权限或更新登录 Cookie')
    return _ORIGINAL_PARSE(self, data)


def select_stream(stream):
    # Preserve legacy preference; current SSR uses opaque codec group names.
    # EF4 was verified by ffprobe as H.264, EF5 as HEVC. SSR key order varies.
    for key in ('h264', 'EF4', 'av1', 'h265', 'EF5', 'h266'):
        if stream.get(key):
            return stream[key]
    for entries in stream.values():
        if isinstance(entries, list):
            usable = [item for item in entries if isinstance(item, dict) and item.get('masterUrl')]
            if usable:
                return usable
    return None


XHSAPI._XHSAPI__select_stream = staticmethod(select_stream)
XHSParser.get_raw_url = get_raw_url
XHSParser.__after_clean_parameters__ = ['xsec_token', 'xsec_source']
XHSAPI._XHSAPI__extract_data = staticmethod(extract_data)
XHSAPI._XHSAPI__parse = parse_data


async def fetch_html(self, url):
    # Do not silently treat a captcha/login redirect as an ordinary note page.
    async with httpx.AsyncClient(proxy=self.proxy, cookies=self.cookie,
                                 headers={'User-Agent': UA}, follow_redirects=False) as client:
        response = await client.get(url, timeout=30)
        location = urlsplit(response.headers.get('location', ''))
        if response.status_code in (403, 429, 461, 471) or 'captcha' in location.path:
            raise ParseError('XHS_ACCESS_CHALLENGE: 小红书要求访问验证，已停止连续重试')
        if 300 <= response.status_code < 400 and 'login' in location.path:
            raise ParseError('XHS_LOGIN_REQUIRED: 小红书没有接受当前登录状态，请在机器人主人私聊使用 /relogin 重新登录小红书')
        response.raise_for_status()
        return response.text


XHSAPI._XHSAPI__fetch_html = fetch_html
