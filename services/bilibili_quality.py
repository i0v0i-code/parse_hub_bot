"""Bilibili videos use authenticated yt-dlp, never the legacy 720p API.

Dynamic/image parsing stays upstream. Archival downloads retain the existing
unbounded selector; only the downstream Telegram derivative is size-bounded.
"""
from pathlib import Path

from parsehub.parsers.base.ytdlp import YtParser
from parsehub.parsers.parser.bilibili import BiliParse, BiliYtParse
from parsehub.types import ParseError


class BilibiliUnavailable(ParseError):
    pass


_original_do_parse = BiliParse._do_parse


async def _highest_available_video(self: BiliParse, raw_url: str):
    if await self.is_dynamic(raw_url):
        return await _original_do_parse(self, raw_url)
    try:
        return await self.ytp_parse(raw_url)
    except Exception as exc:
        detail=str(exc).lower()
        if 'supporter-only' in detail or '充电' in detail and '专属' in detail:
            raise BilibiliUnavailable('付费充电专属视频，当前账号无权访问，已跳过') from exc
        # Do not return a successful low-quality API result after auth/extraction
        # failure. Keep upstream details out of this user-visible error.
        raise ParseError('Bilibili 最高可用画质解析失败，请检查登录 Cookie 或稍后重试；未回退低清 API') from exc


def _authenticated_cookie_text(self: BiliYtParse) -> str | None:
    cookie = self.cookie.get_value()
    if not cookie:
        return None
    if len(cookie) == 1:
        (key, value), = cookie.items()
        if key.startswith('/') and not value:
            try:
                text = Path(key).read_text(encoding='utf-8')
            except OSError as exc:
                raise ParseError('Bilibili 登录 Cookie 文件不可读') from exc
            if not text.strip():
                raise ParseError('Bilibili 登录 Cookie 文件为空')
            return text
    return '# Netscape HTTP Cookie File\n' + ''.join(
        '.bilibili.com\tTRUE\t/\tTRUE\t0\t' + key + '\t' + value + '\n'
        for key, value in cookie.items()
        if not any(c in key + value for c in '\t\r\n')
    )


def _highest_parse_args(self: BiliYtParse) -> list[str]:
    return [*YtParser.cli_args.fget(self), '-f', 'bv*+ba/b', '-S', 'res,fps,hdr,vcodec:av01']


BiliParse._do_parse = _highest_available_video
BiliYtParse.get_cookie_text = _authenticated_cookie_text
BiliYtParse.cli_args = property(_highest_parse_args)

from services import bilibili_cdn  # measured CDN download routing
