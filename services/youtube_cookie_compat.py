"""yt-dlp cookie file compatibility hooks for YouTube and Bilibili.

ParseHub converts configured cookie strings into Netscape text, but that conversion
loses flags needed by exported browser cookies.  A platform cookie may therefore be
a read-only Netscape file path.  The file is read into memory and ParseHub's existing
0600 temporary-cookie mechanism passes the text to yt-dlp, so yt-dlp never writes the
read-only secret mount itself.

Enabled from the bot startup path by importing this module.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from parsehub.parsers.parser.bilibili import BiliYtParse
from parsehub.parsers.parser.youtube import YtbParse


def _read_cookie_file(cookie: dict[str, Any] | None) -> str | None:
    if not cookie or len(cookie) != 1:
        return None
    (key, value), = cookie.items()
    if value or not key.startswith("/"):
        return None
    path = Path(key)
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8")


def _youtube_cookie_text_with_file_support(self: YtbParse) -> str | None:
    cookie = self.cookie.get_value()
    if not cookie:
        return None
    if cookie_text := _read_cookie_file(cookie):
        return cookie_text
    return self.to_netscape_cookie(cookie, "youtube.com")


def _bilibili_cookie_text_with_file_support(self: BiliYtParse) -> str | None:
    return _read_cookie_file(self.cookie.get_value())


def apply() -> None:
    """Install both cookie-file hooks idempotently."""
    if getattr(YtbParse, "get_cookie_text") is not _youtube_cookie_text_with_file_support:
        YtbParse.get_cookie_text = _youtube_cookie_text_with_file_support  # type: ignore[method-assign]
    if getattr(BiliYtParse, "get_cookie_text") is not _bilibili_cookie_text_with_file_support:
        BiliYtParse.get_cookie_text = _bilibili_cookie_text_with_file_support  # type: ignore[method-assign]


apply()
