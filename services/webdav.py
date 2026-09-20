from __future__ import annotations

import asyncio
import base64
import os
import posixpath
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, BinaryIO, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from log import logger
from services.owner_policy import require_archive

logger = logger.bind(name="WebDavArchive")

_PLATFORM_FOLDERS = {
    "twitter": "X",
    "telegram": "Telegram",
    "youtube": "YouTube",
    "facebook": "Facebook",
    "instagram": "Instagram",
    "threads": "Threads",
    "bilibili": "Bilibili",
    "douyin": "抖音",
    "tiktok": "TikTok",
    "weibo": "微博",
    "xhs": "小红书",
    "tieba": "贴吧",
    "wechat": "微信公众号",
    "kuaishou": "快手",
    "coolapk": "酷安",
    "pipixia": "皮皮虾",
    "zuiyou": "最右",
    "xiaoheihe": "小黑盒",
    "snapchat": "Snapchat",
    "zhihu": "知乎",
}


@dataclass(frozen=True, slots=True)
class WebDavArchiveConfig:
    url: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> WebDavArchiveConfig | None:
        url = os.getenv("WEBDAV_URL", "").strip()
        user = os.getenv("WEBDAV_USER", "").strip()
        password = os.getenv("WEBDAV_PASS", "")
        if not url and not user and not password:
            return None
        if not url or not user or not password:
            raise ValueError("WEBDAV_URL、WEBDAV_USER、WEBDAV_PASS 必须同时配置")
        return cls(url.rstrip("/"), user, password)


def platform_folder(platform_id: str) -> str:
    if platform_id in _PLATFORM_FOLDERS:
        return _PLATFORM_FOLDERS[platform_id]
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", platform_id).strip("-.")
    return safe or "Other"


def _archive_files(output_dir: Path) -> list[Path]:
    root = output_dir.resolve()
    files: list[Path] = []
    for p in output_dir.rglob("*"):
        try:
            resolved = p.resolve()
            resolved.relative_to(root)
        except (OSError, ValueError):
            continue
        if not resolved.is_file():
            continue
        if "processed" in p.relative_to(output_dir).parts:
            continue
        files.append(p)
    return sorted(files)


def _remote_url(base: str, relative: str, *, directory: bool = False) -> str:
    parts = urlsplit(base)
    had_trailing_slash = relative.endswith("/")
    path = parts.path.rstrip("/") + "/" + quote(relative.strip("/"), safe="/-._~")
    if (directory or had_trailing_slash) and not path.endswith("/"):
        path += "/"
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def _request(
    config: WebDavArchiveConfig,
    method: str,
    relative: str,
    *,
    data: bytes | BinaryIO | None = None,
    content_length: int | None = None,
) -> tuple[int, bytes]:
    if method.upper() not in {'GET', 'HEAD', 'OPTIONS', 'PROPFIND'}:
        require_archive()
    url = _remote_url(config.url, relative, directory=method == "MKCOL")
    token = base64.b64encode(f"{config.user}:{config.password}".encode()).decode()
    headers = {"Authorization": f"Basic {token}", "User-Agent": "parse-hub-bot/1.0"}
    if method == "PROPFIND":
        headers["Depth"] = "0"
    if data is not None:
        if content_length is None:
            if not isinstance(data, bytes):
                raise ValueError("流式 WebDAV PUT 必须提供 Content-Length")
            content_length = len(data)
        headers["Content-Type"] = "application/octet-stream"
        headers["Content-Length"] = str(content_length)
    request = Request(url, data=cast(Any, data), headers=headers, method=method)
    try:
        with urlopen(request, timeout=120) as response:
            return response.status, response.read()
    except HTTPError as error:
        return error.code, error.read()
    except URLError as error:
        raise RuntimeError(f"WebDAV {method} 连接失败: {error.reason}") from error


def _ensure_directory(config: WebDavArchiveConfig, relative: str) -> None:
    status, _ = _request(config, "MKCOL", relative)
    if status in {201, 405}:
        return
    if status == 403:
        probe_status, _ = _request(config, "PROPFIND", relative.rstrip("/") + "/")
        if probe_status == 207:
            return
    if status not in {201, 405}:
        raise RuntimeError(f"WebDAV MKCOL 失败: status={status} path={relative}")


def _remote_size(config: WebDavArchiveConfig, relative: str) -> int:
    status, body = _request(config, "PROPFIND", relative)
    if status != 207:
        raise RuntimeError(f"WebDAV PROPFIND 失败: status={status} path={relative}")
    try:
        root = ET.fromstring(body)
        node = root.find(".//{DAV:}getcontentlength")
        return int(node.text or "-1") if node is not None else -1
    except (ET.ParseError, ValueError) as error:
        raise RuntimeError(f"WebDAV PROPFIND 响应无有效大小: path={relative}") from error


def _upload_sync(
    output_dir: Path,
    platform_id: str,
    raw_url: str,
    config: WebDavArchiveConfig,
    now: datetime,
) -> list[str]:
    files = _archive_files(output_dir)
    if not files:
        return []
    folder = platform_folder(platform_id)
    local_now = now.astimezone(timezone(timedelta(hours=8)))
    month = local_now.strftime("%Y%m")
    stamp = local_now.strftime("%d%H%M%S")
    _ensure_directory(config, folder)
    _ensure_directory(config, f"{folder}/{month}")

    uploaded: list[str] = []
    for path in files:
        remote = posixpath.join(folder, month, f"{stamp}_{path.name}")
        local_size = path.stat().st_size
        with path.open("rb") as stream:
            status, _ = _request(config, "PUT", remote, data=stream, content_length=local_size)
        if status not in {200, 201, 204}:
            raise RuntimeError(f"WebDAV PUT 失败: status={status} path={remote}")

        uploaded.append(remote)
    return uploaded


async def archive_output(
    output_dir: Path,
    *,
    platform_id: str,
    raw_url: str,
    config: WebDavArchiveConfig,
    now: datetime | None = None,
) -> list[str]:
    uploaded = await asyncio.to_thread(
        _upload_sync,
        output_dir,
        platform_id,
        raw_url,
        config,
        now or datetime.now(UTC),
    )
    logger.info(f"WebDAV 归档完成: platform={platform_id}, files={len(uploaded)}")
    return uploaded
