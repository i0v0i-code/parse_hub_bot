"""Owner-private archive authority, scoped to each async request/job.

The default is no authority (including inline requests). asyncio.to_thread
copies the context, so every WebDAV mutation checks the same principal.
"""
import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps


@dataclass(frozen=True, slots=True)
class ArchivePrincipal:
    requester_id: int
    chat_id: int
    chat_type: str


_current: ContextVar[ArchivePrincipal | None] = ContextVar('archive_principal', default=None)
BATCH_DENIED = '主页批量下载仅限主人私聊使用；单作品链接仍可正常解析。'


def owner_id():
    raw = os.getenv('WEBDAV_OWNER_TGID', '')
    return int(raw) if raw.isascii() and raw.isdigit() and int(raw) > 0 else None


def allowed(principal):
    owner = owner_id()
    return bool(owner is not None and isinstance(principal, ArchivePrincipal)
                and type(principal.requester_id) is int and type(principal.chat_id) is int
                and principal.requester_id == owner == principal.chat_id
                and principal.chat_type == 'private')


def message_principal(message):
    chat = getattr(message, 'chat', None)
    user = getattr(message, 'from_user', None)
    kind = getattr(chat, 'type', None)
    kind = getattr(kind, 'value', kind)
    principal = ArchivePrincipal(getattr(user, 'id', None), getattr(chat, 'id', None), kind)
    return principal if allowed(principal) else None


def job_principal(job):
    principal = ArchivePrincipal(job.get('requester_id'), job.get('chat_id'), job.get('request_chat_type'))
    return principal if allowed(principal) else None


def archive_allowed():
    return allowed(_current.get())


def require_archive():
    if not archive_allowed():
        raise PermissionError('WebDAV 保存仅限主人私聊')


@contextmanager
def archive_scope(principal):
    token = _current.set(principal if allowed(principal) else None)
    try:
        yield
    finally:
        _current.reset(token)


def message_archive_scope(fn):
    @wraps(fn)
    async def wrapped(req, *args, **kwargs):
        with archive_scope(message_principal(req.msg)):
            return await fn(req, *args, **kwargs)
    return wrapped
