"""Bot-side bridge to the browser login ops over the root-only Unix socket.

Triggers a platform login in the authenticated browser, returns the QR PNG
bytes, and polls until the page shows logged-in.  Used by profile_jobs when
enumeration reports login_required/captcha_required.
"""
import asyncio
import base64
import json
import os
import time

SOCKET = os.getenv('PROFILE_RELOGIN_SOCKET', '/run/parse-hub-browser/relogin.sock')


async def _call(payload: dict, timeout: float = 120, socket: str | None = None) -> dict:
    reader, writer = await asyncio.open_unix_connection(socket or SOCKET, limit=64 * 1024 * 1024)
    try:
        writer.write(json.dumps(payload).encode() + b'\n')
        await writer.drain()
        raw = await asyncio.wait_for(reader.readline(), timeout)
        result = json.loads(raw)
        if not result.get('ok'):
            raise RuntimeError(result.get('message') or result.get('error') or '登录桥操作失败')
        return result['result']
    finally:
        writer.close()
        await writer.wait_closed()


async def start_login(platform: str, timeout: float = 120) -> dict:
    """Open the login dialog and return {session_id, status, qr (data URL), expires_at}."""
    if platform != 'bilibili':
        raise ValueError('仅保留 B站登录')
    return await _call({'op': 'login_start', 'platform': platform}, timeout)


async def login_status(session_id: str) -> dict:
    return await _call({'op': 'login_status', 'session_id': session_id}, timeout=60)


async def cancel_login(session_id: str) -> dict:
    return await _call({'op': 'login_cancel', 'session_id': session_id}, timeout=30)


async def refresh_login(session_id: str) -> dict:
    return await _call({'op': 'login_refresh', 'session_id': session_id}, timeout=120)


def qr_bytes(result: dict) -> bytes | None:
    """Extract PNG bytes from a login_start/status result's qr data URL."""
    qr = result.get('qr') or ''
    if qr.startswith('data:image/png;base64,'):
        return base64.b64decode(qr.split(',', 1)[1])
    return None


async def wait_logged_in(session_id: str, poll: float = 5.0, timeout: float = 300) -> dict:
    """Poll login_status until logged_in/expired/cancelled. Returns final status dict."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = await login_status(session_id)
        state = last.get('status', '')
        if state in ('logged_in', 'expired', 'cancelled', 'not_found'):
            return last
        await asyncio.sleep(poll)
    last = await login_status(session_id)
    if last.get('status') not in ('logged_in', 'expired', 'cancelled'):
        last = dict(last, status='timed_out')
    return last