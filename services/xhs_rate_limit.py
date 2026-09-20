"""Pace XHS note requests and persist access-challenge cooldowns."""
import asyncio
import json
import os
import time
import math
from pathlib import Path

STATE = Path('/app/data/xhs-access-cooldown.json')
MIN_INTERVAL = 5.0
_lock = asyncio.Lock()
_last_attempt = 0.0


def read_state():
    try:
        return json.loads(STATE.read_text())
    except FileNotFoundError:
        return {}


def block(reason):
    old = read_state()
    level = min(int(old.get('level', 0)) + 1, 4)
    delay = min(1800 * 2 ** (level - 1), 21600)
    state = {'until': time.time() + delay, 'level': level, 'reason': reason, 'updated': time.time()}
    STATE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE.with_suffix('.tmp')
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(state, f)
    os.replace(tmp, STATE)
    return state


def succeeded():
    if STATE.exists():
        STATE.unlink()


def check_available():
    remaining = float(read_state().get('until', 0)) - time.time()
    if remaining > 0:
        raise RuntimeError(f'小红书因登录或访问验证暂时冷却，约 {math.ceil(remaining/60)} 分钟后可重试。请勿重复提交；主人可在私聊使用 /relogin 检查登录。')


async def wait_turn(*, wait_cooldown=True):
    global _last_attempt
    # Caller holds _lock for the entire ParseHub attempt, including redirects.
    while True:
        if not wait_cooldown:
            check_available()
        delay = max(float(read_state().get('until', 0)) - time.time(),
                    MIN_INTERVAL - (time.monotonic() - _last_attempt))
        if delay <= 0:
            _last_attempt = time.monotonic()
            return
        await asyncio.sleep(delay)


def is_access_error(error):
    return any(marker in str(error) for marker in (
        'XHS_ACCESS_CHALLENGE', 'XHS_LOGIN_REQUIRED',
        '小红书页面未返回笔记数据', '小红书未返回可见笔记',
    ))
