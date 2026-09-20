"""Shared pacing for Bilibili API/extraction and Douyin extraction; durable cooldowns."""
import asyncio
import json
import os
import time
import math
import tempfile
from pathlib import Path

ROOT=Path('/app/data')
_locks={}
_last={}

def lock(platform):
    return _locks.setdefault(platform,asyncio.Lock())

def read_state(platform):
    try:return json.loads((ROOT/(platform+'-access-cooldown.json')).read_text())
    except FileNotFoundError:return {}

def block(platform,reason):
    old=read_state(platform)
    level=min(int(old.get('level',0))+1,4)
    state=dict(until=time.time()+1800*2**(level-1),level=level,reason=reason,updated=time.time())
    path=ROOT/(platform+'-access-cooldown.json')
    fd,tmp=tempfile.mkstemp(prefix=platform+'-cooldown-',suffix='.tmp',dir=ROOT)
    with os.fdopen(fd,'w') as f:json.dump(state,f)
    os.replace(tmp,path)
    return state

def succeeded(platform):
    (ROOT/(platform+'-access-cooldown.json')).unlink(missing_ok=True)

def check_available(platform):
    remaining=float(read_state(platform).get('until',0))-time.time()
    if remaining>0:
        label={'bilibili':'B站','douyin':'抖音'}.get(platform,platform)
        raise RuntimeError(f'{label}因登录或访问限制暂时冷却，约 {math.ceil(remaining/60)} 分钟后可重试，请勿重复提交。')

async def wait_turn(platform, *, wait_cooldown=True):
    while True:
        if not wait_cooldown:check_available(platform)
        delay=max(float(read_state(platform).get('until',0))-time.time(),5-(time.monotonic()-_last.get(platform,0)))
        if delay<=0:
            _last[platform]=time.monotonic();return
        await asyncio.sleep(delay)

def access_error(error):
    texts=[]
    for _ in range(8):
        if error is None:break
        message=str(error).lower()
        if '最高可用画质解析失败' not in message:texts.append(message)
        error=error.__cause__ or error.__context__
    return any(marker in ' '.join(texts) for marker in (
        'captcha','登录','cookie','empty body','-412','-352','429','412','403','风控','验证',
    ))
