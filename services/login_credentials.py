"""Immutable per-item credentials; atomic generations remain private."""
import json
from pathlib import Path
from contextvars import ContextVar
from contextlib import contextmanager

ROOT=Path('/app/data/login_cookies')
_current=ContextVar('item_login_snapshot',default={})

def load(platform):
    path=ROOT/(platform+'.json')
    if not path.exists():return None
    data=json.loads(path.read_text())
    if data.get('platform')!=platform:raise ValueError('credential_platform_mismatch')
    return data

def snapshot(platform):
    current=_current.get()
    return current[platform] if platform in current else load(platform)

@contextmanager
def item_scope(platform):
    token=_current.set({platform:load(platform)})
    try:yield
    finally:_current.reset(token)

def cookie_text(data):
    if data is None:return None
    return '; '.join(c['name']+'='+c['value'] for c in data['cookies'] if c['name'])

def fill_jar(jar,data):
    from http.cookiejar import Cookie
    for c in data['cookies']:
        domain=c['domain'];expiry=c.get('expires',-1)
        jar.set_cookie(Cookie(0,c['name'],c['value'],None,False,domain,True,domain.startswith('.'),c.get('path','/'),True,c.get('secure',False),int(expiry) if expiry>0 else None,expiry<=0,None,None,{'HttpOnly':c.get('httpOnly',False)},False))
