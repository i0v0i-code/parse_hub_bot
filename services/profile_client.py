import asyncio,json,os,re
from urllib.parse import urlsplit,parse_qs
import httpx
from parsehub.utils.helpers import UA,match_url


def identify_profile(url):
    parts=urlsplit(url);host=(parts.hostname or '').lower()
    if parts.scheme not in {'http','https'}:return None
    if host == 'space.bilibili.com':
        match=re.fullmatch(r'/([1-9][0-9]*)(?:/(?:upload/)?video)?/?',parts.path)
        if match:return 'bilibili',match[1],'https://space.bilibili.com/'+match[1]
    if host in {'www.xiaohongshu.com','xiaohongshu.com'}:
        match=re.fullmatch(r'/user/profile/([A-Fa-f0-9]{24})/?',parts.path)
        if match:return 'xhs',match[1],url
    if host in {'www.douyin.com','douyin.com','www.iesdouyin.com','iesdouyin.com'}:
        match=re.fullmatch(r'/(?:share/)?user/([A-Za-z0-9_-]+)/?',parts.path)
        if match:
            uid=parse_qs(parts.query).get('sec_uid',[match[1]])[0]
            if re.fullmatch(r'[A-Za-z0-9_-]+',uid):
                return 'douyin',uid,'https://www.douyin.com/user/'+uid
    return None


async def resolve_profile(text):
    url=match_url(text) or text
    if result:=identify_profile(url):return result
    host=urlsplit(url).hostname or ''
    if host not in {'v.douyin.com','xhslink.cn','xhslink.com','b23.tv'}:return None
    if host in {'xhslink.cn','xhslink.com'}:
        from services.xhs_links import resolve_share_url
        from parsehub.types import ParseError
        try:return identify_profile(await resolve_share_url(url))
        except ParseError:return None  # The normal parse handler reports the error.
    async with httpx.AsyncClient(timeout=20,follow_redirects=True,headers={'User-Agent':UA}) as client:
        response=await client.get(url)
        for hop in [*response.history,response]:
            if result:=identify_profile(str(hop.url)):return result
    return None


async def enumerate_profile(url):
    identity=identify_profile(url)
    if identity and identity[0]=='bilibili':
        from services.bilibili_space import enumerate_space
        return await enumerate_space(identity[1])
    socket=os.getenv('PROFILE_BROWSER_SOCKET','/run/parse-hub-browser/browser.sock')
    reader,writer=await asyncio.open_unix_connection(socket,limit=64*1024*1024)
    try:
        writer.write(json.dumps({'op':'enumerate','url':url}).encode()+b'\n');await writer.drain()
        raw=await asyncio.wait_for(reader.readline(),3700)
        result=json.loads(raw)
        if not result.get('ok'):raise RuntimeError(result.get('message','主页枚举失败'))
        return result['result']
    finally:
        writer.close();await writer.wait_closed()


async def enumerate_profile_stream(url,on_batch):
    """Receive bounded discovery batches while the browser keeps paginating.

    The final event is only a summary; item payloads are delivered through
    ``on_batch`` and are not duplicated in the terminal response.
    """
    identity=identify_profile(url)
    if identity and identity[0]=='bilibili':
        raise ValueError('B站主页使用应用内扫描器，不走浏览器流式协议')
    socket=os.getenv('PROFILE_BROWSER_SOCKET','/run/parse-hub-browser/browser.sock')
    reader,writer=await asyncio.open_unix_connection(socket,limit=64*1024*1024)
    try:
        writer.write(json.dumps({'op':'enumerate_stream','url':url,'ack_batches':True}).encode()+b'\n')
        await writer.drain()
        while True:
            raw=await asyncio.wait_for(reader.readline(),3700)
            if not raw:
                raise RuntimeError('主页枚举桥提前断开')
            message=json.loads(raw)
            if not message.get('ok'):
                raise RuntimeError(message.get('message','主页枚举失败'))
            event=message.get('event')
            result=message.get('result') or {}
            if event=='batch':
                if not isinstance(result,dict):
                    raise RuntimeError('主页枚举批次格式无效')
                await on_batch(result)
                writer.write(b'{"op":"ack"}\n')
                await writer.drain()
                continue
            if event in ('complete','done'):
                if not isinstance(result,dict):
                    raise RuntimeError('主页枚举结束结果格式无效')
                return result
            # Permit a one-line legacy terminal response during a rolling
            # browser-bridge restart; it contains no streaming batches.
            if event is None and 'result' in message:
                legacy_items=result.get('items') or []
                if legacy_items:
                    await on_batch(dict(result,items=legacy_items,complete=False,reason='legacy_terminal_batch'))
                return result
            raise RuntimeError('主页枚举桥返回未知事件')
    finally:
        writer.close();await writer.wait_closed()
