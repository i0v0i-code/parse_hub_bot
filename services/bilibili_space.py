"""Public Bilibili uploads + seasons/series; one paced API stream, all CIDs.

Successful expansions and completed lists are persisted per job. An unfinished
list is read again to account for insertions/removals, but its known videos need
no further detail requests. A new job starts a fresh metadata scan.
"""
import asyncio
import re
import json

# /view: a removed/private video is an item failure, not an access challenge.
UNAVAILABLE_VIDEO_CODES = {-404, -403, -400, 62002, 62012}
ACCESS_CODES = {-101, -111, -352, -412, 401, 403, 412, 429}


class SpaceAPIError(RuntimeError):
    def __init__(self, code):
        self.code=code
        super().__init__(f'B站接口暂不可用（{code}），本轮停止，稍后重试')


class Quiet:
    def debug(self,*args,**kwargs):pass
    info=warning=error=debug


class SpaceAPI:
    def __init__(self,uid):
        import yt_dlp
        from yt_dlp.extractor.bilibili import BilibiliSpaceVideoIE
        from core import pl_cfg
        from pathlib import Path
        from services.login_credentials import snapshot, fill_jar
        credentials=snapshot('bilibili')
        configured=pl_cfg.roll_cookie('bilibili')
        cookie=configured.get_secret_value() if configured and credentials is None else None
        if cookie and (not Path(cookie).is_file() or not Path(cookie).stat().st_size):
            raise ValueError('B站 Cookie 文件不可读或为空')
        self.y=yt_dlp.YoutubeDL({'cookiefile':cookie or None,'logger':Quiet(),'cachedir':False,'socket_timeout':30})
        if credentials is not None:fill_jar(self.y.cookiejar,credentials)
        self.y.cookiejar.filename=None
        self.y.params['cookiefile']=None  # close() must not save the read-only credential jar
        self.ie=BilibiliSpaceVideoIE(self.y)
        self.ie.initialize()
        self.uid=uid

    def _request(self,path,query,wbi):
        if wbi:query=self.ie._sign_wbi({**query,**self.ie._dm_params},self.uid)
        try:
            result=self.ie._download_json('https://api.bilibili.com'+path,self.uid,query=query,
                headers={'Referer':'https://space.bilibili.com/'+self.uid,'Origin':'https://space.bilibili.com'})
        except Exception as exc:
            raise SpaceAPIError(getattr(getattr(exc,'cause',None),'status',type(exc).__name__)) from None
        if result.get('code')!=0:raise SpaceAPIError(result.get('code','invalid_response'))
        if not isinstance(result.get('data'),dict):raise SpaceAPIError('invalid_data')
        return result['data']

    async def __call__(self,path,query,wbi=False):
        from services import platform_rate_limit as rate
        async with rate.lock('bilibili'):
            await rate.wait_turn('bilibili')
            try:
                return await asyncio.to_thread(self._request,path,query,wbi)
            except SpaceAPIError as exc:
                if exc.code in ACCESS_CODES:rate.block('bilibili', str(exc.code))
                raise


def expand_parts(bvid,detail,memberships):
    pages=detail.get('pages')
    if not isinstance(pages,list) or not pages:raise SpaceAPIError('missing_pages')
    rows=[];seen=set()
    for p in pages:
        cid=str(p['cid']);page=int(p['page'])
        if not cid.isdigit() or page<1 or cid in seen:raise SpaceAPIError('invalid_pages')
        seen.add(cid)
        url=f'https://www.bilibili.com/video/{bvid}?p={page}'
        rows.append(dict(id=f'{bvid}_{cid}',bvid=bvid,cid=cid,page=page,url=url,canonical_url=url,
            title=f'{detail.get("title",bvid)}｜P{page:03d} {p.get("part","")}',is_video=True,
            owner_uid=str((detail.get('owner') or {}).get('mid','')),memberships=sorted(memberships),unavailable_reason=''))
    return rows


class SpaceScanner:
    def __init__(self,uid,request,checkpoint=None,*,cache_get=None,cache_put=None):
        self.uid=str(uid);self.request=request;self.checkpoint=checkpoint
        self.result=dict(platform='bilibili',user_id=self.uid,public_id=self.uid,numeric_uid=self.uid,
            username=self.uid,items=[],collections=[],complete=False,reason='',pages=0,receipts=[])
        self.videos={};self.collections={}
        self.cache_get=cache_get or (lambda key:None)
        self.cache_put=cache_put or (lambda key,data:None)
        self.expanded={}

    async def save(self):
        self.result['collections']=list(self.collections.values())
        if self.checkpoint:await self.checkpoint(self.result)

    def add_video(self,row,membership=None):
        bvid=row.get('bvid','')
        if not re.fullmatch(r'BV[A-Za-z0-9]{10}',bvid):raise SpaceAPIError('invalid_bvid')
        entry=self.videos.setdefault(bvid,dict(row=row,memberships=set()))
        if membership:entry['memberships'].add(membership)

    def add_collection(self,kind,meta):
        cid=str(meta.get(kind+'_id',meta.get('id','')))
        if not cid.isdigit():raise SpaceAPIError('invalid_collection_id')
        key=kind+'_'+cid
        self.collections.setdefault(key,dict(id=key,kind=kind,collection_id=cid,name=str(meta.get('name') or cid)))
        return key

    async def pages(self,path,query,page_key,extract,wbi=False):
        # Only an actual empty page commits a list snapshot. Never trust a saved
        # numeric offset in a still-changing list: deletion can shift unseen rows
        # backwards across that offset. Re-reading lists is cheap; /view is not.
        key='list:'+json.dumps([path,query,page_key],sort_keys=True,separators=(',',':'))
        committed=self.cache_get(key)
        if committed is not None:
            for batch in committed['pages']:
                self.result['pages']+=1
                self.result['receipts'].append(batch['receipt'])
                if batch['rows']:yield batch['rows']
                await self.save()
            return
        batches=[]
        seen=set()
        for page in range(1,10001):
            data=await self.request(path,{**query,page_key:page},wbi=wbi)
            rows=extract(data)
            if not isinstance(rows,list):raise SpaceAPIError('invalid_list')
            self.result['pages']+=1
            reported=data.get('page') or (data.get('items_lists') or {}).get('page') or {}
            receipt=dict(endpoint=path,page=page,count=len(rows),reported_page=reported)
            self.result['receipts'].append(receipt)
            batches.append(dict(rows=rows,receipt=receipt))
            if not rows:
                await self.save()
                self.cache_put(key,dict(pages=batches))
                return
            # Non-progress/repeated non-empty pages are incomplete, never terminal.
            fingerprint=json.dumps(rows,sort_keys=True,ensure_ascii=False)
            if fingerprint in seen:raise SpaceAPIError('repeated_page')
            seen.add(fingerprint)
            yield rows
            await self.save()
        raise SpaceAPIError('page_limit')

    async def expand_video(self, bvid):
        entry=self.videos[bvid]
        existing=self.expanded.get(bvid)
        if existing:
            if all(item.get('memberships')==sorted(entry['memberships']) for item in existing):return
            for item in existing:item['memberships']=sorted(entry['memberships'])
            await self.save()
            return
        cached=self.cache_get('video:'+bvid)
        if cached is not None:
            rows=cached['items']
            for item in rows:item['memberships']=sorted(entry['memberships'])
            self.expanded[bvid]=rows
            self.result['items'].extend(rows)
            await self.save()
            return
        try:
            if entry['row'].get('is_lesson_video'):
                raise SpaceAPIError('course')
            detail=await self.request('/x/web-interface/view',dict(bvid=bvid))
            if detail.get('is_chargeable_season') or (detail.get('rights') or {}).get('ugc_pay'):
                raise SpaceAPIError('paid_exclusive')
            rows=expand_parts(bvid,detail,entry['memberships'])
        except SpaceAPIError as exc:
            if exc.code not in UNAVAILABLE_VIDEO_CODES | {'course','paid_exclusive'}:raise
            reason=f'不可用/课程/付费专属，未下载（{exc.code}）'
            rows=[dict(id=bvid+'_unavailable',bvid=bvid,url='https://www.bilibili.com/video/'+bvid,
                title=entry['row'].get('title',bvid),is_video=True,memberships=sorted(entry['memberships']),unavailable_reason=reason)]
        # Cache only a fully expanded response (all CIDs), never partial database
        # item records. Saving before delivery lets a crash safely replay it.
        self.cache_put('video:'+bvid,dict(items=rows))
        self.expanded[bvid]=rows
        self.result['items'].extend(rows)
        await self.save()

    async def run(self):
        try:
            async for rows in self.pages('/x/space/wbi/arc/search',dict(mid=self.uid,ps=30,order='pubdate',platform='web',
                    order_avoided='true',tid=0,keyword='',web_location='333.1387',special_type='',index=0),
                    'pn',lambda d:d['list']['vlist'],wbi=True):
                for row in rows:
                    if row.get('author') and self.result['username']==self.uid:self.result['username']=row['author']
                    meta=row.get('meta') or {}
                    if meta.get('attribute')==156:
                        self.add_collection('season',meta)
                    else:
                        self.add_video(row)
                        await self.expand_video(row['bvid'])
            async for rows in self.pages('/x/polymer/web-space/seasons_series_list',dict(mid=self.uid,page_size=20),
                    'page_num',lambda d:d['items_lists']['seasons_list']+d['items_lists']['series_list']):
                for row in rows:
                    meta=row['meta']
                    self.add_collection('season' if 'season_id' in meta else 'series',meta)
            for key,collection in list(self.collections.items()):
                kind=collection['kind'];cid=collection['collection_id']
                if kind=='season':
                    path='/x/polymer/web-space/seasons_archives_list';query=dict(mid=self.uid,season_id=cid,page_size=30);page_key='page_num'
                else:
                    path='/x/series/archives';query=dict(mid=self.uid,series_id=cid,ps=30);page_key='pn'
                async for rows in self.pages(path,query,page_key,lambda d:d['archives']):
                    for row in rows:
                        self.add_video(row,key)
                        await self.expand_video(row['bvid'])
            self.result.update(complete=True,reason='投稿、合集及列表均实际分页至空页；已展开所有可访问普通视频分P；不含课程/专属/动态')
        except (SpaceAPIError,KeyError,TypeError,ValueError) as exc:
            self.result.update(complete=False,reason=str(exc) if isinstance(exc,SpaceAPIError) else 'B站响应结构不符合预期，本轮未完成')
        await self.save()
        return self.result


async def enumerate_space(uid,checkpoint=None,*,cache_get=None,cache_put=None):
    if not re.fullmatch(r'[1-9][0-9]*',str(uid)):raise ValueError('B站 UID 无效')
    api=SpaceAPI(str(uid))
    try:return await SpaceScanner(uid,api,checkpoint,cache_get=cache_get,cache_put=cache_put).run()
    finally:api.y.close()
