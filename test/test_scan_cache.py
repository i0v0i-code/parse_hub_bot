import asyncio
import importlib.util
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


space=load('space',ROOT/'services/bilibili_space.py')
store_module=load('store',ROOT/'services/profile_store.py')


def bv(n):return f'BV{n:010d}'


class API:
    def __init__(self):
        self.uploads=[bv(1),bv(2),bv(3)]
        self.collections={10:[bv(1),bv(4)],20:[bv(2),bv(5)]}
        self.parts={}
        self.failed=None
        self.calls=[]

    async def __call__(self,path,q,**kw):
        self.calls.append((path,dict(q)))
        if path.endswith('/view'):
            if q['bvid']==self.failed:raise space.SpaceAPIError(-412)
            return dict(pages=[dict(cid=100*int(q['bvid'][2:])+p,page=p)
                               for p in range(1,self.parts.get(q['bvid'],2)+1)])
        if path.endswith('arc/search'):
            start=(q['pn']-1)*2
            return {'list':{'vlist':[dict(bvid=b) for b in self.uploads[start:start+2]]}}
        if path.endswith('seasons_series_list'):
            rows=[{'meta':{'season_id':n,'name':str(n)}} for n in self.collections]
            return {'items_lists':{'seasons_list':rows if q['page_num']==1 else [],'series_list':[]}}
        return {'archives':[dict(bvid=b) for b in self.collections[int(q['season_id'])]] if q['page_num']==1 else []}


class Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'state.sqlite3'
        self.store=store_module.ProfileStore(self.path)
        self.api=API()
        self.job=self.new_job()

    async def asyncTearDown(self):self.store.close()

    def new_job(self):
        # Different requester/chat ensures two independent jobs for this fixture.
        jid=len(self.store.db.execute('select id from jobs').fetchall())+1
        return self.store.enqueue('bilibili','1','https://space.bilibili.com/1',jid,1)[0]['id']

    async def scan(self,checkpoint=None):
        async def ingest(result):
            self.store.upsert_profile('bilibili','1','1','1')
            self.store.merge_collections('bilibili','1',result['collections'])
            self.store.merge_items('bilibili','1',result['items'])
            if checkpoint:await checkpoint(result)
        return await space.SpaceScanner('1',self.api,ingest,
            cache_get=lambda key:self.store.scan_cache(self.job,key),
            cache_put=lambda key,data:self.store.save_scan_cache(self.job,key,data)).run()

    async def test_restart_reuses_parts_but_rereads_changed_unfinished_list(self):
        self.api.failed=bv(3)
        first=await self.scan()
        self.assertFalse(first['complete'])
        self.store.close();self.store=store_module.ProfileStore(self.path)
        self.api.calls.clear();self.api.failed=None
        # An insertion and deletion move page boundaries during downtime.
        self.api.uploads=[bv(9),bv(1),bv(3)]
        result=await self.scan()
        self.assertTrue(result['complete'])
        requested=[q['bvid'] for p,q in self.api.calls if p.endswith('/view')]
        self.assertNotIn(bv(1),requested)
        self.assertNotIn(bv(2),requested)
        self.assertIn(bv(9),requested)
        self.assertIn(bv(3),requested)
        self.assertEqual(len([i for i in result['items'] if i['bvid']==bv(9)]),2)

    async def test_completed_lists_skip_network_and_replay_memberships(self):
        self.api.failed=bv(5)
        result=await self.scan()
        self.assertFalse(result['complete'])
        self.api.calls.clear();self.api.failed=None
        result=await self.scan()
        self.assertTrue(result['complete'])
        self.assertFalse(any(p.endswith('arc/search') or p.endswith('seasons_series_list') for p,q in self.api.calls))
        self.assertFalse(any(str(q.get('season_id'))=='10' for p,q in self.api.calls))
        self.assertEqual([q['bvid'] for p,q in self.api.calls if p.endswith('/view')],[bv(5)])
        rows=[i for i in result['items'] if i['bvid']==bv(1)]
        self.assertEqual(len(rows),2)
        self.assertTrue(all(i['memberships']==['season_10'] for i in rows))
        self.assertEqual(set(self.store.profile('bilibili','1')['collections']),{'season_10','season_20'})

    async def test_crash_after_expansion_is_replayed_before_list_commit(self):
        async def crash(result):
            if result['items']:raise asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):await self.scan(crash)
        self.store.close();self.store=store_module.ProfileStore(self.path)
        self.api.calls.clear()
        result=await self.scan()
        self.assertTrue(result['complete'])
        self.assertNotIn(bv(1),[q['bvid'] for p,q in self.api.calls if p.endswith('/view')])
        self.assertEqual(len(result['items']),10)

    async def test_new_job_refreshes_changed_parts_and_new_uploads(self):
        await self.scan()
        self.job=self.new_job();self.api.calls.clear()
        self.api.uploads.insert(0,bv(9));self.api.parts[bv(1)]=3
        result=await self.scan()
        self.assertTrue(result['complete'])
        self.assertIn(bv(1),[q['bvid'] for p,q in self.api.calls if p.endswith('/view')])
        self.assertEqual(len([i for i in result['items'] if i['bvid']==bv(1)]),3)
        self.assertTrue(any(i['bvid']==bv(9) for i in result['items']))

    async def test_failed_detail_is_not_cached(self):
        self.api.failed=bv(1)
        await self.scan()
        self.assertIsNone(self.store.scan_cache(self.job,'video:'+bv(1)))
        self.api.failed=None
        result=await self.scan()
        self.assertTrue(result['complete'])

    async def test_full_replay_uses_no_api_calls(self):
        await self.scan();self.api.calls.clear()
        result=await self.scan()
        self.assertTrue(result['complete'])
        self.assertEqual(self.api.calls,[])


if __name__=='__main__':unittest.main()
