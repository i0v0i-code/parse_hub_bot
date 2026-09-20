import ast
import asyncio
import re
import tempfile
import time
import types
import unittest
from pathlib import Path

from test_scan_cache import ROOT, store_module


class ResumeTest(unittest.IsolatedAsyncioTestCase):
    async def test_archive_retry_uses_saved_enumeration_even_during_cooldown(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            store=store_module.ProfileStore(root/'state.sqlite3')
            self.addCleanup(store.close)
            job,_=store.enqueue('bilibili','1','https://space.bilibili.com/1',1,1)
            jid=job['id']
            store.upsert_profile('bilibili','1','u','1')
            store.merge_items('bilibili','1',[{'id':'video_1','url':'https://example.invalid'}])
            store.set_item('bilibili','1','video_1',download_status='succeeded',local_dir=directory)
            # Simulates a crash after scan completion but before job finalization.
            store.save_scan_cache(jid,'complete',dict(platform='bilibili',user_id='1',
                public_id='1',complete=True,pages=20,reason='all_lists_complete'))
            store.set_job(jid,status='running',enumeration_complete=False)
            counts={'archived':0,'messages':0}
            async def no_op(*args,**kwargs):pass
            async def message(*args,**kwargs):counts['messages']+=1
            sender=types.SimpleNamespace(text_no_preview=message,document=no_op)
            ns=dict(asyncio=asyncio,time=time,re=re,Path=Path,ProfileStore=store_module.ProfileStore,
                WebDavArchiveConfig=types.SimpleNamespace(from_env=lambda:object()),
                upload_report=no_op,user_folder=lambda *a:'folder',
                logger=types.SimpleNamespace(info=no_op,warning=no_op),
                CapacityDeferred=type('CapacityDeferred',(Exception,),{}),
                CollectionPending=type('CollectionPending',(Exception,),{}))
            tree=ast.parse((ROOT/'services/profile_jobs.py').read_text())
            nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef))]
            exec(compile(ast.Module(body=nodes,type_ignores=[]),'profile_jobs.py','exec'),ns)
            ns['cooldown_until']=lambda p:time.time()+3600
            manager=object.__new__(ns['ProfileManager'])
            manager.root=root;manager.store=store
            async def context(job):return sender,None
            async def archive(job,item,*args):
                counts['archived']+=1
                store.set_item('bilibili','1','video_1',status='complete',archive_status='succeeded',files=[{'name':'media'}])
                store.mark_delivered('bilibili','1','video_1',1,[123])
            manager._context=context;manager._item=archive
            # No services/core dependencies are installed in this test process:
            # entering the live enumeration branch would fail and queue the job.
            await asyncio.wait_for(manager._run_authorized(jid),2)
            current=store.job(jid)
            self.assertEqual(current['status'],'completed')
            self.assertTrue(current['enumeration_complete'])
            self.assertEqual(current['pages'],20)
            self.assertEqual(counts,{'archived':1,'messages':1})


if __name__=='__main__':unittest.main()
