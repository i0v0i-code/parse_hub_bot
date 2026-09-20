import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from services.profile_store import ProfileStore
from services.profile_jobs import ProfileManager
from services import profile_jobs as jobs, profile_archive as archive


def test_upload_success_needs_no_readback(tmp_path, monkeypatch):
    (tmp_path/'video.mp4').write_bytes(b'video')
    monkeypatch.setattr(archive,'ensure_folders',lambda *a:None)
    monkeypatch.setattr(archive,'_request',lambda *a,**k:(201,b''))
    monkeypatch.setattr(archive,'_remote_size',lambda *a:pytest.fail('unexpected readback'))
    assert archive._upload(None,'folder','post',tmp_path)[0]['size']==5


def test_collection_failure_does_not_block_delivery(tmp_path,monkeypatch):
    async def run():
        m=ProfileManager.__new__(ProfileManager);m.root=tmp_path;m.send_locks={}
        m.store=ProfileStore(tmp_path/'state.sqlite3')
        m.store.upsert_profile('bilibili','123','test','123')
        m.store.merge_items('bilibili','123',[dict(id='BV1234567890_1',url='https://www.bilibili.com/video/BV1234567890?p=1',memberships=['season_1'])])
        p=tmp_path/'bilibili/123/files/BV1234567890_1/original.mp4';p.parent.mkdir(parents=True);p.write_bytes(b'123')
        files=[dict(name='archive.mp4',path='remote/archive.mp4',size=3,local_path=str(p),type='video')]
        m.store.set_item('bilibili','123','BV1234567890_1',archive_status='succeeded',download_status='succeeded',local_dir=str(p.parent),files=files)
        import services.profile_media as media
        import services.youtube_variants as variants
        monkeypatch.setattr(media,'from_records',lambda *a:SimpleNamespace(output_dir=p.parent))
        sent=AsyncMock(return_value=[SimpleNamespace(id=55)])
        monkeypatch.setattr(media,'send_download',sent)
        monkeypatch.setattr(variants,'needs_telegram_variant',lambda *a:False)
        m.cli=SimpleNamespace(get_messages=AsyncMock(return_value=[SimpleNamespace(id=55,empty=False)]))
        with pytest.raises(RuntimeError):
            await m._item_snapshot(dict(platform='bilibili',user_id='123',chat_id=906346853),m.store.item('bilibili','123','BV1234567890_1'),None,None,None)
        assert sent.await_count==1
        assert m.store.delivered('bilibili','123','BV1234567890_1',906346853)
        assert p.exists(), 'pending collection must retain local original'
        m.store.merge_collections('bilibili','123',[dict(id='season_1',name='fixed')])
        upload=AsyncMock(return_value=[dict(path='remote/copy.mp4',name='archive.mp4',size=3)])
        monkeypatch.setattr(jobs,'upload_collection',upload)
        monkeypatch.setattr(jobs,'upload_item',AsyncMock(side_effect=AssertionError('main archive must not repeat')))
        await m._item_snapshot(dict(platform='bilibili',user_id='123',chat_id=906346853),m.store.item('bilibili','123','BV1234567890_1'),None,None,None)
        assert upload.await_count==1 and sent.await_count==1
        assert not p.exists()
        assert m.store.item('bilibili','123','BV1234567890_1')['status']=='complete'
    asyncio.run(run())


def test_streaming_ingest_persists_collections_before_items():
    import inspect
    source=inspect.getsource(ProfileManager._run_authorized)
    assert "self.store.merge_collections(platform,uid,batch.get('collections') or [])" in source
    assert source.index('self.store.merge_collections')<source.index('self.store.merge_items')


def test_capacity_blocks_new_but_allows_local_drain(tmp_path,monkeypatch):
    from services import profile_capacity as cap
    gate=cap.ProfileCapacity(tmp_path)
    monkeypatch.setattr(cap.shutil,'disk_usage',lambda p:SimpleNamespace(free=10*cap.GIB))
    monkeypatch.setattr(gate,'pending_bytes',lambda:50*cap.GIB)
    with pytest.raises(cap.CapacityDeferred):gate.require()
    gate.require(local_drain=True)
    monkeypatch.setattr(cap.shutil,'disk_usage',lambda p:SimpleNamespace(free=1*cap.GIB))
    with pytest.raises(cap.CapacityDeferred):gate.require(local_drain=True)


def test_inflight_download_cancelled_and_joined(tmp_path,monkeypatch):
    from services import profile_capacity as cap
    monkeypatch.setattr(cap.shutil,'disk_usage',lambda p:SimpleNamespace(free=0))
    stopped=[]
    async def run():
        async def download():
            try:await asyncio.sleep(60)
            finally:stopped.append(True)
        with pytest.raises(cap.CapacityDeferred):await cap.ProfileCapacity(tmp_path).guard(download())
    asyncio.run(run());assert stopped==[True]


def test_collection_folder_survives_rename(tmp_path):
    s=ProfileStore(tmp_path/'state.sqlite3');s.upsert_profile('bilibili','123','test','123')
    a=s.merge_collections('bilibili','123',[dict(id='season_1',name='old')])
    b=s.merge_collections('bilibili','123',[dict(id='season_1',name='new'),dict(id='season_2',name='two')])
    assert a['season_1']['folder']==b['season_1']['folder']
    s.close()


@pytest.mark.parametrize('status',[200,201,204,403,500])
def test_collection_upload_response_contract(tmp_path,monkeypatch,status):
    p=tmp_path/'video.mp4';p.write_bytes(b'video')
    monkeypatch.setattr(archive,'ensure_folders',lambda *a:None)
    monkeypatch.setattr(archive,'_request',lambda *a,**k:(status,b''))
    monkeypatch.setattr(archive,'_remote_size',lambda *a:pytest.fail('unexpected readback'))
    fn=lambda:archive._collection_upload(None,'123','season_1_test',[dict(name='video.mp4',size=5)],[p])
    if status in [200,201,204]:assert fn()[0]['size']==5
    else:
        with pytest.raises(RuntimeError):fn()


def test_streaming_checkpoint_runtime_and_retry_completion(tmp_path,monkeypatch):
    async def run():
        from services import bilibili_space
        m=ProfileManager.__new__(ProfileManager);m.root=tmp_path;m.store=ProfileStore(tmp_path/'state.sqlite3');m.send_locks={}
        m.store.upsert_profile('bilibili','123','test','123')
        job,_=m.store.enqueue('bilibili','123','https://space.bilibili.com/123',906346853,42,requester_id=906346853,request_chat_type='private')
        m.store.set_job(job['id'],failed_count=9)
        async def context(j):return SimpleNamespace(),None
        m._context=context
        monkeypatch.setattr(jobs.WebDavArchiveConfig,'from_env',lambda:object())
        monkeypatch.setattr(jobs,'upload_report',AsyncMock())
        monkeypatch.setattr(jobs,'_send_terminal_messages',AsyncMock(return_value=True))
        async def scan(uid,checkpoint,**cache_callbacks):
            batch=dict(platform='bilibili',user_id=uid,public_id=uid,collections=[dict(id='season_1',name='test')],items=[dict(id='BV1234567890_1',url='https://www.bilibili.com/video/BV1234567890?p=1',memberships=['season_1'])],pages=1,complete=True)
            await checkpoint(batch)
            return batch
        monkeypatch.setattr(bilibili_space,'enumerate_space',scan)
        seen=[]
        async def process(j,item,*args):
            assert m.store.profile('bilibili','123')['collections']['season_1']['folder']=='season_1_test'
            seen.append(item['post_id'])
            m.store.set_item('bilibili','123',item['post_id'],status='complete',archive_status='succeeded',files=[{'name':'x'}],collection_files={'season_1':[]})
            m.store.mark_delivered('bilibili','123',item['post_id'],906346853,[123])
        m._item=process
        await m._run_authorized(job['id'])
        assert seen==['BV1234567890_1']
        assert m.store.job(job['id'])['status']=='completed', 'historical failures must not keep successful retries queued forever'
    asyncio.run(run())

