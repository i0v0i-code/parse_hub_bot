import asyncio
from contextlib import contextmanager
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock, patch
import pytest

OWNER = 906346853

@pytest.fixture(autouse=True)
def owner_config(monkeypatch):
    monkeypatch.setenv('WEBDAV_OWNER_TGID', str(OWNER))

def msg(uid=OWNER, cid=None, kind='private'):
    return NS(chat=NS(id=uid if cid is None else cid, type=kind),
              from_user=NS(id=uid), id=77, message_thread_id=None, empty=False)

def req(message):
    return NS(msg=message, cli=Mock(), config=Mock(), url='https://space.bilibili.com/946974', chat_id=message.chat.id)

def test_sink_denies_unscoped_writes_before_network():
    from services.webdav import _request, WebDavArchiveConfig
    config = WebDavArchiveConfig('http://127.0.0.1/dav', 'test', 'test')
    with patch('services.webdav.urlopen', side_effect=AssertionError('network reached')) as net:
        for method in ('PUT', 'MKCOL', 'COPY', 'MOVE', 'DELETE'):
            with pytest.raises(PermissionError):
                _request(config, method, 'policy-probe', data=b'x')
        net.assert_not_called()

@pytest.mark.parametrize('uid,cid,kind', [(123456789,123456789,'private'),(OWNER,-100123,'supergroup'),(OWNER,-12,'group'),(123456789,-12,'group'),(OWNER,-12,'channel')])
def test_denied_profiles_do_not_enqueue(uid,cid,kind):
    from services.profile_jobs import ProfileManager
    manager = object.__new__(ProfileManager)
    manager.store = Mock()
    manager.store.enqueue.return_value = ({'id':'1'}, True)
    manager.wake = Mock()
    sender = NS(text_no_preview=AsyncMock(return_value=NS(id=1)))
    with patch('plugins.parse.sender.MessageSender', return_value=sender), \
         patch('services.profile_jobs.WebDavArchiveConfig.from_env', side_effect=AssertionError('config reached')):
        assert asyncio.run(manager.submit(req(msg(uid,cid,kind)))) is True
    manager.store.enqueue.assert_not_called()
    manager.wake.set.assert_not_called()
    assert '仅限' in sender.text_no_preview.call_args.args[0]

@pytest.mark.parametrize('uid,cid,kind,expected', [(OWNER,OWNER,'private',True),(123,123,'private',False),(OWNER,-123,'group',False),(OWNER,-123,'supergroup',False),(OWNER,-123,'channel',False),(OWNER,OWNER,None,False),(123,OWNER,'private',False)])
def test_message_policy_matrix(uid,cid,kind,expected):
    from services.owner_policy import message_principal
    assert bool(message_principal(msg(uid,cid,kind))) is expected

def test_enum_and_anonymous_and_missing_config(monkeypatch):
    from services.owner_policy import message_principal
    from pyrogram.enums import ChatType
    assert message_principal(msg(kind=ChatType.PRIVATE))
    m=msg(); m.from_user=None
    assert message_principal(m) is None
    monkeypatch.delenv('WEBDAV_OWNER_TGID')
    assert message_principal(msg()) is None


def test_context_isolation_threads_and_reset():
    from services.owner_policy import archive_scope, message_principal, archive_allowed
    async def worker(message):
        with archive_scope(message_principal(message)):
            await asyncio.sleep(0)
            return await asyncio.to_thread(archive_allowed)
    async def run():
        assert await asyncio.gather(worker(msg()),worker(msg(123)),worker(msg(OWNER,-2,'group'))) == [True,False,False]
        assert not archive_allowed()
        try:
            with archive_scope(message_principal(msg())):
                raise ValueError('cancel path')
        except ValueError:
            pass
        assert not archive_allowed()
    asyncio.run(run())


def test_owner_profile_persists_trusted_identity(tmp_path):
    from services.profile_jobs import ProfileManager
    from services.profile_store import ProfileStore
    manager=object.__new__(ProfileManager); manager.store=ProfileStore(tmp_path/'state.sqlite3');manager.wake=Mock()
    sender=NS(text_no_preview=AsyncMock(return_value=NS(id=1)))
    with patch('plugins.parse.sender.MessageSender', return_value=sender), patch('services.profile_jobs.WebDavArchiveConfig.from_env', return_value=object()):
        assert asyncio.run(manager.submit(req(msg())))
    job=manager.store.queued_jobs()[0]
    assert (job['requester_id'],job['chat_id'],job['request_chat_type'])==(OWNER,OWNER,'private')
    manager.store.close()
    reopened=ProfileStore(tmp_path/'state.sqlite3',recover=True)
    assert reopened.queued_jobs()[0]['requester_id']==OWNER
    reopened.close()

@pytest.mark.parametrize('kind', ['new_owner','new_other','new_group','legacy_owner','legacy_group','legacy_missing','legacy_bot','legacy_network_error'])
def test_resumed_jobs_require_owner_private(kind):
    from services.profile_jobs import ProfileManager
    from services.owner_policy import archive_allowed
    manager=object.__new__(ProfileManager);manager.store=Mock(); manager.cli=Mock()
    job=dict(id='1',platform='bilibili',chat_id=OWNER,message_id=77)
    if kind.startswith('new'):
        job.update(requester_id=OWNER,request_chat_type='private')
    if kind=='new_other':job.update(requester_id=123,chat_id=123)
    if kind in ('new_group','legacy_group'):job['chat_id']=-123
    original=msg()
    if kind=='legacy_missing':original=NS(empty=True)
    if kind=='legacy_bot':original=msg(777,OWNER)
    manager.cli.get_messages=AsyncMock(return_value=original)
    if kind=='legacy_network_error':manager.cli.get_messages.side_effect=ConnectionError('offline')
    manager.store.job.return_value=job
    seen=[]
    async def run_authorized(jid):seen.append((jid,archive_allowed()))
    manager._run_authorized=run_authorized
    asyncio.run(manager._run('1'))
    if kind in ('new_owner','legacy_owner'):
        assert seen==[('1',True)]
        manager.store.set_job.assert_not_called()
    else:
        assert not seen
        expected='queued' if kind=='legacy_network_error' else 'failed'
        assert manager.store.set_job.call_args.kwargs['status']==expected
    if kind in ('new_other','new_group','legacy_group'):manager.cli.get_messages.assert_not_called()
    assert not archive_allowed()


def test_profile_upload_sinks_fail_without_scope(tmp_path):
    from services.profile_archive import upload_item,upload_report,upload_collection
    from services.webdav import WebDavArchiveConfig
    (tmp_path/'fixture.mp4').write_bytes(b'fixture-not-real-media')
    config=WebDavArchiveConfig('http://127.0.0.1/dav','test','test')
    async def run():
        for action in [lambda: upload_item(config,'xhs','public','post',tmp_path),
                       lambda: upload_report(config,'xhs','public','report'),
                       lambda: upload_collection(config,'123','season_1_test',[{'name':'fixture.mp4','size':22}],[str(tmp_path/'fixture.mp4')])]:
            with pytest.raises(PermissionError):await action()
    with patch('services.webdav.urlopen', side_effect=AssertionError('network reached')) as net:
        asyncio.run(run())
        net.assert_not_called()

@pytest.mark.parametrize('authorized', [False,True])
def test_single_pipeline_preserves_delivery_preparation(authorized,tmp_path):
    from services.pipeline import ParsePipeline
    from services.owner_policy import archive_scope,message_principal
    from parsehub.types import PostType
    download=NS(output_dir=tmp_path,media=[])
    parsed=NS(type=PostType.VIDEO,media=[],download=AsyncMock(return_value=download))
    parser=Mock();parser.parser.get_platform.return_value=NS(id='bilibili')
    reporter=NS(report=AsyncMock(),report_error=AsyncMock(),dismiss=AsyncMock())
    async def run():
        with archive_scope(message_principal(msg() if authorized else msg(123))):
            pipeline=ParsePipeline('https://example/video','url',reporter,parse_result=parsed,singleflight=False,skip_media_processing=True,t=lambda s:s)
            return await pipeline.run()
    with patch('services.pipeline.ParseService',return_value=parser), patch('services.pipeline.pl_cfg',NS(roll_downloader_proxy=lambda _:None)), patch('services.pipeline.WebDavArchiveConfig.from_env',return_value=object()) as conf, patch('services.pipeline.archive_output',new=AsyncMock(return_value=['file'])) as archive:
        result=asyncio.run(run())
    assert result.output_dir==tmp_path
    parsed.download.assert_awaited_once()
    assert archive.await_count==int(authorized)
    assert conf.call_count==int(authorized)

@pytest.mark.parametrize('authorized',[False,True])
def test_handler_cache_and_scope(authorized):
    from plugins.parse import handlers
    from plugins.parse.context import ParseRequest
    from repo.settings import ParseMode
    from services.owner_policy import archive_allowed
    observed=[]
    class Pipeline:
        waited=False
        def __init__(self,*args,**kwargs):observed.append((archive_allowed(),kwargs['singleflight']))
        def __enter__(self):return self
        def __exit__(self,*args):pass
        async def run(self):return NS(parse_result=NS(media=[],title='fixture',content='fixture'),processed_list=[])
    request=ParseRequest(cli=Mock(),msg=msg() if authorized else msg(123),url='https://example/video',mode=ParseMode.PREVIEW,config=Mock(),t_=lambda s:s)
    parser=NS(get_raw_url=AsyncMock(return_value='raw'))
    sender=NS(typing=AsyncMock(),text_no_preview=AsyncMock())
    reporter=NS(dismiss=AsyncMock(),report_error=AsyncMock())
    cache_data={'raw':object()}
    cache=NS(get=AsyncMock(side_effect=cache_data.get),set=AsyncMock(side_effect=lambda k,v:cache_data.__setitem__(k,v)))
    with patch.object(handlers,'ParseService',return_value=parser),patch.object(handlers,'MessageSender',return_value=sender),patch.object(handlers,'MessageStatusReporter',return_value=reporter),patch.object(handlers,'ParsePipeline',Pipeline),patch.object(handlers,'persistent_cache',cache),patch.object(handlers,'parse_cache',NS(get=AsyncMock(return_value=None),set=AsyncMock())),patch.object(handlers,'send_cached',new=AsyncMock()) as cached,patch.object(handlers,'build_caption',return_value='caption'):
        # Skip only the rate limiter; execute the actual identity-scope wrapper.
        result=asyncio.run(handlers.handle_parse.__wrapped__(request))
        if authorized:
            cached.assert_not_awaited()
            assert 'owner-archive:raw' in cache_data
            assert asyncio.run(handlers.handle_parse.__wrapped__(request))
    assert result is True
    if authorized:
        assert observed==[(True,False)]
        cached.assert_awaited_once()
        sender.text_no_preview.assert_awaited_once()
    else:
        assert observed==[]
        cached.assert_awaited_once()
    assert not archive_allowed()
