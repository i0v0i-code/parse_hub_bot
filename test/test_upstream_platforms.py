import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from parsehub import Platform
from parsehub.parsers.parser.douyin import DouyinParser
from parsehub.parsers.parser.xhs import XHSParser
from parsehub.provider_api.xhs import XHSAPI
from services import parser, login_credentials, platform_rate_limit
from services.profile_client import resolve_profile
from services.profile_jobs import ProfileManager
from services.profile_store import ProfileStore, RetryRejected
from plugins import relogin, profile_tasks


@pytest.mark.parametrize('platform,url', [
    (Platform.XHS, 'https://www.xiaohongshu.com/explore/123'),
    (Platform.DOUYIN, 'https://www.douyin.com/video/123'),
])
def test_standard_parse_ignores_legacy_credentials_and_platform_cooldown(monkeypatch, platform, url):
    # A stale export or 4-hour cooldown must not block the upstream parser.
    forbidden = Mock(side_effect=AssertionError('legacy login/cooldown was used'))
    monkeypatch.setattr(login_credentials, 'snapshot', forbidden)
    monkeypatch.setattr(platform_rate_limit, 'check_available', forbidden)
    monkeypatch.setattr(parser, 'pl_cfg', SimpleNamespace(roll_cookie=Mock(return_value=None), roll_parser_proxy=Mock(return_value=None)))
    service = parser.ParseService()
    service.parser = SimpleNamespace(get_platform=Mock(return_value=platform), parse=AsyncMock(return_value='parsed'))
    assert asyncio.run(service.parse(url)) == 'parsed'
    service.parser.parse.assert_awaited_once_with(url, cookie=None, proxy=None)
    forbidden.assert_not_called()


def test_platform_classes_are_upstream_and_bilibili_remains_patched():
    from parsehub.parsers.parser.bilibili import BiliParse, BiliYtParse
    assert DouyinParser._fetch_api_result.__module__ == 'parsehub.parsers.parser.douyin'
    assert XHSAPI._XHSAPI__fetch_html.__module__ == 'parsehub.provider_api.xhs'
    assert XHSParser.get_raw_url.__module__ == 'parsehub.parsers.parser.xhs'
    assert BiliParse._do_parse.__module__ == 'services.bilibili_quality'
    assert BiliYtParse.get_cookie_text.__module__ == 'services.bilibili_quality'


def test_bilibili_uses_latest_export_and_its_own_rate_limit(monkeypatch):
    credential = {'cookies': [{'name': 'SESSDATA', 'value': 'fake-test-session'}], 'saved_at': 10}
    monkeypatch.setattr(login_credentials, 'snapshot', lambda p: credential)
    monkeypatch.setattr(login_credentials, 'load', lambda p: credential)
    monkeypatch.setattr(parser, 'pl_cfg', SimpleNamespace(roll_cookie=Mock(return_value=None), roll_parser_proxy=Mock(return_value=None)))
    monkeypatch.setattr(platform_rate_limit, 'read_state', lambda p: {})
    check = Mock(); wait = AsyncMock(); success = Mock()
    monkeypatch.setattr(platform_rate_limit, 'check_available', check)
    monkeypatch.setattr(platform_rate_limit, 'wait_turn', wait)
    monkeypatch.setattr(platform_rate_limit, 'succeeded', success)
    service = parser.ParseService()
    service.parser = SimpleNamespace(get_platform=lambda url: Platform.BILIBILI, parse=AsyncMock(return_value='bili'))
    assert asyncio.run(service.parse('https://www.bilibili.com/video/BV1test')) == 'bili'
    assert service.parser.parse.call_args.kwargs['cookie'] == 'SESSDATA=fake-test-session'
    check.assert_called_once_with('bilibili')
    wait.assert_awaited_once_with('bilibili', wait_cooldown=False)


def test_only_bilibili_profiles_are_intercepted(monkeypatch):
    import services.profile_client as module
    monkeypatch.setattr(module.httpx, 'AsyncClient', Mock(side_effect=AssertionError('unexpected preflight request')))
    for url in ['https://xhslink.cn/o/example', 'https://v.douyin.com/example/',
                'https://www.xiaohongshu.com/user/profile/601abaad000000000100b50d',
                'https://www.douyin.com/user/example']:
        assert asyncio.run(resolve_profile(url)) is None
    assert asyncio.run(resolve_profile('https://space.bilibili.com/3670216')) == (
        'bilibili', '3670216', 'https://space.bilibili.com/3670216')


def test_pause_is_idempotent_and_preserves_all_media_and_bilibili_jobs(tmp_path):
    store = ProfileStore(tmp_path / 'state.sqlite3')
    for platform in ('xhs', 'douyin', 'bilibili'):
        job, _ = store.enqueue(platform, 'user', 'https://example.com', 123, 1)
        store.set_job(job['id'], status='running', retry_at=1234)
        store.merge_items(platform, 'user', [{'id': 'saved'}])
        store.set_item(platform, 'user', 'saved', archive_status='succeeded', files=[{'name': 'saved.mp4'}])
        store.set_delivery(platform, 'user', 'saved', 123, status='succeeded', message_ids=[5])
    media_before = [tuple(row) for table in ('items', 'deliveries') for row in store.db.execute('SELECT * FROM '+table)]
    bili_before = store.job(3)
    assert store.pause_legacy_profiles() == 2
    assert store.pause_legacy_profiles() == 0
    assert store.job(1)['status'] == store.job(2)['status'] == 'paused'
    assert store.job(1)['retry_at'] == 1234
    assert store.job(3) == bili_before
    assert media_before == [tuple(row) for table in ('items', 'deliveries') for row in store.db.execute('SELECT * FROM '+table)]
    store.recover()
    assert store.job(1)['status'] == 'paused'
    assert store.job(3)['status'] == 'queued'
    assert [job['platform'] for job in store.queued_jobs()] == ['bilibili']


@pytest.mark.parametrize('platform', ['xhs', 'douyin'])
def test_retired_profiles_cannot_be_retried_or_logged_in(tmp_path, monkeypatch, platform):
    monkeypatch.setenv('WEBDAV_OWNER_TGID', '123')
    store = ProfileStore(tmp_path / 'state.sqlite3')
    job, _ = store.enqueue(platform, 'user', 'https://example.com', 123, 1, requester_id=123, request_chat_type='private')
    store.set_job(job['id'], status='incomplete')
    manager = ProfileManager.__new__(ProfileManager); manager.store = store
    with pytest.raises(RetryRejected) as exc:
        asyncio.run(manager.retry_from_management(job['id'], caller_id=123, chat_id=123))
    assert exc.value.code == 'platform_disabled'
    assert not profile_tasks._retryable(store.job(job['id']))
    call = AsyncMock(side_effect=AssertionError('removed login called'))
    monkeypatch.setattr(relogin, '_call', call)
    asyncio.run(relogin.run_login(None, None, platform, 'token'))
    call.assert_not_called()
    assert relogin.LABELS == {'bilibili': 'B站'}
