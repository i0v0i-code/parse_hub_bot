import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import pytest
from plugins import relogin
from services import profile_jobs

@pytest.mark.parametrize('platform,state,expected',[(p,s,int(s=='logged_in')) for p in ('bilibili',) for s in ('logged_in','expired','cancelled','not_found')])
def test_verified_login_hook(monkeypatch,platform,state,expected):
 manager=Mock();manager.resume_after_login.return_value=['10','66']
 monkeypatch.setattr(profile_jobs,'start_profile_manager',Mock(return_value=manager))
 monkeypatch.setattr(relogin,'qr_bytes',lambda r:b'qr')
 call=AsyncMock(side_effect=[{'session_id':'test'}, {'status':state}, {}])
 monkeypatch.setattr(relogin,'_call',call)
 cli=SimpleNamespace(send_photo=AsyncMock(return_value=SimpleNamespace(id=2)),get_messages=AsyncMock(return_value=SimpleNamespace(photo=True)),send_message=AsyncMock())
 msg=SimpleNamespace(chat=SimpleNamespace(id=123),id=1,edit_reply_markup=AsyncMock())
 asyncio.run(relogin.run_login(cli,msg,platform,'token'))
 assert manager.resume_after_login.call_count==expected
 if expected:
  manager.resume_after_login.assert_called_once_with(platform,123)
  assert '2 个' in cli.send_message.call_args.args[1]
