import importlib.util
import tempfile
import unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('store',Path(__file__).parent/'services/profile_store.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class ResumeTest(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.s=m.ProfileStore(Path(self.tmp.name)/'db')
 def tearDown(self):
  self.s.close();self.tmp.cleanup()
 def job(self,uid='a',platform='xhs',chat=123,status='incomplete',reason='login_expired'):
  j,_=self.s.enqueue(platform,uid,'https://example.org/profile',chat,42,requester_id=chat,request_chat_type='private')
  return self.s.set_job(j['id'],status=status,enumeration_reason=reason,login_state='expired')
 def test_resume_preserves_media_and_is_idempotent(self):
  j=self.job();self.s.merge_items('xhs','a',[{'id':'ok'},{'id':'bad'}])
  self.s.set_item('xhs','a','ok',status='complete',download_status='succeeded',archive_status='succeeded',files=[{'name':'ok'}])
  self.s.set_item('xhs','a','bad',status='failed',download_status='failed',local_dir='/partial',attempts=3)
  self.s.set_delivery('xhs','a','ok',123,status='succeeded',message_ids=[9])
  before=[tuple(r) for r in self.s.db.execute('select * from items')]+[tuple(r) for r in self.s.db.execute('select * from deliveries')]
  self.assertEqual(self.s.resume_login_jobs('xhs',123),[j['id']]);self.assertEqual(self.s.resume_login_jobs('xhs',123),[])
  after=[tuple(r) for r in self.s.db.execute('select * from items')]+[tuple(r) for r in self.s.db.execute('select * from deliveries')]
  self.assertEqual(before,after);self.assertIsNone(self.s.job(j['id'])['ended_at'])
 def test_scope_and_non_auth_failures(self):
  for kw in ({'platform':'douyin'},{'chat':456},{'status':'completed'},{'reason':'pagination_stalled'},{'reason':'browser_navigation_or_state_read_failed'},{'reason':'上线验收样本，尚未执行全量任务'}):
   self.job(uid=str(kw),**kw)
  self.assertEqual(self.s.resume_login_jobs('xhs',123),[])
 def test_latest_completed_or_active_supersedes_old(self):
  self.job();self.job(status='completed');self.job(uid='b');self.job(uid='b',status='queued')
  self.assertEqual(self.s.resume_login_jobs('xhs',123),[])
 def test_duplicate_history_requeues_only_latest_in_order(self):
  self.job();b=self.job(uid='b');a=self.job()
  self.assertEqual(self.s.resume_login_jobs('xhs',123),[b['id'],a['id']])
 def test_all_platforms_are_isolated(self):
  jobs={p:self.job(platform=p) for p in ('xhs','douyin','bilibili')}
  for platform,job in jobs.items():
   self.assertEqual(self.s.resume_login_jobs(platform,123),[job['id']])
   self.assertEqual(self.s.resume_login_jobs(platform,123),[])
 def test_unsupported_platform(self):
  self.job(platform='unknown')
  self.assertEqual(self.s.resume_login_jobs('unknown',123),[])
 def test_untrusted_provenance_not_resumed(self):
  j,_=self.s.enqueue('xhs','a','url',123,42,requester_id=456,request_chat_type='group')
  self.s.set_job(j['id'],status='incomplete',enumeration_reason='login_expired')
  self.assertEqual(self.s.resume_login_jobs('xhs',123),[])

if __name__=='__main__':unittest.main()
