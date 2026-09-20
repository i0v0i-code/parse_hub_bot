"""Persistent user-profile jobs; 1 item worker per user, existing media delivery."""
import asyncio
import html
import re
import shutil
import time
from pathlib import Path
from urllib.parse import urlsplit,urlunsplit

from core import bs,pl_cfg
from db import get_session
from i18n import t_
from log import logger
from services.profile_store import ProfileStore, RetryRejected
from services.profile_capacity import ProfileCapacity, CapacityDeferred, CollectionPending
from services.profile_client import resolve_profile,enumerate_profile,enumerate_profile_stream
from services.profile_archive import upload_item,upload_report,restore_item,user_folder,upload_collection
from services.webdav import WebDavArchiveConfig,_archive_files
from services.owner_policy import (BATCH_DENIED, archive_scope, job_principal, message_principal,
                                   owner_id)


def safe_error(error):
    text=ProfileStore._safe_text(error)
    text=re.sub(r'https?://\S+','[链接]',text)
    return f'{type(error).__name__}: {text[:250]}'


def cooldown_until(platform):
    if platform == 'xhs':
        from services import xhs_rate_limit
        state = xhs_rate_limit.read_state()
    elif platform in ('bilibili', 'douyin'):
        from services import platform_rate_limit
        state = platform_rate_limit.read_state(platform)
    else:
        return 0.0
    return float(state.get('until', 0))


async def _send_terminal_messages(sender, *, status, job_id, current, skipped,
                                  result, root, platform, user_id):
    """Send one final summary only after the job is truly complete.

    Transient item/enumeration failures are persisted as queued for automatic
    retry.  Their progress message is updated in place; sending the summary and
    download record here would repeat both on every retry cycle.
    """
    if status == 'queued':
        return False
    label = '完成' if status == 'completed' else '部分完成'
    reason='' if result.get('complete') else '\n注意：平台未确认列表读取完毕，本次不能算全量完成。'+result.get('reason','')
    await sender.text_no_preview(
        f'用户下载任务 #{job_id}：{label}'
        f'\n新增完成 {current.get("new_count",0)}，跳过 {skipped}，失败 {current.get("failed_count",0)}。'
        f'{reason}\n再次发送同一主页可增量补下/补发。'
    )
    await sender.document(
        str(root/platform/user_id/'下载记录.txt'),
        caption=f'用户 {user_id} 下载记录',
    )
    return True


class ProfileManager:
    def __init__(self,cli):
        self.cli=cli
        self.root=bs.data_path/'profile_jobs'
        self.root.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.store=ProfileStore(self.root/'state.sqlite3')
        self.store.recover()
        self.active={}
        self.send_locks={}
        self.wake=asyncio.Event()
        self.closing=False
        self.runner=asyncio.create_task(self._scheduler())

    async def stop(self):
        self.closing=True
        self.runner.cancel()
        active=list(self.active.items())
        for _,task in active:task.cancel()
        await asyncio.gather(self.runner,*(task for _,task in active),return_exceptions=True)
        # A cancellation can land before _run enters its own handler. Make the
        # restart boundary durable so queued work resumes and profile workdirs
        # remain available for stage-aware recovery instead of being discarded.
        for jid,_ in active:
            try:
                job=self.store.job(jid)
                if job and job.get('status')=='running':
                    self.store.set_job(jid,status='queued',retry_at=0,last_error='服务重启，未完成进度已保留')
            except Exception as error:
                logger.warning(f'用户下载任务停止状态保存失败: job={jid}, error={type(error).__name__}')
        self.active.clear()

    async def submit(self,req):
        resolved=await resolve_profile(req.url)
        if resolved is None:return False
        from plugins.parse.sender import MessageSender
        sender=MessageSender(req.cli,req.msg,req.config)
        principal=message_principal(req.msg)
        if principal is None:
            await sender.text_no_preview(BATCH_DENIED);return True
        if not WebDavArchiveConfig.from_env():
            await sender.text_no_preview('用户批量下载需要先配置 WebDAV。');return True
        platform,uid,url=resolved
        job,created=self.store.enqueue(platform,uid,url,req.chat_id,req.msg.id,
                                       thread_id=req.msg.message_thread_id,
                                       requester_id=principal.requester_id,
                                       request_chat_type=principal.chat_type)
        if created:
            note=await sender.text_no_preview(f'已创建用户下载任务 #{job["id"]}\n用户 ID：{uid}\n最多同时运行 3 个主页任务，每个平台 1 个；每个主页内 1 个作品并发。视频和图文保存到 WebDAV 并回传。\n按需分页读取作品列表，重复作品会跳过。')
            self.store.set_job(job['id'],progress_message_id=note.id)
        else:
            await sender.text_no_preview(f'该用户已有任务 #{job["id"]} 正在执行，不会重复下载。')
        self.wake.set()
        return True

    async def retry_from_management(self, job_id, *, caller_id, chat_id, requested_message_id=None):
        """Validate the owner and original request before creating a retry job."""
        owner = owner_id()
        if owner is None or caller_id != owner or chat_id != owner:
            raise RetryRejected('owner_only', '任务管理仅限主人私聊使用')
        job = self.store.job(job_id)
        if job is None or str(job.get('chat_id')) != str(chat_id):
            raise RetryRejected('not_found', f'未找到任务 #{job_id}')

        principal = job_principal(job)
        if principal is None:
            original_id = job.get('message_id')
            try:
                original = await self.cli.get_messages(job['chat_id'], original_id)
            except Exception:
                raise RetryRejected(
                    'legacy_unverified',
                    '任务来源无法核验为主人私聊，未执行重试。请重新发送原始主页链接创建新任务。',
                ) from None
            if isinstance(original, list):
                original = original[0] if len(original) == 1 else None
            if not original or getattr(original, 'empty', False) or getattr(original, 'id', None) != original_id:
                raise RetryRejected(
                    'legacy_unverified',
                    '任务来源无法核验为主人私聊，未执行重试。请重新发送原始主页链接创建新任务。',
                )
            principal = message_principal(original)
        if principal is None or principal.chat_id != chat_id:
            raise RetryRejected(
                'legacy_unverified',
                '任务来源无法核验为主人私聊，未执行重试。请重新发送原始主页链接创建新任务。',
            )
        retry = self.store.retry_job(
            job_id,
            requester_id=principal.requester_id,
            request_chat_type=principal.chat_type,
            requested_message_id=requested_message_id,
        )
        self.wake.set()
        return retry

    def resume_after_login(self, platform, chat_id):
        if platform not in {'xhs', 'douyin', 'bilibili'} or owner_id() is None or chat_id != owner_id():
            return []
        resumed = self.store.resume_login_jobs(platform, chat_id)
        if resumed:
            self.wake.set()
        return resumed

    async def _scheduler(self):
        while not self.closing:
            # Up to three profile jobs globally, with one active job per platform.
            queued=self.store.queued_jobs()
            if shutil.disk_usage(self.root).free<8*1024**3:
                # Under pressure, drain existing originals instead of letting
                # earlier download-only jobs repeatedly occupy the available slots.
                queued.sort(key=lambda j:not any(i.get('local_dir') and i.get('status')!='complete' for i in self.store.items(j['platform'],j['user_id'])))
            for job in queued:
                if len(self.active)>=3:break
                jid=job['id']
                if jid in self.active:continue
                try:
                    if any(self.store.job(active_id)['platform'] == job['platform'] for active_id in self.active):continue
                    self.store.set_job(jid,status='running')
                    task=asyncio.create_task(self._run(jid))
                    self.active[jid]=task
                    task.add_done_callback(lambda task,jid=jid:self._done(jid,task))
                except Exception as error:
                    # A malformed/legacy job must never kill the scheduler loop.
                    logger.warning(f'用户下载任务启动失败: job={jid}, error={type(error).__name__}')
                    try:self.store.set_job(jid,status='queued',retry_at=time.time()+300,last_error=f'{type(error).__name__}: {error}')
                    except Exception:pass
            self.wake.clear()
            try:await asyncio.wait_for(self.wake.wait(),5)
            except asyncio.TimeoutError:pass

    def _done(self,jid,task):
        self.active.pop(jid,None);self.wake.set()
        if not task.cancelled() and task.exception():
            logger.error(f'用户下载任务异常: job={jid}, error={type(task.exception()).__name__}')

    async def _context(self,job):
        from plugins.context import get_config_target
        from plugins.parse.sender import MessageSender
        from services import SettingsService,UserService
        msg=await self.cli.get_messages(job['chat_id'],job['message_id'])
        if not msg or getattr(msg,'empty',False):
            msg=await self.cli.send_message(job['chat_id'],f'恢复用户下载任务 #{job["id"]}',message_thread_id=job.get('thread_id'))
        async with get_session() as session:
            config=await SettingsService(session).get_config(get_config_target(msg))
            lang=await UserService(session).get_lang(msg.from_user.id) if msg.from_user else None
        return MessageSender(self.cli,msg,config),t_[lang]

    async def _request_login(self,jid,job,platform,sender):
        """Persist QR delivery and wait for verified browser authentication."""
        from services.profile_login import start_login,wait_logged_in,cancel_login,qr_bytes
        import tempfile
        platform_name={'xhs':'小红书','douyin':'抖音'}.get(platform,platform)
        sid=None;qr_path=None
        self.store.set_job(jid,login_state='starting',enumeration_reason='login_required')
        try:
            started=await start_login(platform)
            sid=started.get('session_id')
            state=started.get('status')
            if state!='logged_in':
                raw=qr_bytes(started)
                if not raw or not sid:raise RuntimeError('no_valid_login_qr')
                qr_path=Path(tempfile.gettempdir())/f'parse-hub-login-{platform}-{jid}.png'
                qr_path.write_bytes(raw);qr_path.chmod(0o600)
                sent=await sender.photo(str(qr_path),caption=(
                    f'请用{platform_name}App扫码并确认登录（二维码可能提前失效）\n'
                    f'任务 #{jid}：确认登录后自动继续；等待上限 5 分钟。'))
                # Actual Telegram readback, not merely a file or send return.
                delivered=await self.cli.get_messages(job['chat_id'],sent.id)
                if not delivered or getattr(delivered,'empty',False) or not getattr(delivered,'photo',None):
                    raise RuntimeError('login_qr_delivery_unverified')
                self.store.set_job(jid,login_state='waiting_scan',login_message_id=sent.id,
                                   login_expires_at=started.get('expires_at'),enumeration_reason='waiting_scan')
                from services.login_verification import wait_for_login
                final=await wait_for_login(sid,job,sender,self.cli,self.store,jid,poll=5.0,timeout=300)
                state=final.get('status','unknown')
            if state=='logged_in':
                self.store.set_job(jid,status='queued',retry_at=0,login_state='logged_in',
                                   last_error='',enumeration_reason='登录成功，自动继续任务')
                resumed = self.resume_after_login(platform, job['chat_id'])
                try:await sender.text_no_preview(f'✅ {platform_name}登录已确认，任务 #{jid} 自动继续。另有 {len(resumed)} 个登录中断任务已自动加入队列。')
                except Exception:pass
            else:
                self.store.set_job(jid,status='incomplete',retry_at=0,login_state=state,
                                   enumeration_reason=f'login_{state}')
                await sender.text_no_preview(f'{platform_name}扫码未完成或二维码已失效。任务已保留；重新发送主页链接可获取新二维码继续，不会定时刷屏。')
        except asyncio.CancelledError:
            self.store.set_job(jid,status='queued',retry_at=0,login_state='interrupted',
                               enumeration_reason='login_interrupted')
            raise
        except Exception as error:
            # Delivery/start errors must not be presented as awaiting a scan.
            self.store.set_job(jid,status='incomplete',retry_at=0,login_state='failed',
                               last_error=type(error).__name__,enumeration_reason='login_flow_failed')
            try:await sender.text_no_preview(f'{platform_name}登录流程失败，任务已保留。请稍后重发主页链接重试。')
            except Exception:pass
        finally:
            if qr_path is not None:qr_path.unlink(missing_ok=True)
            if sid:
                try:await cancel_login(sid)
                except Exception:pass

    async def _run(self,jid):
        # New jobs persist trusted Telegram provenance. Legacy jobs must prove
        # it from the original message, never from the bot's recovery message.
        job=self.store.job(jid)
        principal=job_principal(job)
        if 'requester_id' not in job and job.get('chat_id')==owner_id():
            try:
                original=await self.cli.get_messages(job['chat_id'],job['message_id'])
            except Exception:
                self.store.set_job(jid,status='queued',retry_at=time.time()+300,
                                   last_error='无法核验原请求身份，未启动批量下载')
                return
            if (original and not getattr(original,'empty',False)
                    and getattr(original,'id',None)==job['message_id']):
                principal=message_principal(original)
        if principal is None:
            self.store.set_job(jid,status='failed',retry_at=0,last_error=BATCH_DENIED)
            return
        with archive_scope(principal):
            await self._run_authorized(jid)

    async def _run_authorized(self,jid):
        job=self.store.job(jid);platform,uid=job['platform'],job['user_id']
        config=WebDavArchiveConfig.from_env()
        reporter_lock=asyncio.Lock();last_report=[0.0]
        counter_lock=asyncio.Lock()
        sender=None
        queue=asyncio.Queue(maxsize=6)
        workers=[]
        workers_stopped=False
        scheduled=set()
        listed=set()
        skipped=set()
        profile_seeded=False
        pages_seen=0

        def item_complete(item):
            pid=item.get('post_id',item.get('id'))
            if item.get('status')=='skipped':
                return True
            return (item.get('archive_status')=='succeeded' and item.get('files') and
                    self.store.delivered(platform,uid,pid,job['chat_id']) and
                    set(item.get('memberships',[]))<=set(item.get('collection_files',{})))

        async def increment_job(field):
            async with counter_lock:
                current=self.store.job(jid)
                self.store.set_job(jid,**{field:current.get(field,0)+1})

        async def report(force=False):
            async with reporter_lock:
                if not force and time.monotonic()-last_report[0]<30:return
                text=self.store.render_report(platform,uid,jid)
                report_path=self.root/platform/uid/'下载记录.txt'
                report_path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
                report_path.write_text(text,encoding='utf-8');report_path.chmod(0o600)
                await upload_report(config,platform,self.store.folder_id(platform,uid),text)
                last_report[0]=time.monotonic()
                current=self.store.job(jid)
                status=f'用户下载任务 #{jid}｜{current["status"]}\n用户 ID：{uid}\n本轮新增完成：{current.get("new_count",0)}｜跳过：{current.get("skipped_count",0)}｜失败：{current.get("failed_count",0)}\nWebDAV：{user_folder(platform,self.store.folder_id(platform,uid))}'
                if current['status']=='queued':
                    status+='\n等待原因：'+str(current.get('enumeration_reason') or '未完成作品稍后重试')
                    status+=f'\n约 {max(0, int((current.get("retry_at",0)-time.time()+59)//60))} 分钟后自动重试'
                if current.get('progress_message_id'):
                    try:await self.cli.edit_message_text(job['chat_id'],current['progress_message_id'],status)
                    except Exception:pass

        async def enqueue_item(item):
            pid=str(item.get('post_id',item.get('id','')))
            if not pid or pid in scheduled:
                return
            current=self.store.item(platform,uid,pid)
            if current is None:
                return
            if item_complete(current):
                skipped.add(pid)
                return
            scheduled.add(pid)
            await queue.put(current)

        async def seed_existing(force=False):
            nonlocal profile_seeded
            if profile_seeded or not (self.store.profile(platform,uid) or {}).get('folder_id'):
                return
            if platform=='bilibili' and not force:
                known=set((self.store.profile(platform,uid) or {}).get('collections',{}))
                needed={m for i in self.store.items(platform,uid) for m in i.get('memberships',[]) if m not in i.get('collection_files',{})}
                if not needed<=known:return
            profile_seeded=True
            existing=self.store.items(platform,uid)
            existing.sort(key=lambda i:not bool(i.get('local_dir')))
            for item in existing:
                await enqueue_item(item)

        async def ingest_batch(batch):
            nonlocal pages_seen
            if not isinstance(batch,dict):
                raise ValueError('主页枚举批次格式无效')
            if batch.get('platform') not in (None,'',platform):
                raise ValueError('主页平台与任务不一致')
            batch_uid=batch.get('user_id')
            if batch_uid not in (None,'',uid):
                raise ValueError('主页稳定用户 ID 与任务不一致')
            pages_seen=max(pages_seen,int(batch.get('pages') or 0))
            public_id=batch.get('public_id')
            if public_id:
                self.store.upsert_profile(
                    platform,uid,batch.get('username') or uid,public_id,
                    batch.get('numeric_uid'),
                )
            items=batch.get('items') or []
            if not isinstance(items,list):
                raise ValueError('主页枚举批次作品格式无效')
            if platform=='bilibili':
                self.store.merge_collections(platform,uid,batch.get('collections') or [])
            self.store.merge_items(platform,uid,items)
            has_folder=bool((self.store.profile(platform,uid) or {}).get('folder_id'))
            for item in items:
                pid=str(item.get('post_id',item.get('id','')))
                if not pid:
                    continue
                listed.add(pid)
                if has_folder:
                    await enqueue_item(item)
            await queue.join()
            # Current-page work is queued first; old failed/unsent work is then
            # admitted through the same bounded queue for recovery compatibility.
            await seed_existing()
            self.store.set_job(
                jid,
                enumeration_complete=False,
                pages=pages_seen,
                skipped_count=len(skipped),
                enumeration_reason='扫描中'+(('：'+str(batch.get('reason'))) if batch.get('reason') else ''),
            )
            if has_folder:
                try:await report()
                except Exception as error:logger.warning(f'用户记录更新失败: job={jid}, error={type(error).__name__}')

        async def worker():
            while True:
                item=await queue.get()
                if item is None:
                    queue.task_done()
                    return
                pid=item.get('post_id',item.get('id'))
                try:
                    if cooldown_until(platform)>time.time() and item.get('download_status')!='succeeded':
                        continue
                    await self._item(job,item,sender,_t,config)
                    await increment_job('new_count')
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    saved=self.store.item(platform,uid,pid)
                    updates={'status':'failed','last_error':safe_error(error)}
                    from services.bilibili_quality import BilibiliUnavailable
                    unavailable=isinstance(error,BilibiliUnavailable)
                    if unavailable:
                        updates['status']='skipped'
                        skipped.add(str(pid))
                    if isinstance(error,CapacityDeferred):
                        updates['status']='waiting_space'
                        self.store.set_job(jid,last_error=str(error))
                    if saved and saved.get('download_status')=='running':updates['download_status']='failed'
                    if saved and saved.get('archive_status')=='running':updates['archive_status']='failed'
                    if saved and saved.get('archive_status')=='succeeded' and not isinstance(error,(CollectionPending,CapacityDeferred)) and not self.store.delivered(platform,uid,pid,job['chat_id']):
                        old=self.store.delivery(platform,uid,pid,job['chat_id']) or {}
                        self.store.set_delivery(platform,uid,pid,job['chat_id'],status='failed',last_error=safe_error(error),message_ids=old.get('message_ids'),completed_batches=old.get('completed_batches',0))
                    if saved:self.store.set_item(platform,uid,pid,**updates)
                    if not unavailable:await increment_job('failed_count')
                    logger.warning(f'用户视频下载失败: job={jid}, post={pid}, error={type(error).__name__}')
                finally:
                    if item_complete(self.store.item(platform,uid,pid) or {}):
                        scheduled.discard(str(pid))
                    queue.task_done()
                try:await report()
                except Exception as error:logger.warning(f'用户记录更新失败: job={jid}, error={type(error).__name__}')

        async def stop_workers(cancel=False):
            nonlocal workers_stopped
            if workers_stopped:return
            if cancel:
                for task in workers:task.cancel()
                await asyncio.gather(*workers,return_exceptions=True)
            else:
                await queue.join()
                for _ in workers:await queue.put(None)
                await asyncio.gather(*workers,return_exceptions=True)
            workers_stopped=True

        try:
            if config is None:raise ValueError('未配置 WebDAV')
            sender,_t=await self._context(job)
            workers=[asyncio.create_task(worker()) for _ in range(1)]
            await seed_existing()
            enumeration_error=False
            result=None
            try:
                scan_complete=self.store.scan_cache(jid,'complete') if platform=='bilibili' else None
                if scan_complete is None and cooldown_until(platform)>time.time():
                    raise RuntimeError(platform+' 访问冷却，保留任务等待重试')
                if scan_complete is not None:
                    # Enumeration and media delivery have independent progress.
                    # Retrying one failed archive must not scan the whole account.
                    result=scan_complete
                elif platform=='bilibili':
                    from services.bilibili_space import enumerate_space
                    self.store.upsert_profile(platform,uid,uid,uid,uid)
                    emitted_bili={}
                    async def checkpoint_space(partial):
                        batch=dict(partial)
                        fresh=[]
                        for item in partial.get('items') or []:
                            pid=str(item.get('post_id',item.get('id','')))
                            signature=tuple(item.get('memberships',[]))
                            if pid and emitted_bili.get(pid)!=signature:
                                emitted_bili[pid]=signature;fresh.append(item)
                        batch['items']=fresh;batch['complete']=False
                        await ingest_batch(batch)
                    result=await enumerate_space(uid,checkpoint_space,
                        cache_get=lambda key:self.store.scan_cache(jid,key),
                        cache_put=lambda key,data:self.store.save_scan_cache(jid,key,data))
                    if result.get('complete'):
                        self.store.save_scan_cache(jid,'complete',{k:v for k,v in result.items() if k!='items'})
                else:
                    result=await enumerate_profile_stream(job['url'],ingest_batch)
            except Exception as error:
                enumeration_error=True
                result=dict(platform=platform,user_id=uid,items=[],complete=False,reason=safe_error(error),pages=pages_seen)
            if result is None:
                enumeration_error=True
                result=dict(platform=platform,user_id=uid,items=[],complete=False,reason='browser_unavailable',pages=pages_seen)
            if result.get('platform') not in (None,'',platform):
                raise ValueError('主页平台与任务不一致')
            if result.get('user_id') not in (None,'',uid):
                raise ValueError('主页稳定用户 ID 与任务不一致')
            if result.get('public_id'):
                self.store.upsert_profile(platform,uid,result.get('username') or uid,result['public_id'],result.get('numeric_uid'))
            pages_seen=max(pages_seen,int(result.get('pages') or 0))
            reason=result.get('reason','')
            self.store.set_job(
                jid,
                enumeration_complete=bool(result.get('complete')),
                pages=pages_seen,
                skipped_count=len(skipped),
                enumeration_reason=reason,
            )
            # Let already discovered pages finish before asking for a new login.
            await seed_existing(force=True)
            await stop_workers()
            if reason in {'login_required','captcha_required'}:
                await self._request_login(jid,job,platform,sender)
                return
            profile=self.store.profile(platform,uid) or {}
            if not profile.get('folder_id'):
                if enumeration_error or not result.get('complete'):
                    if cooldown_until(platform)>time.time():
                        self.store.set_job(jid,status='queued',retry_at=cooldown_until(platform),last_error='')
                        return
                    raise ConnectionError('主页暂不可用，稍后自动重试')
                raise ValueError('未能识别公开抖音号/小红书号，本次未下载')
            current=self.store.job(jid)
            status='completed' if result.get('complete') and not self.store.pending_items(platform,uid,job['chat_id']) else 'queued'
            retry_at=max(time.time()+300,cooldown_until(platform))
            self.store.set_job(jid,status=status,retry_at=retry_at if status=='queued' else 0,skipped_count=len(skipped))
            await report(True)
            notified=await _send_terminal_messages(
                sender,
                status=status,
                job_id=jid,
                current=self.store.job(jid),
                skipped=len(skipped),
                result=result,
                root=self.root,
                platform=platform,
                user_id=uid,
            )
            if not notified:
                logger.info(f'用户下载任务等待自动重试: job={jid}')
        except asyncio.CancelledError:
            await stop_workers(cancel=True)
            self.store.set_job(jid,status='queued');raise
        except Exception as error:
            await stop_workers(cancel=True)
            terminal_status='failed' if isinstance(error,ValueError) else 'queued'
            self.store.set_job(jid,status=terminal_status,retry_at=max(time.time()+300,cooldown_until(platform)),enumeration_reason=safe_error(error))
            try:
                if config and (self.store.profile(platform,uid) or {}).get('folder_id'):await report(True)
                if sender and terminal_status=='failed':
                    await sender.text_no_preview(f'用户下载任务 #{jid} 未完成：{safe_error(error)}\n已成功归档的记录保留；账号识别错误，请核对主页链接后重新发送。')
                elif terminal_status=='queued':
                    logger.info(f'用户下载任务等待自动重试: job={jid}')
            except Exception:pass

    async def _item(self,job,item,sender,_t,config):
        from services.login_credentials import item_scope
        with item_scope(job['platform']):
            return await self._item_snapshot(job,item,sender,_t,config)

    async def _item_snapshot(self,job,item,sender,_t,config):
        from services import ParseService
        from services.profile_media import describe_download,attach_descriptors,from_records,send_download
        platform,uid=job['platform'],job['user_id'];pid=item.get('post_id',item.get('id'))
        item=self.store.item(platform,uid,pid) or item
        if not hasattr(self,'capacity'):self.capacity=ProfileCapacity(self.root)
        work=self.root/platform/uid/'files'/pid
        work.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.store.set_item(platform,uid,pid,status='downloading',attempts=item.get('attempts',0)+1,last_error='')
        files=item.get('files') or []
        local_dir=Path(item['local_dir']) if item.get('local_dir') else None
        local_paths=[Path(row.get('local_path','')) for row in files]
        local_ready=bool(files) and all(path.is_file() and path.stat().st_size==row['size'] for path,row in zip(local_paths,files))
        canonical=item.get('canonical_url') or ProfileStore._canonical(item['url'])
        item={**item,'canonical_url':canonical}
        parsed=None
        if local_ready:
            downloaded=from_records(files,local_paths,local_dir or work)
        elif item.get('archive_status')=='succeeded' and files:
            self.capacity.require(sum(row['size'] for row in files))
            paths=await restore_item(config,files,work/'restored')
            files=[{**row,'local_path':str(path)} for row,path in zip(files,paths,strict=True)]
            self.store.set_item(platform,uid,pid,files=files,local_dir=str(work/'restored'))
            downloaded=from_records(files,paths,work/'restored')
        else:
            self.capacity.require()
            self.store.set_item(platform,uid,pid,download_status='running',download_attempts=item.get('download_attempts',0)+1)
            parsed=await ParseService().parse(item['url'])
            if not parsed.media:raise ValueError('此作品没有可下载媒体')
            downloaded=await self.capacity.guard(asyncio.wait_for(parsed.download(work,proxy=pl_cfg.roll_downloader_proxy(platform),save_metadata=False),1800))
            local_dir=Path(downloaded.output_dir)
            if not item.get('is_video'):
                document=f"作品 ID：{pid}\n用户 ID：{uid}\n标题：{parsed.title or ''}\n链接：{canonical}\n\n{parsed.content or ''}\n"
                (local_dir/'文案.txt').write_text(document,encoding='utf-8')
            files=describe_download(downloaded)
            self.store.set_item(platform,uid,pid,local_dir=str(local_dir),download_status='succeeded',files=files)
        if item.get('archive_status')!='succeeded':
            self.store.set_item(platform,uid,pid,archive_status='running',archive_attempts=item.get('archive_attempts',0)+1)
            described=describe_download(downloaded)
            uploaded=await upload_item(config,platform,self.store.folder_id(platform,uid),pid,downloaded.output_dir,is_video=item.get('is_video',True))
            files=attach_descriptors(uploaded,described)
            self.store.set_item(platform,uid,pid,status='archived',archive_status='succeeded',download_status='succeeded',files=files,local_dir=str(downloaded.output_dir))
        collection_errors=[]
        if platform=='bilibili':
            copies=dict(item.get('collection_files') or {})
            collections=self.store.profile(platform,uid).get('collections',{})
            # Match immutable archived names to the local originals/restored paths.
            if item.get('archive_status')=='succeeded' and not local_ready:
                paths=[str(Path(downloaded.output_dir)/row['name']) for row in files]
            else:
                paths=[row['local_path'] for row in files]
            for membership in item.get('memberships',[]):
                if membership in copies:continue
                try:
                    if membership not in collections:
                        raise CollectionPending('合集元数据待补齐：'+membership)
                    copies[membership]=await upload_collection(config,self.store.folder_id(platform,uid),collections[membership]['folder'],files,paths)
                    self.store.set_item(platform,uid,pid,collection_files=copies)
                except Exception as error:
                    collection_errors.append(safe_error(error))
        if not self.store.delivered(platform,uid,pid,job['chat_id']):
            if platform=='bilibili':
                from services.youtube_variants import needs_telegram_variant,prepare_telegram_video
                if needs_telegram_variant(downloaded):
                    self.capacity.require(local_drain=True)
                    parsed=parsed or await ParseService().parse(item['url'])
                    downloaded=await self.capacity.guard(prepare_telegram_video(parsed,downloaded,proxy=pl_cfg.roll_downloader_proxy(platform)))
            caption=html.escape(f'{item.get("title") or pid}\n用户 ID：{uid}\n{canonical}')[:1000]
            async with self.send_locks.setdefault(job['chat_id'],asyncio.Lock()):
                old=self.store.delivery(platform,uid,pid,job['chat_id']) or {}
                async def checkpoint(count,ids):
                    messages=await self.cli.get_messages(job['chat_id'],ids)
                    if not isinstance(messages,list):messages=[messages]
                    if len(messages)!=len(ids) or any(not m or getattr(m,'empty',False) for m in messages):
                        raise RuntimeError('Telegram 分批上传回读失败')
                    self.store.set_delivery(platform,uid,pid,job['chat_id'],status='running',message_ids=ids,completed_batches=count)
                sent=await send_download(sender,downloaded,item,caption,_t,progress=old,checkpoint=checkpoint)
                ids=[msg.id for msg in sent]
                readback=await self.cli.get_messages(job['chat_id'],ids)
                if not isinstance(readback,list):readback=[readback]
                if len(readback)!=len(ids) or any(not msg or getattr(msg,'empty',False) for msg in readback):
                    raise RuntimeError('Telegram 上传回读失败')
                self.store.mark_delivered(platform,uid,pid,job['chat_id'],ids)
        if collection_errors:
            raise CollectionPending('; '.join(collection_errors))
        self.store.set_item(platform,uid,pid,status='complete',last_error='',local_dir='')
        shutil.rmtree(work,ignore_errors=True)


_manager=None

def start_profile_manager(cli):
    global _manager
    if _manager is None:_manager=ProfileManager(cli)
    return _manager

async def stop_profile_manager():
    global _manager
    if _manager is not None:await _manager.stop();_manager=None

async def maybe_submit_profile(req):
    return await start_profile_manager(req.cli).submit(req)
