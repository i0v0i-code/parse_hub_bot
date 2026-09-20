"""Private, durable profile workflow state (stdlib only)."""
import json
import os
import re
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qs
from zoneinfo import ZoneInfo


def _now():
    return datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()


class RetryRejected(ValueError):
    """A management retry was rejected without changing persistent state."""

    def __init__(self, code, message):
        self.code = str(code)
        super().__init__(message)


class ProfileStore:
    JOB_FIELDS = set('status started_at ended_at enumeration_complete enumeration_reason enumeration_evidence pages progress_message_id new_count skipped_count failed_count last_error retry_at login_state login_message_id login_expires_at'.split())
    ITEM_FIELDS = set('title canonical_url url source_url is_video status download_status archive_status local_dir files attempts download_attempts archive_attempts last_error collection_files'.split())

    def __init__(self, db_path, recover=False):
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.fchmod(fd, 0o600)
        os.close(fd)
        self.db = sqlite3.connect(str(path), timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=DELETE')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS profiles(platform TEXT,user_id TEXT,data TEXT NOT NULL,PRIMARY KEY(platform,user_id));
        CREATE TABLE IF NOT EXISTS jobs(id INTEGER PRIMARY KEY AUTOINCREMENT,platform TEXT,user_id TEXT,status TEXT NOT NULL,data TEXT NOT NULL);
        DROP INDEX IF EXISTS active_profile_job;
        CREATE TABLE IF NOT EXISTS items(platform TEXT,user_id TEXT,post_id TEXT,data TEXT NOT NULL,PRIMARY KEY(platform,user_id,post_id));
        CREATE TABLE IF NOT EXISTS deliveries(platform TEXT,user_id TEXT,post_id TEXT,chat_id TEXT,data TEXT NOT NULL,PRIMARY KEY(platform,user_id,post_id,chat_id));
        CREATE TABLE IF NOT EXISTS profile_scan_cache(job_id TEXT,cache_key TEXT,data TEXT NOT NULL,PRIMARY KEY(job_id,cache_key));
        ''')
        if recover:
            self.recover()

    def close(self):
        self.db.close()

    @staticmethod
    def _keys(*keys):
        return tuple(str(k) for k in keys)

    def _get(self, table, columns, keys):
        row = self.db.execute('SELECT data FROM '+table+' WHERE '+' AND '.join(c+'=?' for c in columns), keys).fetchone()
        return json.loads(row['data']) if row else None

    def _put(self, table, columns, keys, data):
        self.db.execute('INSERT OR REPLACE INTO '+table+' ('+','.join(columns)+',data) VALUES ('+','.join('?' for _ in range(len(keys)+1))+')', (*keys, json.dumps(data, ensure_ascii=False)))

    def upsert_profile(self, platform, user_id, username, public_id=None, numeric_uid=None):
        keys = self._keys(platform, user_id)
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            data = self._get('profiles', ['platform','user_id'], keys) or dict(platform=keys[0], user_id=keys[1], username='', aliases=[])
            if public_id is not None:
                public_id = str(public_id)
                if not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', public_id) or public_id in ('.','..'):
                    raise ValueError('未能安全识别公开抖音号/小红书号，本次未下载')
                folder_id = data.get('folder_id') or (public_id + '_' if public_id.endswith('.') else public_id)
                for row in self.db.execute('SELECT user_id,data FROM profiles WHERE platform=?', (keys[0],)):
                    other = json.loads(row['data'])
                    if row['user_id'] != keys[1] and other.get('folder_id') == folder_id:
                        raise ValueError('公开账号目录已映射至其他内部用户 ID，本次未下载')
                data.update(public_id=public_id, folder_id=folder_id)
            if numeric_uid:
                data['numeric_uid'] = str(numeric_uid)
            if username:
                data['username'] = str(username)
                if str(username) not in data['aliases']:
                    data['aliases'].append(str(username))
            data['updated_at'] = _now()
            self._put('profiles', ['platform','user_id'], keys, data)
        return data

    def enqueue(self, platform, user_id, url, chat_id, message_id, thread_id=None, *, requester_id=None, request_chat_type=None):
        platform, user_id = self._keys(platform,user_id)
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            row = self.db.execute("SELECT id FROM jobs WHERE platform=? AND user_id=? AND status IN ('queued','running') AND json_extract(data,'$.chat_id')=?", (platform,user_id,int(chat_id))).fetchone()
            if row:
                return self.job(row['id']), False
            data = dict(platform=platform,user_id=user_id,url=str(url),chat_id=int(chat_id),message_id=int(message_id),thread_id=None if thread_id is None else int(thread_id),status='queued',created_at=_now(),started_at=None,ended_at=None,enumeration_complete=False,enumeration_reason='',pages=0,progress_message_id=None,new_count=0,skipped_count=0,failed_count=0,last_error='')
            data.update(requester_id=requester_id, request_chat_type=request_chat_type)
            cursor = self.db.execute('INSERT INTO jobs(platform,user_id,status,data) VALUES (?,?,?,?)', (platform,user_id,'queued',json.dumps(data)))
            data['id'] = str(cursor.lastrowid)
            self.db.execute('UPDATE jobs SET data=? WHERE id=?', (json.dumps(data),data['id']))
        return data, True

    def merge_collections(self, platform, user_id, collections):
        keys=self._keys(platform,user_id)
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            profile=self.profile(*keys)
            if profile is None:raise ValueError('用户尚未登记')
            saved=profile.setdefault('collections',{})
            for collection in collections:
                key=collection['id']
                if not re.fullmatch(r'(season|series)_[0-9]+',key):raise ValueError('合集 ID 无效')
                name=re.sub(r'[\\/:*?"<>|\x00-\x1f]','_',collection['name']).strip('. ')[:48] or '未命名'
                old=saved.get(key,{})
                saved[key]={**collection,'folder':old.get('folder') or key+'_'+name}
            self._put('profiles',['platform','user_id'],keys,profile)
        return saved

    def job(self, id):
        return self._get('jobs', ['id'], (str(id),))

    def scan_cache(self, job_id, key):
        return self._get('profile_scan_cache', ['job_id','cache_key'], self._keys(job_id,key))

    def save_scan_cache(self, job_id, key, data):
        with self.db:
            self._put('profile_scan_cache', ['job_id','cache_key'], self._keys(job_id,key),data)

    def list_jobs(self, *, platform=None, status=None, chat_id=None, limit=6, offset=0):
        """Return filtered jobs in stable ID order and the matching total."""
        limit = min(max(int(limit), 1), 20)
        offset = max(int(offset), 0)
        where = []
        args = []
        if platform:
            where.append('platform=?')
            args.append(str(platform))
        if status:
            where.append('status=?')
            args.append(str(status))
        if chat_id is not None:
            where.append("CAST(json_extract(data,'$.chat_id') AS TEXT)=?")
            args.append(str(chat_id))
        clause = (' WHERE ' + ' AND '.join(where)) if where else ''
        total = self.db.execute('SELECT COUNT(*) FROM jobs' + clause, args).fetchone()[0]
        rows = self.db.execute(
            'SELECT id,status,data FROM jobs' + clause + ' ORDER BY id LIMIT ? OFFSET ?',
            (*args, limit, offset),
        ).fetchall()
        jobs = []
        for row in rows:
            data = json.loads(row['data'])
            # The relational row is authoritative if an older writer left JSON stale.
            data['id'] = int(row['id'])
            data['status'] = row['status']
            jobs.append(data)
        return jobs, int(total)

    def job_stats(self, job):
        """Summarize persisted download/archive/Telegram state for a job's account."""
        if not isinstance(job, dict):
            job = self.job(job)
        if not job:
            raise KeyError(job)
        platform, user_id = self._keys(job.get('platform'), job.get('user_id'))
        items = self.items(platform, user_id)
        delivery_rows = self.db.execute(
            'SELECT data FROM deliveries WHERE platform=? AND user_id=? AND chat_id=?',
            (platform, user_id, str(job.get('chat_id'))),
        ).fetchall()
        delivery = Counter(json.loads(row['data']).get('status', 'pending') for row in delivery_rows)
        return {
            'known_items': len(items),
            'skipped_items': sum(item.get('status') == 'skipped' for item in items),
            'download': dict(Counter(item.get('download_status', 'pending') for item in items)),
            'archive': dict(Counter(item.get('archive_status', 'pending') for item in items)),
            'delivery': dict(delivery),
        }

    def profile(self, platform, user_id):
        return self._get('profiles', ['platform','user_id'], self._keys(platform,user_id))

    def folder_id(self, platform, user_id):
        value = (self.profile(platform,user_id) or {}).get('folder_id')
        if not value:
            raise ValueError('未能识别公开抖音号/小红书号，禁止使用内部 ID 创建目录')
        return value

    def queued_jobs(self):
        import time
        return [d for r in self.db.execute("SELECT data FROM jobs WHERE status='queued' ORDER BY id")
                if (d := json.loads(r['data'])).get('retry_at',0) <= time.time()]

    def resume_login_jobs(self, platform, chat_id):
        """Requeue the latest auth-blocked job per profile; retain all media state."""
        if platform not in {'xhs', 'douyin', 'bilibili'}:
            return []
        resumed = []
        auth_reasons = {'login_required', 'login_expired', 'login_cancelled',
                        'login_not_found', 'login_flow_failed', 'login_interrupted',
                        'waiting_scan', 'cookie_expired', 'cookie_invalid'}
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            rows = self.db.execute(
                "SELECT id,status,user_id,data FROM jobs WHERE platform=? "
                "AND CAST(json_extract(data,'$.chat_id') AS TEXT)=? ORDER BY id DESC",
                (platform, str(chat_id)),
            ).fetchall()
            seen = set()
            active = {r['user_id'] for r in rows if r['status'] in ('queued', 'running')}
            for row in rows:
                uid = row['user_id']
                if uid in seen:
                    continue
                seen.add(uid)
                if uid in active or row['status'] not in ('incomplete', 'failed'):
                    continue
                job = json.loads(row['data'])
                if job.get('enumeration_reason') not in auth_reasons:
                    continue
                if not job.get('url') or not job.get('message_id'):
                    continue
                # Legacy provenance is verified from Telegram by _run before work.
                if 'requester_id' in job and (
                    job.get('requester_id') != chat_id or job.get('request_chat_type') != 'private'
                ):
                    continue
                job.update(status='queued', retry_at=0, ended_at=None,
                           login_state='logged_in', login_expires_at=None,
                           last_error='', enumeration_reason='登录成功，自动重试',
                           enumeration_complete=False)
                self.db.execute('UPDATE jobs SET status=?,data=? WHERE id=?',
                                ('queued', json.dumps(job, ensure_ascii=False), row['id']))
                resumed.append(str(row['id']))
        return list(reversed(resumed))

    def retry_job(self, id, *, requester_id, request_chat_type, requested_message_id=None):
        """Create one queued retry while preserving successful shared records."""
        try:
            job_id = int(id)
        except (TypeError, ValueError):
            raise RetryRejected('invalid_id', '任务编号无效') from None
        if type(requester_id) is not int or request_chat_type != 'private':
            raise RetryRejected('owner_only', '任务重试仅限主人私聊使用')
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            row = self.db.execute('SELECT id,platform,user_id,status,data FROM jobs WHERE id=?', (job_id,)).fetchone()
            if row is None:
                raise RetryRejected('not_found', f'未找到任务 #{job_id}')
            status = row['status']
            if status in ('queued', 'running'):
                raise RetryRejected('active', f'任务 #{job_id} 已在队列或执行中，未创建重复任务')
            if status not in ('failed', 'incomplete'):
                raise RetryRejected('not_retryable', f'任务 #{job_id} 当前状态不可重试')
            old = json.loads(row['data'])
            if not old.get('url') or not old.get('chat_id') or not old.get('message_id'):
                raise RetryRejected('missing_source', '任务缺少原始主页消息，无法安全重试')
            for child_row in self.db.execute('SELECT id,status,data FROM jobs WHERE id<>?', (job_id,)):
                child = json.loads(child_row['data'])
                if str(child.get('retry_of')) == str(job_id) and child_row['status'] in ('queued', 'running'):
                    raise RetryRejected('active', f'任务 #{job_id} 已有重试任务在队列或执行中')

            # Failed stages become pending, but successful archive/download stages,
            # files, attempts and successful Telegram deliveries are retained.
            for item_row in self.db.execute(
                'SELECT platform,user_id,post_id,data FROM items WHERE platform=? AND user_id=?',
                (row['platform'], row['user_id']),
            ).fetchall():
                item = json.loads(item_row['data'])
                if item.get('status') == 'skipped':
                    continue
                changed = False
                if item.get('status') != 'complete' or item.get('download_status') != 'succeeded' or item.get('archive_status') != 'succeeded':
                    if item.get('status') != 'pending':
                        item['status'] = 'pending'
                        changed = True
                    if item.get('download_status') != 'succeeded' and item.get('download_status') != 'pending':
                        item['download_status'] = 'pending'
                        changed = True
                    if item.get('archive_status') != 'succeeded' and item.get('archive_status') != 'pending':
                        item['archive_status'] = 'pending'
                        changed = True
                    if item.get('last_error'):
                        item['last_error'] = ''
                        changed = True
                if changed:
                    item['updated_at'] = _now()
                    self._put(
                        'items', ['platform', 'user_id', 'post_id'],
                        (item_row['platform'], item_row['user_id'], item_row['post_id']), item,
                    )

            for delivery_row in self.db.execute(
                'SELECT platform,user_id,post_id,chat_id,data FROM deliveries WHERE platform=? AND user_id=? AND chat_id=?',
                (row['platform'], row['user_id'], str(old['chat_id'])),
            ).fetchall():
                delivery = json.loads(delivery_row['data'])
                if delivery.get('status') == 'succeeded':
                    continue
                delivery.update(status='pending', last_error='', updated_at=_now())
                self._put(
                    'deliveries', ['platform', 'user_id', 'post_id', 'chat_id'],
                    (delivery_row['platform'], delivery_row['user_id'], delivery_row['post_id'], delivery_row['chat_id']),
                    delivery,
                )

            now = _now()
            new_data = dict(
                platform=row['platform'], user_id=row['user_id'], url=str(old['url']),
                chat_id=int(old['chat_id']), message_id=int(old['message_id']),
                thread_id=old.get('thread_id'), status='queued', created_at=now,
                started_at=None, ended_at=None, enumeration_complete=False,
                enumeration_reason='手动重试，等待读取列表', enumeration_evidence=[], pages=0,
                progress_message_id=None, new_count=0, skipped_count=0, failed_count=0,
                last_error='', retry_at=0, login_state='', login_message_id=None,
                login_expires_at=None, requester_id=requester_id,
                request_chat_type=request_chat_type, retry_of=job_id,
                retry_count=int(old.get('retry_count', 0)) + 1,
                retry_requested_at=now,
            )
            if requested_message_id is not None:
                new_data['retry_requested_message_id'] = int(requested_message_id)
            cursor = self.db.execute(
                'INSERT INTO jobs(platform,user_id,status,data) VALUES (?,?,?,?)',
                (row['platform'], row['user_id'], 'queued', json.dumps(new_data, ensure_ascii=False)),
            )
            new_data['id'] = str(cursor.lastrowid)
            self.db.execute('UPDATE jobs SET data=? WHERE id=?', (json.dumps(new_data, ensure_ascii=False), cursor.lastrowid))
            return new_data

    def set_job(self, id, **fields):
        if fields.keys() - self.JOB_FIELDS:
            raise ValueError('Unknown job fields')
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            data = self.job(id)
            if data is None:
                raise KeyError(id)
            if fields.get('status') == 'running' and not data['started_at']:
                fields.setdefault('started_at', _now())
            if 'status' in fields and fields['status'] not in ('queued','running'):
                fields.setdefault('ended_at', _now())
            data.update(fields)
            self.db.execute('UPDATE jobs SET status=?,data=? WHERE id=?', (data['status'],json.dumps(data),str(id)))
        return data

    def merge_items(self, platform, user_id, items):
        platform,user_id = self._keys(platform,user_id)
        added = existing = 0
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            for incoming in items:
                pid = incoming.get('post_id', incoming.get('id'))
                if pid is None:
                    raise ValueError('Item requires id/post_id')
                pid = str(pid)
                data = self.item(platform,user_id,pid)
                if data is None:
                    added += 1
                    data = dict(platform=platform,user_id=user_id,post_id=pid,id=pid,title='',canonical_url='',url='',source_url='',is_video=True,status='pending',download_status='pending',archive_status='pending',local_dir='',files=[],attempts=0,download_attempts=0,archive_attempts=0,last_error='',created_at=_now())
                else:
                    existing += 1
                for key in ('title','canonical_url','is_video','bvid','cid','page','owner_uid','memberships','unavailable_reason'):
                    if key in incoming:
                        data[key] = sorted(set(data.get(key,[]))|set(incoming[key])) if key=='memberships' else incoming[key]
                if 'url' in incoming or 'source_url' in incoming:
                    data['url'] = data['source_url'] = incoming.get('source_url') or incoming.get('url') or ''
                data['canonical_url'] = self._canonical(data.get('url') if platform=='bilibili' else data.get('canonical_url') or data.get('url'))
                if platform=='bilibili' and incoming.get('unavailable_reason') and data['archive_status']!='succeeded':
                    data.update(status='skipped',last_error=incoming['unavailable_reason'])
                elif platform=='bilibili' and data['status']=='skipped' and not incoming.get('unavailable_reason'):
                    data.update(status='pending',last_error='')
                data['updated_at'] = _now()
                self._put('items',['platform','user_id','post_id'],(platform,user_id,pid),data)
            total = self.db.execute('SELECT COUNT(*) FROM items WHERE platform=? AND user_id=?',(platform,user_id)).fetchone()[0]
        return dict(new_count=added,existing_count=existing,total_count=total)

    def delivery(self, platform, user_id, post_id, chat_id):
        return self._get('deliveries',['platform','user_id','post_id','chat_id'],self._keys(platform,user_id,post_id,chat_id))

    def delivered(self, platform, user_id, post_id, chat_id):
        data = self.delivery(platform,user_id,post_id,chat_id)
        return bool(data and data['status'] == 'succeeded')

    def set_delivery(self, platform, user_id, post_id, chat_id, status='failed', last_error='', attempts=1, message_ids=None, completed_batches=0):
        keys = self._keys(platform,user_id,post_id,chat_id)
        data = dict(zip(('platform','user_id','post_id','chat_id'),keys))
        data.update(status=status,last_error=str(last_error),attempts=attempts,message_ids=list(message_ids or []),completed_batches=completed_batches,updated_at=_now())
        with self.db:
            self._put('deliveries',['platform','user_id','post_id','chat_id'],keys,data)
        return data

    def mark_delivered(self, platform, user_id, post_id, chat_id, message_ids):
        old = self.delivery(platform,user_id,post_id,chat_id)
        return self.set_delivery(platform,user_id,post_id,chat_id,status='succeeded',message_ids=message_ids,attempts=(old or {}).get('attempts',0)+1)

    def pending_items(self, platform, user_id, chat_id):
        return [i for i in self.items(platform,user_id) if i.get('status')!='skipped' and not (i['archive_status']=='succeeded' and self.delivered(platform,user_id,i['post_id'],chat_id) and set(i.get('memberships',[]))<=set(i.get('collection_files',{})))]

    def needs_download(self, platform, user_id, post_id):
        data = self.item(platform,user_id,post_id)
        if not data or data['download_status'] != 'succeeded' or not data['files']:
            return True
        for file in data['files']:
            local = file.get('local_path')
            if not local and data['local_dir'] and file.get('name'):
                local = str(Path(data['local_dir']) / file['name'])
            if not local:
                local = file.get('path')
            try:
                path = Path(local)
                if not path.is_file() or 'size' not in file or path.stat().st_size != int(file['size']):
                    return True
            except (OSError,TypeError,ValueError):
                return True
        return False

    def recover(self):
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            for row in self.db.execute("SELECT id,data FROM jobs WHERE status='running'").fetchall():
                data = json.loads(row['data'])
                data.update(status='queued',ended_at=None)
                self.db.execute("UPDATE jobs SET status='queued',data=? WHERE id=?",(json.dumps(data),row['id']))
            for row in self.db.execute('SELECT platform,user_id,post_id,data FROM items').fetchall():
                data = json.loads(row['data'])
                changed = False
                for field in ('status','download_status','archive_status'):
                    if data.get(field) in ('running','downloading'):
                        data[field] = 'pending'
                        changed = True
                if changed:
                    self._put('items',['platform','user_id','post_id'],(row['platform'],row['user_id'],row['post_id']),data)

    @staticmethod
    def _safe_text(value):
        text = re.sub(r'https?://[^\s<>]+', '[链接]', str(value or ''), flags=re.I)
        text = re.sub(r'(?i)\b(?:token|access_token|auth|authorization|signature|sig|cookie|password|secret)\s*[:=]\s*[^\s,;]+', '[已隐藏]', text)
        return ''.join(c for c in text if c in '\n\t' or ord(c) >= 32)

    @staticmethod
    def _canonical(value):
        try:
            parts = urlsplit(str(value or ''))
            if parts.scheme not in ('http','https') or not parts.hostname:
                return ''
            host = parts.hostname
            if ':' in host:
                host = '['+host+']'
            if parts.port:
                host += ':'+str(parts.port)
            query=''
            if parts.hostname in ('www.bilibili.com','bilibili.com') and re.fullmatch(r'/video/BV[A-Za-z0-9]{10}/?',parts.path):
                p=parse_qs(parts.query).get('p',[''])[0]
                if re.fullmatch(r'[1-9][0-9]*',p):query='p='+p
            return urlunsplit((parts.scheme,host,parts.path,query,''))
        except ValueError:
            return ''

    def render_report(self, platform, user_id, job_id):
        job = self.job(job_id)
        if job is None or (job['platform'],job['user_id']) != self._keys(platform,user_id):
            raise KeyError(job_id)
        profile = self._get('profiles',['platform','user_id'],self._keys(platform,user_id)) or {}
        items = self.items(platform,user_id)
        deliveries = [self.delivery(platform,user_id,i['post_id'],job['chat_id']) or {} for i in items]
        safe = self._safe_text
        labels = dict(queued='等待重试/执行',running='处理中',pending='待处理',downloading='下载中',archived='已归档',complete='成功',completed='完成',incomplete='部分完成',succeeded='成功',failed='失败',skipped='跳过')
        lines = [f'用户作品下载记录｜任务 #{safe(job_id)}',f'平台：{safe(platform)}｜用户 ID：{safe(user_id)}',f'用户名：{safe(profile.get("username"))}',f'用户名历史：{safe("、".join(profile.get("aliases",[])))}',f'开始时间：{safe(job.get("started_at")) or "未开始"}',f'结束时间：{safe(job.get("ended_at")) or "未结束"}',f'状态：{labels.get(job["status"],safe(job["status"]))}',f'列表读取：{"可见作品分页结束" if job["enumeration_complete"] else "未完成"}｜页数：{job["pages"]}',f'读取说明：{safe(job["enumeration_reason"])}',f'任务错误：{safe(job["last_error"])}',f'作品总数：{len(items)}']
        lines.insert(3, f'公开账号：{safe(profile.get("public_id"))}｜归档目录账号：{safe(profile.get("folder_id"))}｜数字 UID：{safe(profile.get("numeric_uid"))}')
        for label,values in [('下载',[i['download_status'] for i in items]),('归档',[i['archive_status'] for i in items]),('回传',[d.get('status','pending') for d in deliveries])]:
            lines.append(f'{label}成功：{values.count("succeeded")}｜失败：{values.count("failed")}｜其他：{sum(v not in ("succeeded","failed") for v in values)}')
        lines.append(f'本轮新增：{job["new_count"]}｜跳过：{job["skipped_count"]}｜失败：{job["failed_count"]}')
        for item,delivery in zip(items,deliveries):
            if platform=='bilibili':
                lines.extend(['',f'BVID：{safe(item.get("bvid"))}｜CID：{safe(item.get("cid"))}｜分P：{safe(item.get("page"))}｜主作者 UID：{safe(item.get("owner_uid"))}',f'合集/列表：{safe("、".join(item.get("memberships",[])))}｜副本完成：{safe("、".join(item.get("collection_files",{})))}'])
            lines.extend(['',f'{"视频" if item["is_video"] else "图文"}｜{safe(item["post_id"])}｜{safe(item["title"])}',self._canonical(item['canonical_url']),f'状态：{labels.get(item["status"],safe(item["status"]))}｜下载：{labels.get(item["download_status"],safe(item["download_status"]))}｜归档：{labels.get(item["archive_status"],safe(item["archive_status"]))}｜回传：{labels.get(delivery.get("status","pending"),safe(delivery.get("status")))}',f'作品错误：{safe(item["last_error"])}',f'回传错误：{safe(delivery.get("last_error"))}'])
        return '\n'.join(lines)+'\n'

    def items(self, platform, user_id):
        return [json.loads(r['data']) for r in self.db.execute('SELECT data FROM items WHERE platform=? AND user_id=? ORDER BY rowid', self._keys(platform,user_id))]

    def item(self, platform, user_id, post_id):
        return self._get('items',['platform','user_id','post_id'],self._keys(platform,user_id,post_id))

    def set_item(self, platform, user_id, post_id, **fields):
        if fields.keys() - self.ITEM_FIELDS:
            raise ValueError('Unknown item fields')
        keys = self._keys(platform,user_id,post_id)
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            data = self.item(*keys)
            if data is None:
                raise KeyError(post_id)
            if 'source_url' in fields or 'url' in fields:
                fields['url'] = fields['source_url'] = fields.get('source_url', fields.get('url'))
            data.update(fields)
            data['updated_at'] = _now()
            self._put('items',['platform','user_id','post_id'],keys,data)
        return data
