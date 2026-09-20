"""Owner-private /relogin menu, native QR and isolated polling."""
import asyncio,hashlib,io,secrets,time
from pyrogram import Client,filters
from pyrogram.types import InlineKeyboardMarkup,InlineKeyboardButton,ForceReply
from services.owner_policy import message_principal,owner_id
from services.profile_login import _call,qr_bytes
from services.login_verification import Pending,_pending,PROMPT

SOCKET='/run/parse-hub-browser/relogin.sock'
LABELS={'bilibili':'B站'}
_menus={}
_active={}

def permitted(msg,user_id=None):
    if user_id is None:return message_principal(msg) is not None
    kind=getattr(getattr(msg.chat,'type',None),'value',getattr(msg.chat,'type',None))
    return kind=='private' and msg.chat.id==user_id==owner_id()

@Client.on_message(filters.command('relogin'),group=-30)
async def relogin(cli,msg):
    if not permitted(msg):
        await msg.reply_text('重新登录仅限主人私聊使用。');msg.stop_propagation();return
    if msg.chat.id in _active:
        await msg.reply_text('已有重新登录正在进行，请完成或取消当前扫码。');msg.stop_propagation();return
    now=time.monotonic()
    for k,v in list(_menus.items()):
        if v['expires']<now:_menus.pop(k,None)
    token=secrets.token_hex(8)
    menu=await msg.reply_text('选择要重新登录的平台。\n当前作品继续使用旧 Cookie；登录成功后，新启动的作品使用新 Cookie。',reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(label,callback_data=f'rl:{token}:{p}') for p,label in LABELS.items()]]))
    _menus[token]={'chat':msg.chat.id,'message':menu.id,'expires':now+300}
    msg.stop_propagation()

@Client.on_callback_query(filters.regex(r'^rl:'),group=-30)
async def relogin_callback(cli,q):
    if not q.message or not permitted(q.message,q.from_user.id):
        await q.answer('仅限主人私聊',show_alert=True);return
    parts=q.data.split(':')
    if len(parts)!=3:return
    _,token,action=parts;menu=_menus.get(token)
    if not menu or menu['chat']!=q.message.chat.id or menu['message']!=q.message.id or menu['expires']<time.monotonic():
        await q.answer('菜单已过期，请重新发送 /relogin',show_alert=True);return
    chat=q.message.chat.id
    if action=='cancel':
        task=_active.get(chat)
        if task:task.cancel()
        await q.answer('正在取消');return
    if action not in LABELS:return
    if chat in _active:
        await q.answer('正在登录，请稍候');return
    await q.answer('正在获取二维码')
    await q.message.edit_text('正在获取独立登录二维码，原下载继续。',reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('取消重新登录',callback_data=f'rl:{token}:cancel')]]))
    task=asyncio.create_task(run_login(cli,q.message,action,token));_active[chat]=task

async def run_login(cli,msg,platform,token):
    if platform not in LABELS:
        return
    sid=None;key=None;chat=msg.chat.id
    security_qr_digest=None;security_notice_sent=False;security_extended=False;last_state=None
    try:
        r=await _call({'op':'login_start','platform':platform},socket=SOCKET,timeout=150);sid=r['session_id']
        raw=qr_bytes(r)
        if not raw:raise RuntimeError('missing_native_qr')
        image=io.BytesIO(raw);image.name='relogin.png'
        sent=await cli.send_photo(chat,image,caption=f'{LABELS[platform]}重新登录：请扫码并在 App 内确认。\n扫码失败或过期不会替换旧 Cookie。',reply_to_message_id=msg.id)
        back=await cli.get_messages(chat,sent.id)
        if not back or not getattr(back,'photo',None):raise RuntimeError('qr_photo_readback_failed')
        deadline=time.monotonic()+300
        while time.monotonic()<deadline:
            r=await _call({'op':'login_status','session_id':sid},socket=SOCKET,timeout=90);state=r.get('status');last_state=state
            if state=='logged_in':
                note = ''
                if platform in LABELS:
                    from services.profile_jobs import start_profile_manager
                    try:
                        resumed = start_profile_manager(cli).resume_after_login(platform, chat)
                        note = f'\n已自动将 {len(resumed)} 个因登录失效中断的任务加入队列，已成功的作品会跳过。'
                    except Exception:
                        note = '\n自动恢复任务失败，登录已保存；请使用 /tasks 查看并手动重试。'
                await cli.send_message(chat,f'✅ {LABELS[platform]}新登录已确认并保存。\n后续新启动的作品使用新 Cookie。{note}',reply_to_message_id=msg.id)
                return
            if state in ('expired','cancelled','not_found'):break
            if state=='sms_required' and key is None:
                prompt=await cli.send_message(chat,PROMPT+'重新登录\n请回复本条消息发送短信中的 6 位验证码，或回复「重发」。验证码不写日志或数据库。',reply_markup=ForceReply(selective=True))
                key=(chat,prompt.id);deadline=time.monotonic()+300
                _pending[key]=Pending(sid,chat,prompt.id,chat,deadline,socket=SOCKET)
            if state=='verification_required':
                if not security_notice_sent:
                    await cli.send_message(chat,'B站需要额外安全验证，请在 App 内完成验证。',reply_to_message_id=msg.id)
                    security_notice_sent=True
                if not security_extended:
                    deadline=max(deadline,time.monotonic()+300);security_extended=True
                await asyncio.sleep(3);continue
            await asyncio.sleep(3)
        if last_state=='verification_required':
            await cli.send_message(chat,'额外安全验证在等待期限内未完成，旧登录和下载队列保持不变。请重新发送 /relogin。')
        else:
            await cli.send_message(chat,'本次扫码已过期或未完成，旧登录和下载队列保持不变。请重新发送 /relogin。')
    except asyncio.CancelledError:
        await cli.send_message(chat,'已取消重新登录；未确认的新登录不会替换旧 Cookie。')
    except Exception:
        await cli.send_message(chat,'重新登录未完成，未确认的新登录不会替换旧 Cookie。请稍后发送 /relogin 重试。')
    finally:
        if key:_pending.pop(key,None)
        if sid:
            try:await _call({'op':'login_cancel','session_id':sid},socket=SOCKET,timeout=30)
            except Exception:pass
        _active.pop(chat,None);_menus.pop(token,None)
        try:await msg.edit_reply_markup(reply_markup=None)
        except Exception:pass
