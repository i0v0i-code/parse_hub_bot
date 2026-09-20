"""Owner-private, reply-bound SMS handoff. Codes are never persisted/logged."""
import asyncio
import re
import time
from dataclasses import dataclass,field
from services.owner_policy import message_principal
from services.profile_login import login_status,_call

@dataclass
class Pending:
 sid:str
 chat_id:int
 prompt_id:int
 requester_id:int
 deadline:float
 lock:asyncio.Lock=field(default_factory=asyncio.Lock)
 last_message_id:int=0
 last_resend:float=0
 socket:str|None=None

_pending={}
PROMPT='短信验证｜'

async def wait_for_login(sid,job,sender,cli,store,jid,poll=5.0,timeout=300):
    from pyrogram.types import ForceReply
    deadline=time.monotonic()+timeout
    key=None
    try:
        while time.monotonic()<deadline:
            result=await login_status(sid)
            state=result.get('status')
            if state in ('logged_in','expired','cancelled','not_found'):
                return result
            if state=='sms_required' and key is None:
                prompt=await sender.text_no_preview(
                    PROMPT+f'任务 #{jid}\n扫码后平台要求短信验证码。\n请直接回复本条消息，发送短信中的 6 位数字。\n验证码错误可回复新码；需要重新发送短信，请回复「重发」。\n只接受你在本私聊的回复；验证码不写入应用日志或任务数据库。',
                    reply_markup=ForceReply(selective=True,placeholder='回复6位验证码，或“重发”'))
                back=await cli.get_messages(job['chat_id'],prompt.id)
                if not back or getattr(back,'empty',False) or not (getattr(back,'text','') or '').startswith(PROMPT):
                    raise RuntimeError('sms_prompt_readback_failed')
                deadline=time.monotonic()+300
                key=(job['chat_id'],prompt.id)
                _pending[key]=Pending(sid,job['chat_id'],prompt.id,job['chat_id'],deadline)
                store.set_job(jid,login_state='waiting_sms',login_message_id=prompt.id)
            await asyncio.sleep(poll)
        return {'status':'timed_out'}
    finally:
        if key is not None:_pending.pop(key,None)

async def handle_reply(cli,msg):
    principal=message_principal(msg)
    if principal is None:return False
    if any(getattr(msg,k,None) for k in ('forward_date','forward_origin','forward_from','sender_chat')):return False
    reply=getattr(msg,'reply_to_message',None)
    if reply is None:return False
    key=(msg.chat.id,reply.id)
    pending=_pending.get(key)
    if pending is None:
        me=getattr(cli,'me',None);author=getattr(reply,'from_user',None)
        if me and author and author.id==me.id and (getattr(reply,'text','') or '').startswith(PROMPT):
            await msg.reply_text('这次验证等待已结束或机器人已重启。请重新发送主页链接，获取当前登录提示。')
            return True
        return False
    if msg.from_user.id!=pending.requester_id:return False
    if time.monotonic()>=pending.deadline:
        await msg.reply_text('验证码等待已超时，请重新发送主页链接。');return True
    value=(msg.text or '').strip()
    if value!='重发' and not re.fullmatch(r'[0-9]{6}',value):
        await msg.reply_text('请回复本条短信验证提示，发送 6 位数字；重新获取短信请回复「重发」。');return True
    if pending.lock.locked():
        await msg.reply_text('正在验证上一条输入，请稍候。');return True
    async with pending.lock:
        if msg.id<=pending.last_message_id:return True
        pending.last_message_id=msg.id
        if value=='重发' and time.monotonic()-pending.last_resend<60:
            await msg.reply_text('短信重发间隔至少 60 秒，请稍后再试。');return True
        try:
            if value=='重发':
                result=await _call({'op':'login_resend_code','session_id':pending.sid},timeout=30,socket=pending.socket)
                if result.get('code_result')=='resent':pending.last_resend=time.monotonic()
            else:
                result=await _call({'op':'login_submit_code','session_id':pending.sid,'code':value},timeout=45,socket=pending.socket)
            state=result.get('status');reason=result.get('code_result')
            if state=='logged_in':text='✅ 验证通过，登录已确认，原任务将自动继续。'
            elif reason=='resent':text='已请求重新发送短信。收到新验证码后，请回复上方「短信验证」提示。'
            elif reason=='expired':text='验证码已过期。请回复上方提示「重发」，收到新码后再回复。'
            elif reason=='incorrect':text='验证码不正确，请核对最新短信，并回复上方提示重试。'
            elif reason=='rate_limited':text='平台暂时限制了验证频率，请稍后再试。'
            elif state=='verification_required':text='验证码已提交，平台还要求使用已登录的小红书 App 扫码安全验证；二维码将发送到本私聊。确认前不会替换旧 Cookie。'
            elif reason=='submitted':text='验证码已提交，正在等待平台确认；尚未确认登录成功。'
            elif state in ('expired','not_found','cancelled'):text='登录会话已结束，请重新发送主页链接。'
            else:text='当前页面无法提交短信验证，任务仍保留；请稍后重试或重新发送主页链接。'
            await msg.reply_text(text)
        except Exception:
            # Never include an exception/Playwright payload containing the code.
            await msg.reply_text('验证请求未完成，尚未确认登录成功。请稍后回复上方提示重试。')
        finally:
            value=None
    return True