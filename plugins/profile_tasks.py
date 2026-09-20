"""Owner-private profile-task list, detail and explicit retry controls."""
from __future__ import annotations

import re
import secrets
import time
from collections.abc import Iterable
from datetime import datetime
from zoneinfo import ZoneInfo

from pyrogram import Client, filters
from pyrogram.enums import ParseMode as TgParseMode
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    Message,
)

from log import logger
from services.owner_policy import message_principal, owner_id
from services.profile_jobs import start_profile_manager
from services.profile_store import ProfileStore, RetryRejected


PAGE_SIZE = 6
MENU_TTL = 600
CALLBACK_PREFIX = "tm:"

PLATFORM_LABELS = {"xhs": "小红书", "douyin": "抖音", "bilibili": "B站"}
PLATFORM_ALIASES = {
    "xhs": "xhs",
    "xiaohongshu": "xhs",
    "小红书": "xhs",
    "douyin": "douyin",
    "抖音": "douyin",
    "bilibili": "bilibili",
    "bili": "bilibili",
    "b站": "bilibili",
    "哔哩哔哩": "bilibili",
}
STATUS_LABELS = {
    "paused": "已暂停",
    "queued": "排队中",
    "running": "执行中",
    "completed": "完成",
    "incomplete": "部分完成",
    "failed": "失败",
}
STATUS_ALIASES = {
    "paused": "paused",
    "暂停": "paused",
    "已暂停": "paused",
    "queued": "queued",
    "queue": "queued",
    "排队": "queued",
    "排队中": "queued",
    "running": "running",
    "run": "running",
    "执行中": "running",
    "completed": "completed",
    "complete": "completed",
    "done": "completed",
    "success": "completed",
    "完成": "completed",
    "incomplete": "incomplete",
    "partial": "incomplete",
    "部分完成": "incomplete",
    "failed": "failed",
    "fail": "failed",
    "失败": "failed",
}
RETRYABLE_STATUSES = frozenset(("failed", "incomplete"))
REASON_LABELS = {
    "login_required": "需要登录",
    "login_expired": "登录已过期",
    "captcha_required": "需要验证",
    "pagination_stalled": "列表分页未确认结束",
    "browser_navigation_or_state_read_failed": "浏览器页面读取失败",
}
_menus: dict[str, dict] = {}


def _plain(value, limit=180):
    """Sanitize stored text and keep the UI free of URLs and credentials."""
    text = ProfileStore._safe_text(value).replace("\r", " ").replace("\n", " ").strip()
    return text[:limit] or "-"


def _time(value):
    if not value:
        return "未开始"
    try:
        return datetime.fromisoformat(str(value)).astimezone(ZoneInfo("Asia/Shanghai")).strftime("%m-%d %H:%M")
    except (TypeError, ValueError):
        return _plain(value, 32)


def _page_count(total):
    return max(1, (int(total) + PAGE_SIZE - 1) // PAGE_SIZE)


def _platform(value):
    return PLATFORM_LABELS.get(str(value), _plain(value, 24))


def _status(value):
    return STATUS_LABELS.get(str(value), _plain(value, 24))


def _account(store, job):
    profile = store.profile(job.get("platform"), job.get("user_id")) or {}
    return _plain(profile.get("public_id") or profile.get("username") or f"任务 #{job.get('id', '-')}", 80)


def _stage_counts(stats, key):
    known = int(stats.get("known_items", 0))
    skipped = int(stats.get("skipped_items", 0))
    counts = stats.get(key) or {}
    success = int(counts.get("succeeded", 0))
    failed = int(counts.get("failed", 0))
    expected = max(0, known - skipped)
    other = max(0, expected - success - failed)
    return success, failed, other


def _stage_line(stats, key, label):
    success, failed, other = _stage_counts(stats, key)
    return f"{label}：成功 {success}｜失败 {failed}｜待处理 {other}"


def _enum_line(job):
    if job.get("enumeration_complete"):
        return f"列表读取：已完成｜页数 {int(job.get('pages') or 0)}"
    return f"列表读取：未完成｜页数 {int(job.get('pages') or 0)}｜剩余作品数未知"


def _reason_line(job):
    reason = _plain(job.get("enumeration_reason"), 220)
    if reason == "-":
        return None
    label = REASON_LABELS.get(str(job.get("enumeration_reason")), reason)
    return f"读取说明：{label}"


def _login_hint(job):
    if job.get("platform") in {"xhs", "douyin"}:
        return "该平台的主页批量功能已停用，历史记录保留；请发送单个作品链接。"
    reason = str(job.get("enumeration_reason") or "")
    state = str(job.get("login_state") or "")
    if reason in {"login_required", "login_expired"} or state in {"login_required", "expired"}:
        if job.get("platform") in {"xhs", "douyin", "bilibili"}:
            return "操作提示：发送 /relogin 登录对应平台，成功后因登录失效中断的任务会自动加入队列。"
        return "操作提示：先发送 /relogin，选择对应平台登录成功后，再使用 /retry 任务编号。"
    if reason == "captcha_required":
        return "操作提示：先完成平台验证或发送 /relogin，再使用 /retry 任务编号。"
    return None


def render_task_list(store, jobs: Iterable[dict], total, page, *, platform=None, status=None):
    pages = _page_count(total)
    page = max(1, min(int(page), pages))
    filter_text = []
    if platform:
        filter_text.append(_platform(platform))
    if status:
        filter_text.append(_status(status))
    suffix = f"｜筛选：{' / '.join(filter_text)}" if filter_text else ""
    lines = [f"用户下载任务｜第 {page}/{pages} 页｜共 {int(total)} 个{suffix}"]
    if not jobs:
        lines.append("没有符合条件的任务。")
        return "\n".join(lines)
    for job in jobs:
        stats = store.job_stats(job)
        known = int(stats.get("known_items", 0))
        archive_success, archive_failed, _ = _stage_counts(stats, "archive")
        delivery_success, delivery_failed, _ = _stage_counts(stats, "delivery")
        lines.append("")
        lines.append(f"#{_plain(job.get('id'), 16)}｜{_platform(job.get('platform'))}｜{_account(store, job)}")
        lines.append(
            f"状态：{_status(job.get('status'))}｜已知作品 {known}｜"
            f"WebDAV归档 成功 {archive_success}/失败 {archive_failed}｜"
            f"TG回传 成功 {delivery_success}/失败 {delivery_failed}"
        )
        if not job.get("enumeration_complete"):
            lines.append("列表读取：未完成，剩余作品数未知")
    return "\n".join(lines)


def render_task_detail(store, job):
    stats = store.job_stats(job)
    profile = store.profile(job.get("platform"), job.get("user_id")) or {}
    known = int(stats.get("known_items", 0))
    skipped = int(stats.get("skipped_items", 0))
    lines = [
        f"用户下载任务 #{_plain(job.get('id'), 16)}",
        f"平台：{_platform(job.get('platform'))}",
        f"账号：{_account(store, job)}",
        f"状态：{_status(job.get('status'))}",
        f"创建：{_time(job.get('created_at'))}｜开始：{_time(job.get('started_at'))}｜结束：{_time(job.get('ended_at'))}",
        _enum_line(job),
    ]
    if job.get("retry_of") is not None:
        lines.append(f"重试自：任务 #{_plain(job.get('retry_of'), 16)}｜本次重试计数：{int(job.get('retry_count') or 0)}")
    if profile.get("username") and profile.get("public_id"):
        lines.append(f"昵称：{_plain(profile.get('username'), 80)}")
    if reason := _reason_line(job):
        lines.append(reason)
    if hint := _login_hint(job):
        lines.append(hint)
    lines.extend(
        [
            "",
            f"本次尝试计数：完成 {int(job.get('new_count') or 0)}｜跳过 {int(job.get('skipped_count') or 0)}｜失败 {int(job.get('failed_count') or 0)}",
            f"已知作品记录：{known}（含历史任务持久化记录）" + ("；列表未完成，剩余未知" if not job.get("enumeration_complete") else ""),
            _stage_line(stats, "download", "下载"),
            _stage_line(stats, "archive", "WebDAV归档"),
            _stage_line(stats, "delivery", "TG回传"),
            f"明确跳过：{skipped}",
        ]
    )
    if job.get("last_error"):
        lines.append(f"任务错误：{_plain(job.get('last_error'), 220)}")
    return "\n".join(lines)


def parse_task_list_args(tokens):
    page = 1
    platform = None
    status = None
    for raw in tokens:
        token = str(raw).strip()
        lowered = token.casefold()
        if re.fullmatch(r"[1-9][0-9]*", token):
            if page != 1:
                raise ValueError("页码只能指定一次")
            page = min(int(token), 10000)
        elif lowered in {"all", "全部"}:
            continue
        elif lowered in PLATFORM_ALIASES:
            value = PLATFORM_ALIASES[lowered]
            if platform and platform != value:
                raise ValueError("平台筛选只能指定一次")
            platform = value
        elif lowered in STATUS_ALIASES:
            value = STATUS_ALIASES[lowered]
            if status and status != value:
                raise ValueError("状态筛选只能指定一次")
            status = value
        else:
            raise ValueError("用法：/tasks [页码] [xhs|douyin|bilibili] [queued|running|completed|incomplete|failed]")
    return page, platform, status


def _job_number(value):
    match = re.fullmatch(r"#?([1-9][0-9]*)", str(value or "").strip())
    if not match:
        raise ValueError("任务编号必须是正整数，例如 /task 10")
    return int(match.group(1))


def _retryable(job):
    return job.get("platform") == "bilibili" and str(job.get("status")) in RETRYABLE_STATUSES


def _prune_menus():
    now = time.monotonic()
    for token, menu in list(_menus.items()):
        if menu.get("expires", 0) < now:
            _menus.pop(token, None)


def _remember(message, *, platform=None, status=None, page=1):
    _prune_menus()
    token = secrets.token_hex(6)
    _menus[token] = {
        "chat_id": message.chat.id,
        "message_id": message.id,
        "platform": platform,
        "status": status,
        "page": int(page),
        "expires": time.monotonic() + MENU_TTL,
    }
    return token


def _callback_data(token, action, value):
    return f"{CALLBACK_PREFIX}{token}:{action}:{value}"


def _list_markup(token, jobs, page, total):
    rows = []
    for job in jobs:
        jid = job.get("id")
        buttons = [InlineKeyboardButton(f"查看 #{jid}", callback_data=_callback_data(token, "detail", jid))]
        if _retryable(job):
            buttons.append(InlineKeyboardButton("重试", callback_data=_callback_data(token, "retry", jid)))
        rows.append(buttons)
    pages = _page_count(total)
    navigation = []
    if page > 1:
        navigation.append(InlineKeyboardButton("上一页", callback_data=_callback_data(token, "list", page - 1)))
    if page < pages:
        navigation.append(InlineKeyboardButton("下一页", callback_data=_callback_data(token, "list", page + 1)))
    if navigation:
        rows.append(navigation)
    rows.append([InlineKeyboardButton("刷新", callback_data=_callback_data(token, "list", page))])
    return InlineKeyboardMarkup(rows)


def _detail_markup(token, job, page):
    rows = []
    if _retryable(job):
        rows.append([InlineKeyboardButton("🔁 重试此任务", callback_data=_callback_data(token, "retry", job.get("id")))])
    rows.append(
        [
            InlineKeyboardButton("返回列表", callback_data=_callback_data(token, "list", page)),
            InlineKeyboardButton("刷新", callback_data=_callback_data(token, "detail", job.get("id"))),
        ]
    )
    return InlineKeyboardMarkup(rows)


def _command_allowed(message):
    return message_principal(message) is not None


def _callback_allowed(query):
    message = query.message
    user = query.from_user
    owner = owner_id()
    if owner is None or not message or not message.chat or not user:
        return False
    kind = getattr(getattr(message.chat, "type", None), "value", getattr(message.chat, "type", None))
    return kind == "private" and message.chat.id == owner and user.id == owner


async def _reply(message, text, markup=None):
    return await message.reply_text(
        text,
        parse_mode=TgParseMode.DISABLED,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
        reply_markup=markup,
    )


async def _edit(message, text, markup=None):
    return await message.edit_text(
        text,
        parse_mode=TgParseMode.DISABLED,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
        reply_markup=markup,
    )


async def _show_list(message, manager, *, page, platform=None, status=None, token=None):
    jobs, total = manager.store.list_jobs(
        platform=platform,
        status=status,
        chat_id=owner_id(),
        limit=PAGE_SIZE,
        offset=(max(1, page) - 1) * PAGE_SIZE,
    )
    page = max(1, min(page, _page_count(total)))
    if page != 1 and not jobs:
        jobs, total = manager.store.list_jobs(
            platform=platform,
            status=status,
            chat_id=owner_id(),
            limit=PAGE_SIZE,
            offset=(page - 1) * PAGE_SIZE,
        )
    text = render_task_list(manager.store, jobs, total, page, platform=platform, status=status)
    if token is None:
        sent = await _reply(message, text)
        token = _remember(sent, platform=platform, status=status, page=page)
    else:
        menu = _menus[token]
        menu.update(platform=platform, status=status, page=page, expires=time.monotonic() + MENU_TTL)
        await _edit(message, text, _list_markup(token, jobs, page, total))
        return
    await sent.edit_reply_markup(_list_markup(token, jobs, page, total))


async def _show_detail(message, manager, job_id, *, token, page):
    job = manager.store.job(job_id)
    if not job or str(job.get("chat_id")) != str(owner_id()):
        await _edit(message, "未找到这个主人私聊任务。")
        return
    _menus[token].update(view="detail", job_id=int(job_id), page=page, expires=time.monotonic() + MENU_TTL)
    await _edit(message, render_task_detail(manager.store, job), _detail_markup(token, job, page))


async def _retry_and_reply(message, manager, job_id, *, requested_message_id):
    try:
        retry = await manager.retry_from_management(
            job_id,
            caller_id=message.from_user.id,
            chat_id=message.chat.id,
            requested_message_id=requested_message_id,
        )
    except RetryRejected as error:
        await _reply(message, _plain(str(error), 300))
        return None
    except Exception as error:
        logger.warning(f"任务重试处理失败: error={type(error).__name__}")
        await _reply(message, "重试任务创建失败，未执行下载；请稍后再试。")
        return None
    try:
        note = await _reply(
            message,
            f"已创建重试任务 #{retry['id']}（原任务 #{job_id}）。\n"
            "已成功归档和回传的作品保留，只处理未完成阶段。\n"
            "当前状态：排队中，按现有队列顺序执行。",
        )
        manager.store.set_job(retry["id"], progress_message_id=note.id)
    except Exception as error:
        logger.warning(f"重试确认消息发送失败: error={type(error).__name__}")
    return retry


@Client.on_message(filters.command(["tasks", "task", "retry"]), group=-40)
async def profile_task_command(cli: Client, message: Message) -> None:
    # Pyrogram's stop_propagation() raises immediately; send the command reply
    # first, then stop lower groups so URL-bearing management commands cannot
    # fall through to the ordinary parser/bulk handler.
    if not _command_allowed(message):
        await _reply(message, "任务管理仅限主人私聊使用。")
        message.stop_propagation()
        return
    try:
        command = str(message.command[0]).casefold().split("@", 1)[0] if message.command else ""
        manager = start_profile_manager(cli)
        if command == "tasks":
            page, platform, status = parse_task_list_args(message.command[1:] if message.command else [])
            await _show_list(message, manager, page=page, platform=platform, status=status)
        elif not message.command or len(message.command) != 2:
            await _reply(message, "用法：/task ID 查看详情；/retry ID 显式重试失败或部分完成任务。")
        else:
            job_id = _job_number(message.command[1])
            if command == "task":
                job = manager.store.job(job_id)
                if not job or str(job.get("chat_id")) != str(owner_id()):
                    await _reply(message, "未找到这个主人私聊任务。")
                else:
                    token = _remember(message, page=1)
                    sent = await _reply(message, render_task_detail(manager.store, job))
                    _menus[token]["message_id"] = sent.id
                    _menus[token]["view"] = "detail"
                    _menus[token]["job_id"] = job_id
                    await sent.edit_reply_markup(_detail_markup(token, job, 1))
            else:
                await _retry_and_reply(message, manager, job_id, requested_message_id=message.id)
    except ValueError as error:
        await _reply(message, _plain(str(error), 300))
    except Exception as error:
        logger.warning(f"任务管理命令失败: command={command}, error={type(error).__name__}")
        await _reply(message, "任务管理暂时不可用，请稍后再试。")
    message.stop_propagation()


@Client.on_callback_query(filters.regex(r"^tm:"), group=-40)
async def profile_task_callback(cli: Client, query: CallbackQuery) -> None:
    if not _callback_allowed(query):
        await query.answer("仅限主人私聊使用", show_alert=True)
        return
    if not query.data or not query.message:
        await query.answer("菜单数据无效", show_alert=True)
        return
    parts = query.data.split(":")
    if len(parts) != 4 or parts[0] != "tm":
        await query.answer("菜单数据无效", show_alert=True)
        return
    _, token, action, raw_value = parts
    menu = _menus.get(token)
    if not menu or menu.get("chat_id") != query.message.chat.id or menu.get("message_id") != query.message.id:
        await query.answer("菜单已过期，请重新发送 /tasks", show_alert=True)
        return
    manager = start_profile_manager(cli)
    try:
        if action == "list":
            page = _job_number(raw_value)
            jobs, total = manager.store.list_jobs(
                platform=menu.get("platform"),
                status=menu.get("status"),
                chat_id=owner_id(),
                limit=PAGE_SIZE,
                offset=(page - 1) * PAGE_SIZE,
            )
            page = max(1, min(page, _page_count(total)))
            menu.update(page=page, view="list", expires=time.monotonic() + MENU_TTL)
            await _edit(
                query.message,
                render_task_list(manager.store, jobs, total, page, platform=menu.get("platform"), status=menu.get("status")),
                _list_markup(token, jobs, page, total),
            )
            await query.answer()
            return
        if action == "detail":
            job_id = _job_number(raw_value)
            await _show_detail(query.message, manager, job_id, token=token, page=int(menu.get("page", 1)))
            await query.answer()
            return
        if action == "retry":
            job_id = _job_number(raw_value)
            retry = await manager.retry_from_management(
                job_id,
                caller_id=query.from_user.id,
                chat_id=query.message.chat.id,
                requested_message_id=query.message.id,
            )
            menu.update(view="detail", job_id=int(retry["id"]), expires=time.monotonic() + MENU_TTL)
            await _edit(query.message, render_task_detail(manager.store, retry), _detail_markup(token, retry, int(menu.get("page", 1))))
            await query.answer(f"已创建重试任务 #{retry['id']}，当前排队中")
            return
        await query.answer("未知操作", show_alert=True)
    except RetryRejected as error:
        await query.answer(_plain(str(error), 180), show_alert=True)
    except (TypeError, ValueError):
        await query.answer("任务编号或菜单数据无效", show_alert=True)
    except Exception as error:
        logger.warning(f"任务管理按钮失败: action={action}, error={type(error).__name__}")
        await query.answer("操作失败，请稍后重试", show_alert=True)
