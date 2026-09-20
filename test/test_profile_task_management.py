from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from plugins.profile_tasks import (
    _callback_allowed,
    _command_allowed,
    parse_task_list_args,
    render_task_detail,
    render_task_list,
)
from services.profile_store import ProfileStore, RetryRejected
from services.profile_jobs import ProfileManager, _send_terminal_messages

OWNER = 906346853


def _private_message(user_id=OWNER, chat_id=OWNER):
    return SimpleNamespace(
        chat=SimpleNamespace(id=chat_id, type=SimpleNamespace(value="private")),
        from_user=SimpleNamespace(id=user_id),
    )


def _store(tmp_path):
    store = ProfileStore(tmp_path / "state.sqlite3")
    store.upsert_profile("xhs", "internal-user-key", "示例昵称", "public_xhs_id")
    job, created = store.enqueue(
        "xhs",
        "internal-user-key",
        "https://www.xiaohongshu.com/user/profile/public_xhs_id",
        OWNER,
        42,
        requester_id=OWNER,
        request_chat_type="private",
    )
    assert created
    store.merge_items(
        "xhs",
        "internal-user-key",
        [
            {"post_id": "success-post", "title": "已完成", "url": "https://www.xiaohongshu.com/explore/success-post"},
            {"post_id": "failed-post", "title": "待重试", "url": "https://www.xiaohongshu.com/explore/failed-post"},
        ],
    )
    store.set_item(
        "xhs",
        "internal-user-key",
        "success-post",
        status="complete",
        download_status="succeeded",
        archive_status="succeeded",
        files=[{"name": "success.mp4", "size": 10, "local_path": "/task/success.mp4"}],
        attempts=2,
    )
    store.set_item(
        "xhs",
        "internal-user-key",
        "failed-post",
        status="failed",
        download_status="failed",
        archive_status="pending",
        last_error="download failed",
        attempts=3,
        download_attempts=3,
        files=[{"name": "partial.mp4", "size": 11, "local_path": "/task/partial.mp4"}],
    )
    store.set_delivery("xhs", "internal-user-key", "success-post", OWNER, status="succeeded", message_ids=[100])
    store.set_delivery(
        "xhs",
        "internal-user-key",
        "failed-post",
        OWNER,
        status="failed",
        last_error="send failed",
        message_ids=[101],
        completed_batches=1,
    )
    store.set_job(
        job["id"],
        status="incomplete",
        enumeration_complete=False,
        pages=6,
        enumeration_reason="login_expired",
        new_count=50,
        failed_count=107,
    )
    return store, int(job["id"])


def test_list_filters_and_explicit_stage_counters(tmp_path):
    store, job_id = _store(tmp_path)
    store.enqueue(
        "xhs",
        "other-internal-key",
        "https://www.xiaohongshu.com/user/profile/other",
        123,
        43,
        requester_id=123,
        request_chat_type="private",
    )
    jobs, total = store.list_jobs(status="incomplete", limit=6, offset=0)
    assert total == 1
    owner_jobs, owner_total = store.list_jobs(chat_id=OWNER, limit=6, offset=0)
    assert owner_total == 1
    assert [int(job["id"]) for job in owner_jobs] == [job_id]
    assert [int(job["id"]) for job in jobs] == [job_id]
    stats = store.job_stats(jobs[0])
    assert stats["known_items"] == 2
    assert stats["archive"] == {"succeeded": 1, "pending": 1}
    assert stats["delivery"] == {"succeeded": 1, "failed": 1}

    text = render_task_detail(store, jobs[0])
    assert "WebDAV归档：成功 1｜失败 0｜待处理 1" in text
    assert "TG回传：成功 1｜失败 1｜待处理 0" in text
    assert "列表读取：未完成" in text
    assert "剩余作品数未知" in text
    assert "internal-user-key" not in text
    assert "xiaohongshu.com" not in text

    list_text = render_task_list(store, jobs, total, 1)
    assert "WebDAV归档 成功 1/失败 0" in list_text
    assert "TG回传 成功 1/失败 1" in list_text


def test_retry_creates_queued_child_and_preserves_successes(tmp_path):
    store, old_id = _store(tmp_path)
    success_before = deepcopy(store.item("xhs", "internal-user-key", "success-post"))
    failed_before = store.item("xhs", "internal-user-key", "failed-post")
    delivery_before = store.delivery("xhs", "internal-user-key", "failed-post", OWNER)

    retry = store.retry_job(
        old_id,
        requester_id=OWNER,
        request_chat_type="private",
        requested_message_id=900,
    )
    new_id = int(retry["id"])
    assert new_id > old_id
    assert retry["status"] == "queued"
    assert retry["retry_of"] == old_id
    assert retry["new_count"] == retry["failed_count"] == 0
    assert store.job(old_id)["status"] == "incomplete"
    assert store.job(old_id)["failed_count"] == 107

    success_after = store.item("xhs", "internal-user-key", "success-post")
    assert success_after["status"] == success_before["status"] == "complete"
    assert success_after["download_status"] == success_before["download_status"] == "succeeded"
    assert success_after["archive_status"] == success_before["archive_status"] == "succeeded"
    assert success_after["files"] == success_before["files"]

    failed_after = store.item("xhs", "internal-user-key", "failed-post")
    assert failed_after["status"] == "pending"
    assert failed_after["download_status"] == "pending"
    assert failed_after["archive_status"] == "pending"
    assert failed_after["attempts"] == failed_before["attempts"]
    assert failed_after["files"] == failed_before["files"]
    assert failed_after["last_error"] == ""

    delivery_after = store.delivery("xhs", "internal-user-key", "failed-post", OWNER)
    assert delivery_after["status"] == "pending"
    assert delivery_after["message_ids"] == delivery_before["message_ids"]
    assert delivery_after["completed_batches"] == delivery_before["completed_batches"]

    with pytest.raises(RetryRejected) as error:
        store.retry_job(old_id, requester_id=OWNER, request_chat_type="private")
    assert error.value.code == "active"
    assert store.list_jobs()[1] == 2

    with pytest.raises(RetryRejected) as error:
        store.retry_job(new_id, requester_id=OWNER, request_chat_type="private")
    assert error.value.code == "active"


def test_retry_rejects_terminal_success_and_non_owner(tmp_path):
    store, old_id = _store(tmp_path)
    with pytest.raises(RetryRejected) as error:
        store.retry_job(old_id, requester_id=123, request_chat_type="group")
    assert error.value.code == "owner_only"
    store.set_job(old_id, status="completed", enumeration_complete=True)
    with pytest.raises(RetryRejected) as error:
        store.retry_job(old_id, requester_id=OWNER, request_chat_type="private")
    assert error.value.code == "not_retryable"


def test_management_parser_and_private_callback_boundary(monkeypatch):
    monkeypatch.setenv("WEBDAV_OWNER_TGID", str(OWNER))
    assert parse_task_list_args(["2", "xhs", "failed"]) == (2, "xhs", "failed")
    assert parse_task_list_args(["小红书", "部分完成"]) == (1, "xhs", "incomplete")
    assert _command_allowed(_private_message())
    assert not _command_allowed(_private_message(user_id=123))
    assert not _command_allowed(_private_message(chat_id=-100123456))

    query = SimpleNamespace(message=_private_message(user_id=999), from_user=SimpleNamespace(id=OWNER))
    assert _callback_allowed(query)
    query.message.chat.type = SimpleNamespace(value="group")
    assert not _callback_allowed(query)


def test_legacy_retry_requires_original_owner_private_message(tmp_path, monkeypatch):
    monkeypatch.setenv("WEBDAV_OWNER_TGID", str(OWNER))
    store, old_id = _store(tmp_path / "valid")
    row = store.db.execute("SELECT data FROM jobs WHERE id=?", (old_id,)).fetchone()
    data = json.loads(row[0])
    data.pop("requester_id", None)
    data.pop("request_chat_type", None)
    store.db.execute("UPDATE jobs SET data=? WHERE id=?", (json.dumps(data), old_id))
    store.db.commit()

    original = _private_message()
    original.id = 42

    class FakeClient:
        async def get_messages(self, chat_id, message_id):
            assert chat_id == OWNER
            assert message_id == 42
            return original

    manager = ProfileManager.__new__(ProfileManager)
    manager.store = store
    manager.cli = FakeClient()
    manager.wake = asyncio.Event()
    retry = asyncio.run(
        manager.retry_from_management(
            old_id,
            caller_id=OWNER,
            chat_id=OWNER,
            requested_message_id=900,
        )
    )
    assert int(retry["retry_of"]) == old_id
    assert retry["requester_id"] == OWNER
    assert retry["request_chat_type"] == "private"

    invalid_store, invalid_id = _store(tmp_path / "invalid")
    invalid_row = invalid_store.db.execute("SELECT data FROM jobs WHERE id=?", (invalid_id,)).fetchone()
    invalid_data = json.loads(invalid_row[0])
    invalid_data.pop("requester_id", None)
    invalid_data.pop("request_chat_type", None)
    invalid_store.db.execute("UPDATE jobs SET data=? WHERE id=?", (json.dumps(invalid_data), invalid_id))
    invalid_store.db.commit()
    invalid = _private_message(user_id=123)
    invalid.id = 42

    class InvalidClient:
        async def get_messages(self, chat_id, message_id):
            return invalid

    invalid_manager = ProfileManager.__new__(ProfileManager)
    invalid_manager.store = invalid_store
    invalid_manager.cli = InvalidClient()
    invalid_manager.wake = asyncio.Event()
    with pytest.raises(RetryRejected) as error:
        asyncio.run(
            invalid_manager.retry_from_management(
                invalid_id,
                caller_id=OWNER,
                chat_id=OWNER,
                requested_message_id=901,
            )
        )
    assert error.value.code == "legacy_unverified"
    assert invalid_store.list_jobs()[1] == 1


def test_profile_manager_stop_requeues_and_keeps_workdir(tmp_path):
    store, job_id = _store(tmp_path / "shutdown")
    store.set_job(job_id, status="running")
    workdir = tmp_path / "shutdown" / "partial-work"
    workdir.mkdir()
    marker = workdir / "partial.mp4"
    marker.write_bytes(b"partial")

    async def exercise():
        manager = ProfileManager.__new__(ProfileManager)
        manager.store = store
        manager.runner = asyncio.create_task(asyncio.sleep(3600))
        manager.active = {job_id: asyncio.create_task(asyncio.sleep(3600))}
        await manager.stop()

    asyncio.run(exercise())
    assert store.job(job_id)["status"] == "queued"
    assert store.job(job_id)["retry_at"] == 0
    assert marker.read_bytes() == b"partial"


def test_queued_retry_does_not_resend_summary_or_download_record(tmp_path):
    class Sender:
        def __init__(self):
            self.calls = []

        async def text_no_preview(self, *args, **kwargs):
            self.calls.append(("text", args, kwargs))

        async def document(self, *args, **kwargs):
            self.calls.append(("document", args, kwargs))

    sender = Sender()
    notified = asyncio.run(
        _send_terminal_messages(
            sender,
            status="queued",
            job_id=24,
            current={"new_count": 0, "failed_count": 8},
            skipped=671,
            result={"complete": True},
            root=tmp_path,
            platform="bilibili",
            user_id="85516078",
        )
    )
    assert notified is False
    assert sender.calls == []


def test_non_retryable_partial_result_sends_summary_and_record(tmp_path):
    class Sender:
        def __init__(self):
            self.calls = []

        async def text_no_preview(self, *args, **kwargs):
            self.calls.append(("text", args, kwargs))

        async def document(self, *args, **kwargs):
            self.calls.append(("document", args, kwargs))

    sender = Sender()
    notified = asyncio.run(
        _send_terminal_messages(
            sender,
            status="incomplete",
            job_id=5,
            current={"new_count": 1, "failed_count": 0},
            skipped=0,
            result={"complete": False, "reason": "分页未确认"},
            root=tmp_path,
            platform="xhs",
            user_id="public_xhs_id",
        )
    )
    assert notified is True
    assert [call[0] for call in sender.calls] == ["text", "document"]
    assert "部分完成" in sender.calls[0][1][0]
    assert sender.calls[1][1][0].endswith("xhs/public_xhs_id/下载记录.txt")


def test_management_command_replies_before_stopping_propagation(monkeypatch):
    import pyrogram
    import plugins.profile_tasks as module

    monkeypatch.setenv("WEBDAV_OWNER_TGID", str(OWNER))
    replies = []

    class Sent:
        def __init__(self, chat):
            self.chat = chat
            self.id = 9001

        async def edit_reply_markup(self, markup):
            return None

    class Message:
        def __init__(self):
            self.chat = SimpleNamespace(id=OWNER, type=SimpleNamespace(value="private"))
            self.from_user = SimpleNamespace(id=OWNER)
            self.command = ["tasks"]
            self.id = 9000

        async def reply_text(self, text, **kwargs):
            replies.append(text)
            return Sent(self.chat)

        def stop_propagation(self):
            raise pyrogram.StopPropagation

    class Store:
        def list_jobs(self, **kwargs):
            return [], 0

    manager = SimpleNamespace(store=Store())
    monkeypatch.setattr(module, "start_profile_manager", lambda cli: manager)

    with pytest.raises(pyrogram.StopPropagation):
        asyncio.run(module.profile_task_command(object(), Message()))

    assert replies and replies[0].startswith("用户下载任务｜第 1/1 页")
