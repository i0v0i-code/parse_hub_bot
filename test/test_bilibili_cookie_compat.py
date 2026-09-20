import importlib.util
from pathlib import Path

from parsehub.parsers.parser.bilibili import BiliYtParse
from parsehub.utils.helpers import SecretCookie


def _load_cookie_compat() -> None:
    module_path = Path("/app/services/youtube_cookie_compat.py")
    spec = importlib.util.spec_from_file_location("youtube_cookie_compat_under_test", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


def test_bilibili_ytdlp_reads_netscape_cookie_file(tmp_path: Path) -> None:
    _load_cookie_compat()
    cookie_text = (
        "# Netscape HTTP Cookie File\n"
        ".bilibili.com\tTRUE\t/\tTRUE\t0\tSESSDATA\ttest-value\n"
    )
    cookie_file = tmp_path / "bilibili.cookies.txt"
    cookie_file.write_text(cookie_text, encoding="utf-8")

    parser = BiliYtParse(cookie=SecretCookie(str(cookie_file)))

    assert parser.get_cookie_text() == cookie_text
