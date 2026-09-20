from pathlib import Path
import subprocess
import sys


PATCH_SCRIPT = Path("/app/docker/patch_parsehub_bilibili.py")


def test_patch_sets_bilibili_unlimited_quality_first_av1_sort(tmp_path: Path) -> None:
    target = tmp_path / "bilibili.py"
    target.write_text(
        '''class BiliYtVideoParseResult:\n'
        '    @property\n'
        '    def cli_args(self) -> list[str]:\n'
        '        return [\n'
        '            *super().cli_args,\n'
        '            "-S",\n'
        '            "+codec:h264,filesize~500M",\n'
        '        ]\n'''.replace("'\n        '", ""),
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(PATCH_SCRIPT), str(target)],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    patched = target.read_text(encoding="utf-8")
    assert '"-f",\n            "bv*+ba/b",' in patched
    assert '"-S",\n            "res,fps,hdr,vcodec:av01",' in patched
    assert "filesize" not in patched
    assert "+codec:h264,filesize~500M" not in patched
