"""Measured Bilibili CDN routing at the download boundary."""
from copy import deepcopy
from urllib.parse import urlsplit, urlunsplit
from parsehub.parsers.parser.bilibili import BiliYtVideoParseResult
from parsehub.types import DownloadError

PRIMARY = 'upos-tf-all-hw.bilivideo.com'
FALLBACK = 'upos-sz-mirrorhwb.bilivideo.com'

def rewrite(info, host):
    result = deepcopy(info)
    def walk(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if key == 'url' and isinstance(item, str):
                    p = urlsplit(item)
                    domain = p.hostname or ''
                    if p.scheme in ('http', 'https') and p.path.startswith('/upgcxcode/') and (domain.endswith('.bilivideo.com') or domain == 'upos-hz-mirrorakam.akamaized.net'):
                        value[key] = urlunsplit(p._replace(netloc=host))
                elif isinstance(item, (dict, list)):
                    walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)
    walk(result)
    return result

_original = BiliYtVideoParseResult._run_download

async def _cdn_download(self, *args, **kwargs):
    original = self.dl.info_json
    try:
        self.dl.info_json = rewrite(original, PRIMARY)
        try:
            return await _original(self, *args, **kwargs)
        except DownloadError:
            self.dl.info_json = rewrite(original, FALLBACK)
            return await _original(self, *args, **kwargs)
    finally:
        self.dl.info_json = original

BiliYtVideoParseResult._run_download = _cdn_download
