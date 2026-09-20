"""Use the authenticated web endpoint; stop on refusal instead of probing mobile hosts."""
from parsehub.parsers.parser.douyin import DouyinParser, DouyinApiResult
from parsehub.provider_api.douyin import DouyinWebCrawler
from parsehub.types import ParseError

async def _authenticated_result(self, raw_url):
    cookie=self.cookie.get_value() or {}
    if not cookie:
        raise ParseError('抖音需要登录 Cookie，已停止匿名接口重试')
    crawler=DouyinWebCrawler(proxy=self.proxy,cookie=cookie)
    response=await crawler.parse(raw_url)
    return DouyinApiResult.parse(response)

DouyinParser._fetch_api_result=_authenticated_result
