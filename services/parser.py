from typing import Self

from parsehub import ParseHub, Platform
from parsehub.types import (
    AnyParseResult,
)

import services.douyin_access  # noqa: F401
import services.xhs_compat  # noqa: F401
import services.youtube_cookie_compat  # noqa: F401  # YouTube cookie 文件路径兼容补丁
import services.bilibili_quality  # noqa: F401  # B站视频始终使用登录账号最高可用画质
from core import pl_cfg
from log import logger

logger = logger.bind(name="ParseService")


def refresh_login_cooldown(platform, credentials, rate):
    """A verified login published after a refusal gets one fresh attempt."""
    state = rate.read_state() if platform == 'xhs' else rate.read_state(platform)
    if state and credentials and float(credentials.get('saved_at', 0)) > float(state.get('updated', 0)):
        rate.succeeded() if platform == 'xhs' else rate.succeeded(platform)


class ParseService:
    _instance: Self | None = None

    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        self.parser = ParseHub()

    def get_platform(self, url: str) -> Platform:
        p = self.parser.get_platform(url)
        if not p:
            raise ValueError("不支持的平台")
        return p

    async def parse(self, url: str) -> AnyParseResult:
        logger.debug(f"开始解析 {url}")
        p = self.get_platform(url)

        from services.login_credentials import snapshot, cookie_text
        credentials = snapshot(p.id)
        cookie = pl_cfg.roll_cookie(p.id)
        cookie_value = cookie_text(credentials) if credentials is not None else (cookie.get_secret_value() if cookie else None)
        if p.id == 'xhs':
            from services import xhs_rate_limit as rate
            refresh_login_cooldown(p.id, credentials, rate)
            rate.check_available()
            # A single worker is not a rate limit: serialize and pace all XHS parses,
            # including ordinary single-link requests and automatic profile retries.
            async with rate._lock:
                for attempt in range(1, 4):
                    await rate.wait_turn(wait_cooldown=False)
                    # A manual login may publish fresh credentials during a long
                    # cooldown. Freeze the current generation when the attempt starts.
                    from services.login_credentials import load
                    latest = load('xhs')
                    attempt_cookie = cookie_text(latest) if latest is not None else cookie_value
                    try:
                        proxy = pl_cfg.roll_parser_proxy(p.id)
                        result = await self.parser.parse(url, cookie=attempt_cookie, proxy=proxy)
                    except Exception as error:
                        if rate.is_access_error(error):
                            state = rate.block('captcha_or_login_required')
                            logger.warning(f"小红书访问验证冷却，停止立即重试，until={state['until']:.0f}")
                            raise
                        if attempt == 3:
                            raise
                    else:
                        rate.succeeded()
                        return result
        if p.id in ('bilibili','douyin'):
            from services import platform_rate_limit as rate
            refresh_login_cooldown(p.id, credentials, rate)
            rate.check_available(p.id)
            async with rate.lock(p.id):
                for attempt in range(1,4):
                    await rate.wait_turn(p.id,wait_cooldown=False)
                    from services.login_credentials import load
                    latest=load(p.id)
                    attempt_cookie=cookie_text(latest) if latest is not None else cookie_value
                    try:
                        result=await self.parser.parse(url,cookie=attempt_cookie,proxy=pl_cfg.roll_parser_proxy(p.id))
                    except Exception as error:
                        from services.bilibili_quality import BilibiliUnavailable
                        if isinstance(error,BilibiliUnavailable):raise
                        if rate.access_error(error):
                            rate.block(p.id,'access_or_auth_failure')
                            raise
                        if attempt==3:raise
                        await __import__('asyncio').sleep(10*attempt)
                    else:
                        rate.succeeded(p.id)
                        return result
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                proxy = pl_cfg.roll_parser_proxy(p.id)
                logger.debug(f"解析尝试 attempt={attempt}/{max_retries}")
                pr = await self.parser.parse(url, cookie=cookie_value, proxy=proxy)
                logger.debug(f"解析完成: {pr}")
                return pr
            except Exception as e:
                logger.warning(f"解析失败, attempt={attempt}/{max_retries}, err={e}")
                if attempt >= max_retries:
                    raise Exception(e) from e
        raise

    async def get_raw_url(self, url: str, clean_all: bool = True) -> str:
        p = self.get_platform(url)

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                proxy = pl_cfg.roll_parser_proxy(p.id)
                logger.debug(f"使用配置: proxy={proxy}, attempt={attempt}/{max_retries}")
                raw_url = await self.parser.get_raw_url(url, proxy=proxy, clean_all=clean_all)
                logger.debug(f"原始 URL: {raw_url}")
                return str(raw_url)
            except Exception as e:
                logger.warning(f"获取原始 URL 失败, attempt={attempt}/{max_retries}, err={e}")
                if attempt >= max_retries:
                    raise Exception(e) from e
        raise
