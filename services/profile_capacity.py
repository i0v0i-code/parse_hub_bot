"""Disk admission for profile downloads; existing originals can always drain."""
import os
import asyncio
import shutil
import time
from pathlib import Path

GIB=1024**3
MIN_FREE=8*GIB
MAX_PENDING=40*GIB

class CapacityDeferred(RuntimeError):
    pass

class CollectionPending(RuntimeError):
    pass

class ProfileCapacity:
    def __init__(self,root):
        self.root=Path(root); self.cached_at=0; self.cached_bytes=0

    def pending_bytes(self):
        if time.monotonic()-self.cached_at<5:return self.cached_bytes
        total=0
        for platform in ('bilibili','douyin','xhs'):
            for directory,_,files in os.walk(self.root/platform):
                for name in files:
                    try:total+=(Path(directory)/name).stat().st_blocks*512
                    except FileNotFoundError:pass
        self.cached_at=time.monotonic();self.cached_bytes=total
        return total

    def require(self,expected=0,local_drain=False):
        free=shutil.disk_usage(self.root).free
        reserve=3*GIB if local_drain else MIN_FREE
        if free<reserve+expected or (not local_drain and self.pending_bytes()>MAX_PENDING):
            raise CapacityDeferred('等待磁盘空间：暂停新下载/恢复，优先处理本地已下载作品')

    async def guard(self,awaitable):
        task=asyncio.ensure_future(awaitable)
        try:
            while not task.done():
                done,_=await asyncio.wait({task},timeout=1)
                if done:break
                # Keep an emergency reserve for SQLite and unrelated services.
                if shutil.disk_usage(self.root).free<2*GIB:
                    raise CapacityDeferred('等待磁盘空间：已中断当前下载并保留断点')
            return await task
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task,return_exceptions=True)
