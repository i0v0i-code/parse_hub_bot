"""Persist media descriptors and reuse the bot's existing processing/sending path."""
from pathlib import Path
from types import SimpleNamespace
from dataclasses import dataclass,field
from parsehub import DownloadResult
from parsehub.types import VideoFile,ImageFile,AniFile,LivePhotoFile,VideoRef,ImageRef,AniRef
from services.webdav import _archive_files
from utils.helpers import to_list


def describe_download(download):
    root=Path(download.output_dir)
    media_map={str(Path(m.path).resolve()):m for m in to_list(download.media)}
    rows=[]
    for path in _archive_files(root):
        row={'name':path.name,'relative':str(path.relative_to(root)),
             'local_path':str(path),'size':path.stat().st_size}
        media=media_map.get(str(path.resolve()))
        if media:
            row.update(media_type=type(media).__name__,width=media.width,height=media.height,
                       duration=getattr(media,'duration',0))
            if isinstance(media,LivePhotoFile) and media.video_path:
                row['partner_relative']=str(Path(media.video_path).relative_to(root))
        rows.append(row)
    return rows


def attach_descriptors(uploaded,described):
    by_path={row['local_path']:row for row in described}
    return [{**by_path.get(row['local_path'],{}),**row} for row in uploaded]


def from_records(rows,paths,root):
    by_relative={row.get('relative',row['name']):Path(path) for row,path in zip(rows,paths,strict=True)}
    media=[]
    for row,path in zip(rows,paths,strict=True):
        kind=row.get('media_type')
        if not kind:continue
        args={'path':Path(path),'width':row.get('width',0),'height':row.get('height',0)}
        if kind=='VideoFile':media.append(VideoFile(**args,duration=row.get('duration',0)))
        elif kind=='ImageFile':media.append(ImageFile(**args))
        elif kind=='AniFile':media.append(AniFile(**args,duration=row.get('duration',0)))
        elif kind=='LivePhotoFile':
            partner=by_relative.get(row.get('partner_relative'))
            if partner is None:raise ValueError('实况图片缺少视频文件')
            media.append(LivePhotoFile(**args,video_path=partner,duration=row.get('duration',3)))
        else:raise ValueError('未知持久化媒体类型')
    if not media:raise ValueError('没有可发送的媒体文件')
    return DownloadResult(media,root)


async def send_download(sender,downloaded,item,caption,_t,*,progress=None,checkpoint=None):
    from plugins.parse.sender import MessageSender,send_media
    from services.media import process_media_files
    from pyrogram.types import Message

    @dataclass(frozen=True,slots=True)
    class RecordingSender(MessageSender):
        sent:list=field(default_factory=list,compare=False)
        async def _send_and_schedule_delete(self,send_coro_fn):
            result=await MessageSender._send_and_schedule_delete(self,send_coro_fn)
            for msg in to_list(result):
                if isinstance(msg,Message):self.sent.append(msg)
            return result

    recorder=RecordingSender(sender.cli,sender.msg,sender.config)
    processed=await process_media_files(downloaded)
    refs=[]
    canonical=item['canonical_url']
    for media in to_list(downloaded.media):
        cls=VideoRef if isinstance(media,VideoFile) else AniRef if isinstance(media,AniFile) else ImageRef
        refs.append(cls(url=canonical))
    from plugins.parse.sender import build_input_media,send_single,send_multi
    photos,animations=build_input_media(refs,processed,video_cover=sender.config.video_cover)
    # Stable transport batches are checkpointed separately, so a rejected later
    # album does not resend successful earlier albums on automatic recovery.
    batches=[([], [ani]) for ani in animations]
    batches.extend((photos[i:i+10],[]) for i in range(0,len(photos),10))
    progress=progress or {}
    done=progress.get('completed_batches',0)
    ids=list(progress.get('message_ids') or [])
    if done>len(batches):raise ValueError('持久化回传批次数与媒体不一致')
    for index,(photos_batch,ani_batch) in enumerate(batches):
        if index<done:continue
        before=len(recorder.sent)
        text=caption if index==len(batches)-1 else ''
        if len(photos_batch)+len(ani_batch)==1:
            await send_single(recorder,photos_batch,ani_batch,text)
        else:
            await send_multi(recorder,photos_batch,ani_batch,text,refs,_t=_t)
        new=recorder.sent[before:]
        if not new:raise RuntimeError('Telegram 未返回任何已发送消息')
        ids.extend(msg.id for msg in new)
        if checkpoint:await checkpoint(index+1,ids)
    if not ids:raise RuntimeError('Telegram 没有可回传的媒体')
    return [SimpleNamespace(id=mid) for mid in ids]
