"""User-ID archive layout; shares the existing WebDAV transport and checks."""
import asyncio
import posixpath
import re
from pathlib import Path
from urllib.request import Request,urlopen
import base64
from services.webdav import (WebDavArchiveConfig, _request, _remote_size,
                             _remote_url, _archive_files, platform_folder)


def user_folder(platform,user_id):
    if user_id in ('.','..') or platform not in {'douyin','xhs','bilibili'} or not re.fullmatch(r'[A-Za-z0-9_.-]+',user_id):
        raise ValueError('平台或用户 ID 无效')
    label='哔哩哔哩' if platform=='bilibili' else platform_folder(platform)
    return f'用户下载/{label}/{user_id}'


def ensure_folders(config,folder):
    parts=folder.split('/')
    for index in range(1,len(parts)+1):
        relative='/'.join(parts[:index])
        status,_=_request(config,'MKCOL',relative)
        if status not in {201,405}:
            probe,_=_request(config,'PROPFIND',relative+'/')
            if probe!=207:raise RuntimeError(f'WebDAV 创建用户目录失败（HTTP {status}）')


def _upload(config,folder,post_id,output_dir):
    if not re.fullmatch(r'[A-Za-z0-9_-]+',post_id):raise ValueError('作品 ID 无效')
    ensure_folders(config,folder)
    files=_archive_files(Path(output_dir))
    if not files:raise ValueError('下载目录没有可归档文件')
    result=[]
    for path in files:
        # Subdirectories are uncommon but must not collapse same-name files.
        suffix='__'.join(path.relative_to(output_dir).parts)
        name=f'{post_id}_{suffix}'
        if len(name.encode('utf-8'))>240:
            name=f'{post_id}_{suffix[:48]}_{__import__("hashlib").sha256(suffix.encode()).hexdigest()[:12]}{path.suffix}'
        remote=posixpath.join(folder,name)
        size=path.stat().st_size
        with path.open('rb') as stream:
            status,_=_request(config,'PUT',remote,data=stream,content_length=size)
        if status not in {200,201,204}:raise RuntimeError(f'WebDAV 视频上传失败（HTTP {status}）')

        result.append({'path':remote,'name':name,'size':size,'local_path':str(path)})
    return result


async def upload_item(config,platform,user_id,post_id,output_dir,*,is_video=True):
    folder=user_folder(platform,user_id)
    if not is_video:folder+=f'/图文/{post_id}'
    return await asyncio.to_thread(_upload,config,folder,post_id,Path(output_dir))


def _report(config,folder,text):
    ensure_folders(config,folder)
    body=text.encode('utf-8');remote=folder+'/下载记录.txt'

    status,_=_request(config,'PUT',remote,data=body)
    if status not in {200,201,204}:raise RuntimeError(f'WebDAV 记录上传失败（HTTP {status}）')

    return remote


async def upload_report(config,platform,user_id,text):
    return await asyncio.to_thread(_report,config,user_folder(platform,user_id),text)


def _restore(config,files,target):
    target=Path(target);target.mkdir(parents=True,exist_ok=True,mode=0o700)
    paths=[]
    for item in files:
        name=Path(item['name']).name
        if name!=item['name']:raise ValueError('归档文件名无效')
        path=target/name
        if path.is_file() and path.stat().st_size==item['size']:
            paths.append(path);continue
        token=base64.b64encode(f'{config.user}:{config.password}'.encode()).decode()
        req=Request(_remote_url(config.url,item['path']),headers={'Authorization':f'Basic {token}'})
        part=path.with_name(path.name+'.part')
        with urlopen(req,timeout=120) as response,part.open('wb') as stream:
            while data:=response.read(1024*1024):stream.write(data)
        if part.stat().st_size!=item['size']:raise RuntimeError('WebDAV 恢复文件大小校验失败')
        part.replace(path);paths.append(path)
    return paths


async def restore_item(config,files,target):
    return await asyncio.to_thread(_restore,config,files,target)


def _collection_upload(config,user_id,folder,files,paths):
    if '/' in folder or '\\' in folder or not re.match(r'^(season|series)_[0-9]+_',folder):
        raise ValueError('合集目录无效')
    target=user_folder('bilibili',user_id)+'/合集/'+folder
    ensure_folders(config,target)
    result=[]
    for record,path in zip(files,paths,strict=True):
        name=record['name']
        if Path(name).name!=name:raise ValueError('归档文件名无效')
        path=Path(path)
        if path.stat().st_size!=record['size']:raise ValueError('合集源文件大小不一致')
        remote=target+'/'+name
        with path.open('rb') as stream:
            status,_=_request(config,'PUT',remote,data=stream,content_length=record['size'])
        if status not in {200,201,204}:
            raise RuntimeError('合集副本上传失败')
        result.append(dict(path=remote,name=name,size=record['size']))
    return result


async def upload_collection(config,user_id,folder,files,paths):
    # Reuse the already downloaded original, or restore from the main archive.
    # No dependency on unsupported WebDAV COPY/symlink behavior.
    return await asyncio.to_thread(_collection_upload,config,user_id,folder,files,paths)
