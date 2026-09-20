import pytest

@pytest.fixture(autouse=True)
def existing_archive_tests_owner_scope(request,monkeypatch):
    # Existing transport fixtures now explicitly model an authorized request.
    if request.node.module.__name__ == 'test_webdav_archive':
        from services.owner_policy import archive_scope,ArchivePrincipal
        monkeypatch.setenv('WEBDAV_OWNER_TGID','906346853')
        with archive_scope(ArchivePrincipal(906346853,906346853,'private')):
            yield
    else:
        yield


@pytest.fixture(autouse=True)
def restore_global_parser_hooks():
    from parsehub.parsers.parser.bilibili import BiliYtParse
    from parsehub.parsers.parser.youtube import YtbParse
    bili, youtube = BiliYtParse.get_cookie_text, YtbParse.get_cookie_text
    yield
    BiliYtParse.get_cookie_text, YtbParse.get_cookie_text = bili, youtube
