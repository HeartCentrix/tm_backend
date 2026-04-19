from workers.chat_export_worker.blob_shard import pick_shard, shard_account, sign_download_url


def test_pick_shard_deterministic():
    assert pick_shard("j1", n=4) == pick_shard("j1", n=4)
    assert 0 <= pick_shard("j1", n=4) < 4


def test_pick_shard_distribution():
    c = [0]*4
    for i in range(10_000): c[pick_shard(f"j-{i}", n=4)] += 1
    assert all(2125 < x < 2875 for x in c)


def test_shard_account_returns_configured():
    assert shard_account(2, accounts=["a","b","c","d"]) == "c"


def test_sign_download_url_shape(monkeypatch):
    import workers.chat_export_worker.blob_shard as bs
    monkeypatch.setattr(bs, "_sas_sig", lambda **k: "sig=FAKE")
    url = sign_download_url(account="acc", container="exports", blob_path="j/export.zip", expires_in_hours=168)
    assert url.startswith("https://acc.blob.core.windows.net/exports/j/export.zip?")
    assert "sig=FAKE" in url
