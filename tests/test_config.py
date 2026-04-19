from shared.config import settings


def test_chat_export_defaults():
    assert settings.chat_export_tenant_concurrent_min == 200
    assert settings.chat_export_tenant_concurrent_per_user == 0.5
    assert settings.chat_export_blob_account_shards == 4
    assert settings.chat_export_progress_transport == "sse"
    assert settings.chat_export_size_soft_cap_bytes == 21_474_836_480
    assert settings.chat_export_size_hard_cap_bytes == 1_099_511_627_776
    assert settings.chat_export_blob_ttl_hours == 168
    assert settings.chat_export_sas_ttl_hours == 168
    assert settings.chat_export_hot_tier_hours == 24
    assert settings.chat_export_dynamic_prefetch is True
    assert settings.chat_hosted_content_concurrency == 8
    assert settings.chat_hosted_content_max_bytes == 25_000_000
    # Accounts list defaults to 4 placeholder names matching shard count
    assert len(settings.chat_export_blob_accounts) == 4
