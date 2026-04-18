"""Shared configuration for all microservices"""
import os
from typing import List
from urllib.parse import quote, urlparse


class Settings:
    def __init__(self):
        # Railway provides DATABASE_URL directly. Be permissive and also
        # accept a full Postgres URL accidentally pasted into DB_HOST.
        self._database_url_override = self._resolve_database_url_override()
        if self._database_url_override:
            parsed = urlparse(self._database_url_override)
            self.DB_HOST = parsed.hostname or "localhost"
            self.DB_PORT = str(parsed.port or "5432")
            self.DB_NAME = parsed.path.lstrip("/") if parsed.path else "tm_vault_db"
            self.DB_USERNAME = parsed.username or "postgres"
            self.DB_PASSWORD = parsed.password or ""
        else:
            self.DB_HOST = os.getenv("DB_HOST")
            self.DB_PORT = os.getenv("DB_PORT", "5432")
            self.DB_NAME = os.getenv("DB_NAME")
            self.DB_USERNAME = os.getenv("DB_USERNAME")
            self.DB_PASSWORD = os.getenv("DB_PASSWORD")

        self.DB_SCHEMA = os.getenv("DB_SCHEMA", "public")
        self.DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "2"))
        self.DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "2"))
        self.DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
        self.DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))
        self.DB_POOL_USE_LIFO = os.getenv("DB_POOL_USE_LIFO", "true").lower() in ("true", "1", "yes")
        self.JWT_SECRET = os.getenv("JWT_SECRET", "")
        self.JWT_ALGORITHM = "HS256"
        self.JWT_EXPIRATION_HOURS = 8
        self.JWT_REFRESH_EXPIRATION_DAYS = 7

        # Railway provides REDIS_URL; fall back to individual vars
        railway_redis_url = os.getenv("REDIS_URL")
        if railway_redis_url:
            parsed = urlparse(railway_redis_url)
            self.REDIS_HOST = parsed.hostname or "localhost"
            self.REDIS_PORT = parsed.port or 6379
            self.REDIS_DB = int((parsed.path.lstrip("/") or "0"))
        else:
            self.REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
            self.REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
            self.REDIS_DB = int(os.getenv("REDIS_DB", "0"))
        self.REDIS_ENABLED = os.getenv("REDIS_ENABLED", "false").lower() in ("true", "1", "yes")

        # Railway provides RABBITMQ_URL or AMQP_URL; fall back to individual vars
        railway_rabbitmq_url = os.getenv("RABBITMQ_URL") or os.getenv("AMQP_URL")
        if railway_rabbitmq_url:
            parsed = urlparse(railway_rabbitmq_url)
            self.RABBITMQ_HOST = parsed.hostname or "localhost"
            self.RABBITMQ_PORT = parsed.port or 5672
            self.RABBITMQ_USER = parsed.username or "guest"
            self.RABBITMQ_PASSWORD = parsed.password or "guest"
        else:
            self.RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
            self.RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
            self.RABBITMQ_USER = os.getenv("RABBITMQ_USERNAME") or os.getenv("RABBITMQ_USER", "guest")
            self.RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
        self.RABBITMQ_ENABLED = os.getenv("RABBITMQ_ENABLED", "false").lower() in ("true", "1", "yes")
        self.AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "")
        self.AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "")
        self.AZURE_STORAGE_BLOB_ENDPOINT = os.getenv("AZURE_STORAGE_BLOB_ENDPOINT", "https://blob.core.windows.net")

        # Azure ARM (Azure Resource Manager) credentials for VM/SQL/PostgreSQL backup
        # Service Principal for ARM API access
        self.AZURE_ARM_CLIENT_ID = os.getenv("AZURE_ARM_CLIENT_ID", "")
        self.AZURE_ARM_CLIENT_SECRET = os.getenv("AZURE_ARM_CLIENT_SECRET", "")
        self.AZURE_ARM_TENANT_ID = os.getenv("AZURE_ARM_TENANT_ID", "")
        self.AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID", "")

        # Azure Backup resource group (for VM restore point collections)
        self.AZURE_BACKUP_RESOURCE_GROUP = os.getenv("AZURE_BACKUP_RESOURCE_GROUP", "rg-tmvault-backup")
        # Backup storage region (for RPC placement)
        self.AZURE_BACKUP_REGION = os.getenv("AZURE_BACKUP_REGION", "eastus")
        
        # High-Performance Backup Configuration
        # Parallelism: Max concurrent Graph API calls per worker.
        # Raised 50 -> 100 when we moved to a single backup-worker replica; the
        # per-replica asyncio.Semaphore covers what three replicas × 50 used to.
        # Tune higher only if Graph throttling (multi_app_manager) isn't saturating.
        self.BACKUP_CONCURRENCY = int(os.getenv("BACKUP_CONCURRENCY", "100"))
        # Parallelism: Max concurrent Server-Side Copy operations
        self.COPY_CONCURRENCY = int(os.getenv("COPY_CONCURRENCY", "100"))
        # File size threshold (bytes) - above this, use Server-Side Copy
        self.SERVER_SIDE_COPY_THRESHOLD = int(os.getenv("SERVER_SIDE_COPY_THRESHOLD", "10485760"))  # 10MB
        # Workload parallelism: concurrent jobs per workload type
        self.WORKLOAD_CONCURRENCY = int(os.getenv("WORKLOAD_CONCURRENCY", "5"))
        # Storage sharding: number of storage accounts to distribute across
        self.STORAGE_SHARD_COUNT = int(os.getenv("STORAGE_SHARD_COUNT", "1"))
        # Comma-separated list of storage account names (for sharding)
        storage_shards = os.getenv("STORAGE_SHARD_ACCOUNTS", "")
        self.STORAGE_SHARD_ACCOUNTS = [s.strip() for s in storage_shards.split(",") if s.strip()] if storage_shards else []
        # Comma-separated list of storage account keys (matching order)
        storage_shard_keys = os.getenv("STORAGE_SHARD_KEYS", "")
        self.STORAGE_SHARD_KEYS = [k.strip() for k in storage_shard_keys.split(",") if k.strip()] if storage_shard_keys else []
        # Retry configuration
        self.MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

        # Backup streaming performance settings (cloud-to-cloud, matches Afi.ai architecture)
        # Azure Blob supports up to 4GB block size, 50,000 blocks per blob
        # 100MB blocks are optimal for throughput (per Azure perf tuning docs)
        self.AZURE_BLOCK_SIZE_MB = int(os.getenv("AZURE_BLOCK_SIZE_MB", "100"))
        # Parallel block uploads per file (5-8 is optimal)
        self.AZURE_UPLOAD_CONCURRENCY = int(os.getenv("AZURE_UPLOAD_CONCURRENCY", "5"))
        # (duplicate removed — single canonical declaration above sets BACKUP_CONCURRENCY)
        # How many resource groups to process in parallel (workload parallelism)
        self.WORKLOAD_CONCURRENCY = int(os.getenv("WORKLOAD_CONCURRENCY", "5"))
        self.RETRY_DELAY_MS = int(os.getenv("RETRY_DELAY_MS", "2000"))
        self.RETRY_BACKOFF_MULTIPLIER = float(os.getenv("RETRY_BACKOFF_MULTIPLIER", "2.0"))
        # Encryption key for storing secrets (Fernet key, base64-encoded 32-byte key)
        self.ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")
        # Batch size for Graph API $batch endpoint
        self.GRAPH_BATCH_SIZE = int(os.getenv("GRAPH_BATCH_SIZE", "20"))
        # Chunk size for processing resources
        self.RESOURCE_CHUNK_SIZE = int(os.getenv("RESOURCE_CHUNK_SIZE", "50"))
        # Discovery staging / merge batch sizes for large tenant onboarding
        self.DISCOVERY_STAGE_CHUNK_SIZE = int(os.getenv("DISCOVERY_STAGE_CHUNK_SIZE", "500"))
        self.DISCOVERY_PROGRESS_LOG_EVERY = int(os.getenv("DISCOVERY_PROGRESS_LOG_EVERY", "250"))

        # ── Mail export (MBOX / EML) — see docs/superpowers/specs/2026-04-19-mbox-mail-export-design.md ──
        self.EXPORT_PARALLELISM = int(os.getenv("EXPORT_PARALLELISM", "12"))
        self.EXPORT_MBOX_SPLIT_BYTES = int(os.getenv("EXPORT_MBOX_SPLIT_BYTES", str(5 * 1024 * 1024 * 1024)))
        self.EXPORT_BLOCK_SIZE_BYTES = int(os.getenv("EXPORT_BLOCK_SIZE_BYTES", str(4 * 1024 * 1024)))
        self.EXPORT_FOLDER_QUEUE_MAXSIZE = int(os.getenv("EXPORT_FOLDER_QUEUE_MAXSIZE", "20"))
        self.MAX_CONCURRENT_EXPORTS_PER_WORKER = int(os.getenv("MAX_CONCURRENT_EXPORTS_PER_WORKER", "2"))
        self.EXPORT_FETCH_BATCH_SIZE = int(os.getenv("EXPORT_FETCH_BATCH_SIZE", "50"))
        self.EXPORT_MEMORY_SOFT_LIMIT_PCT = int(os.getenv("EXPORT_MEMORY_SOFT_LIMIT_PCT", "80"))
        self.EXPORT_MEMORY_KILL_GRACE_SECONDS = int(os.getenv("EXPORT_MEMORY_KILL_GRACE_SECONDS", "60"))
        self.EXPORT_MAIL_V2_ENABLED = os.getenv("EXPORT_MAIL_V2_ENABLED", "false").lower() in ("true", "1", "yes")

        self.ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
        self.ELASTICSEARCH_ENABLED = False
        origins = os.getenv("CORS_ORIGINS") or os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:4200,http://localhost:3000,http://localhost:5173")
        self.CORS_ORIGINS = [o.strip() for o in origins.split(",")]

        # Frontend URL for OAuth redirects
        self.FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:4200").rstrip("/")

        # Multi-app registration for Microsoft Graph API
        # Parse from env: APP_1_CLIENT_ID, APP_1_CLIENT_SECRET, APP_1_TENANT_ID, etc.
        self.GRAPH_APPS = self._parse_graph_apps()

        # Microsoft Auth URLs (constructed from tenant ID)
        self._tenant_id = self.GRAPH_APPS[0]["tenant_id"] if self.GRAPH_APPS else "common"
        self.MICROSOFT_AUTH_URL = os.getenv("MICROSOFT_AUTH_URL", f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/authorize")
        self.MICROSOFT_TOKEN_URL = os.getenv("MICROSOFT_TOKEN_URL", f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/token")

        # Dedicated Power BI / Fabric app credentials (optional).
        # Falls back to the primary Microsoft app when not provided.
        self.POWER_BI_CLIENT_ID = os.getenv("POWER_BI_CLIENT_ID", "")
        self.POWER_BI_CLIENT_SECRET = os.getenv("POWER_BI_CLIENT_SECRET", "")
        self.POWER_BI_TENANT_ID = os.getenv("POWER_BI_TENANT_ID", "")
        self.POWER_BI_FULL_SNAPSHOT_DAYS = int(os.getenv("POWER_BI_FULL_SNAPSHOT_DAYS", "7"))

        # Datasource OAuth URLs (multi-tenant for connecting other orgs)
        self.DATASOURCE_AUTH_URL = os.getenv("DATASOURCE_AUTH_URL", f"https://login.microsoftonline.com/organizations/oauth2/v2.0/authorize")
        self.DATASOURCE_TOKEN_URL = os.getenv("DATASOURCE_TOKEN_URL", f"https://login.microsoftonline.com/organizations/oauth2/v2.0/token")

        # Microservice URLs (Railway or local)
        self.AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL", "http://auth-service:8001")
        self.TENANT_SERVICE_URL = os.getenv("TENANT_SERVICE_URL", "http://tenant-service:8002")
        self.RESOURCE_SERVICE_URL = os.getenv("RESOURCE_SERVICE_URL", "http://resource-service:8003")
        self.JOB_SERVICE_URL = os.getenv("JOB_SERVICE_URL", "http://job-service:8004")
        self.SNAPSHOT_SERVICE_URL = os.getenv("SNAPSHOT_SERVICE_URL", "http://snapshot-service:8005")
        self.DASHBOARD_SERVICE_URL = os.getenv("DASHBOARD_SERVICE_URL", "http://dashboard-service:8006")
        self.ALERT_SERVICE_URL = os.getenv("ALERT_SERVICE_URL", "http://alert-service:8007")
        self.BACKUP_SCHEDULER_URL = os.getenv("BACKUP_SCHEDULER_URL", "http://backup-scheduler:8008")
        self.GRAPH_PROXY_URL = os.getenv("GRAPH_PROXY_URL", "http://graph-proxy:8009")
        self.DELTA_TOKEN_URL = os.getenv("DELTA_TOKEN_URL", "http://delta-token:8010")
        self.PROGRESS_TRACKER_URL = os.getenv("PROGRESS_TRACKER_URL", "http://progress-tracker:8011")
        self.AUDIT_SERVICE_URL = os.getenv("AUDIT_SERVICE_URL", "http://audit-service:8012")
        self.REPORT_SERVICE_URL = os.getenv("REPORT_SERVICE_URL", "http://report-service:8014")

    def _resolve_database_url_override(self) -> str:
        raw_database_url = os.getenv("DATABASE_URL", "").strip()
        if not raw_database_url:
            raw_db_host = os.getenv("DB_HOST", "").strip()
            if raw_db_host.startswith(("postgres://", "postgresql://", "postgresql+asyncpg://")):
                raw_database_url = raw_db_host

        if not raw_database_url:
            return ""

        if raw_database_url.startswith("postgresql+asyncpg://"):
            return raw_database_url
        if raw_database_url.startswith("postgres://"):
            return "postgresql+asyncpg://" + raw_database_url[len("postgres://"):]
        if raw_database_url.startswith("postgresql://"):
            return "postgresql+asyncpg://" + raw_database_url[len("postgresql://"):]
        return raw_database_url

    def _parse_graph_apps(self) -> List[dict]:
        """Parse multiple Graph app registrations from env vars."""
        apps = []
        for i in range(1, 11):  # Support up to 10 app registrations
            # Only use fallback for APP_1 (legacy single-app mode)
            if i == 1:
                client_id = os.getenv(f"APP_{i}_CLIENT_ID") or os.getenv("AZURE_AD_CLIENT_ID", "")
                client_secret = os.getenv(f"APP_{i}_CLIENT_SECRET") or os.getenv("AZURE_AD_CLIENT_SECRET", "")
                tenant_id = os.getenv(f"APP_{i}_TENANT_ID") or os.getenv("AZURE_AD_TENANT_ID", "common")
            else:
                # For APP_2+, require explicit values - no fallback
                client_id = os.getenv(f"APP_{i}_CLIENT_ID", "")
                client_secret = os.getenv(f"APP_{i}_CLIENT_SECRET", "")
                tenant_id = os.getenv(f"APP_{i}_TENANT_ID", "common")

            if client_id and client_secret:
                apps.append({
                    "index": i,
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "tenant_id": tenant_id,
                })

            # If using single app config (legacy), stop after first
            if not os.getenv(f"APP_{i}_CLIENT_ID") and not os.getenv(f"APP_{i}_CLIENT_SECRET"):
                break

        return apps or [{
            "index": 1,
            "client_id": "",
            "client_secret": "",
            "tenant_id": "common",
        }]

    @property
    def GRAPH_APP_COUNT(self) -> int:
        return len(self.GRAPH_APPS)

    # Backward compatibility properties for auth-service
    @property
    def MICROSOFT_CLIENT_ID(self) -> str:
        return self.GRAPH_APPS[0]["client_id"] if self.GRAPH_APPS else ""

    @property
    def MICROSOFT_CLIENT_SECRET(self) -> str:
        return self.GRAPH_APPS[0]["client_secret"] if self.GRAPH_APPS else ""

    @property
    def MICROSOFT_TENANT_ID(self) -> str:
        return self.GRAPH_APPS[0]["tenant_id"] if self.GRAPH_APPS else "common"

    @property
    def EFFECTIVE_POWER_BI_CLIENT_ID(self) -> str:
        return self.POWER_BI_CLIENT_ID or self.MICROSOFT_CLIENT_ID

    @property
    def EFFECTIVE_POWER_BI_CLIENT_SECRET(self) -> str:
        return self.POWER_BI_CLIENT_SECRET or self.MICROSOFT_CLIENT_SECRET

    @property
    def EFFECTIVE_POWER_BI_TENANT_ID(self) -> str:
        return self.POWER_BI_TENANT_ID or self.MICROSOFT_TENANT_ID

    # ARM credentials fallback to Graph app if not explicitly set
    @property
    def EFFECTIVE_ARM_CLIENT_ID(self) -> str:
        return self.AZURE_ARM_CLIENT_ID or self.MICROSOFT_CLIENT_ID

    @property
    def EFFECTIVE_ARM_CLIENT_SECRET(self) -> str:
        return self.AZURE_ARM_CLIENT_SECRET or self.MICROSOFT_CLIENT_SECRET

    @property
    def EFFECTIVE_ARM_TENANT_ID(self) -> str:
        return self.AZURE_ARM_TENANT_ID or self.MICROSOFT_TENANT_ID

    @property
    def DATABASE_URL(self) -> str:
        if self._database_url_override:
            return self._database_url_override

        username = quote(self.DB_USERNAME or "")
        password = quote(self.DB_PASSWORD or "")
        return f"postgresql+asyncpg://{username}:{password}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def RABBITMQ_URL(self) -> str:
        return f"amqp://{self.RABBITMQ_USER}:{self.RABBITMQ_PASSWORD}@{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}/"


settings = Settings()
