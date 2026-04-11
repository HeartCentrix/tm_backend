"""Shared configuration for all microservices"""
import os
from typing import List
from urllib.parse import urlparse


class Settings:
    def __init__(self):
        # Railway provides DATABASE_URL directly; parse it if present
        railway_database_url = os.getenv("DATABASE_URL")
        if railway_database_url:
            parsed = urlparse(railway_database_url)
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
        self.ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
        self.ELASTICSEARCH_ENABLED = False
        origins = os.getenv("CORS_ORIGINS") or os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:4200,http://localhost:3000,http://localhost:5173")
        self.CORS_ORIGINS = [o.strip() for o in origins.split(",")]

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

        # Multi-app registration for Microsoft Graph API
        # Parse from env: APP_1_CLIENT_ID, APP_1_CLIENT_SECRET, APP_1_TENANT_ID, etc.
        self.GRAPH_APPS = self._parse_graph_apps()

    def _parse_graph_apps(self) -> List[dict]:
        """Parse multiple Graph app registrations from env vars."""
        apps = []
        for i in range(1, 11):  # Support up to 10 app registrations
            client_id = os.getenv(f"APP_{i}_CLIENT_ID") or os.getenv("AZURE_AD_CLIENT_ID", "")
            client_secret = os.getenv(f"APP_{i}_CLIENT_SECRET") or os.getenv("AZURE_AD_CLIENT_SECRET", "")
            tenant_id = os.getenv(f"APP_{i}_TENANT_ID") or os.getenv("AZURE_AD_TENANT_ID", "common")

            if client_id and client_secret:
                apps.append({
                    "index": i,
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "tenant_id": tenant_id,
                })

            # If using single app config (legacy), stop after first
            if not os.getenv(f"APP_{i}_CLIENT_ID") and not os.getenv(f"APP_{i}_CLIENT_SECRET"):
                if i == 1 and client_id and client_secret:
                    # Legacy single app mode
                    apps.append({
                        "index": 1,
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "tenant_id": tenant_id,
                    })
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
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.DB_USERNAME}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def RABBITMQ_URL(self) -> str:
        return f"amqp://{self.RABBITMQ_USER}:{self.RABBITMQ_PASSWORD}@{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}/"


settings = Settings()
