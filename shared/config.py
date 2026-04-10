"""Shared configuration for all microservices"""
import os
from typing import List


class Settings:
    def __init__(self):
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
        self.MICROSOFT_CLIENT_ID = os.getenv("MICROSOFT_CLIENT_ID") or os.getenv("AZURE_AD_CLIENT_ID", "")
        self.MICROSOFT_CLIENT_SECRET = os.getenv("MICROSOFT_CLIENT_SECRET") or os.getenv("AZURE_AD_CLIENT_SECRET", "")
        self.MICROSOFT_TENANT_ID = os.getenv("MICROSOFT_TENANT_ID") or os.getenv("AZURE_AD_TENANT_ID", "common")
        self.REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
        self.REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
        self.REDIS_DB = int(os.getenv("REDIS_DB", "0"))
        self.REDIS_ENABLED = os.getenv("REDIS_ENABLED", "false").lower() in ("true", "1", "yes")
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

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.DB_USERNAME}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def MICROSOFT_AUTH_URL(self) -> str:
        return f"https://login.microsoftonline.com/{self.MICROSOFT_TENANT_ID}/oauth2/v2.0/authorize"

    @property
    def MICROSOFT_TOKEN_URL(self) -> str:
        return f"https://login.microsoftonline.com/{self.MICROSOFT_TENANT_ID}/oauth2/v2.0/token"

    @property
    def RABBITMQ_URL(self) -> str:
        return f"amqp://{self.RABBITMQ_USER}:{self.RABBITMQ_PASSWORD}@{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}/"


settings = Settings()
