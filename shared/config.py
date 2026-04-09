"""Shared configuration for all microservices"""
from pydantic_settings import BaseSettings
from typing import List, Optional


class Settings(BaseSettings):
    # Service
    SERVICE_NAME: str = "tm-vault-service"
    SERVICE_PORT: int = 8000
    
    # Database
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "tm_vault_db"
    DB_USERNAME: str = "tm_vault_admin"
    DB_PASSWORD: str = "admin123"
    
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.DB_USERNAME}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    # JWT
    JWT_SECRET: str = "your-secret-key-change-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_HOURS: int = 8
    JWT_REFRESH_EXPIRATION_DAYS: int = 7
    
    # Microsoft OAuth
    MICROSOFT_CLIENT_ID: str = ""
    MICROSOFT_CLIENT_SECRET: str = ""
    MICROSOFT_TENANT_ID: str = "common"
    
    @property
    def MICROSOFT_AUTH_URL(self) -> str:
        return f"https://login.microsoftonline.com/{self.MICROSOFT_TENANT_ID}/oauth2/v2.0/authorize"
    
    @property
    def MICROSOFT_TOKEN_URL(self) -> str:
        return f"https://login.microsoftonline.com/{self.MICROSOFT_TENANT_ID}/oauth2/v2.0/token"
    
    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_ENABLED: bool = False
    
    # RabbitMQ
    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"
    RABBITMQ_ENABLED: bool = False
    
    @property
    def RABBITMQ_URL(self) -> str:
        return f"amqp://{self.RABBITMQ_USER}:{self.RABBITMQ_PASSWORD}@{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}/"
    
    # Azure Storage
    AZURE_STORAGE_ACCOUNT_NAME: str = ""
    AZURE_STORAGE_ACCOUNT_KEY: str = ""
    
    # Elasticsearch
    ELASTICSEARCH_URL: str = "http://localhost:9200"
    ELASTICSEARCH_ENABLED: bool = False
    
    # CORS
    CORS_ORIGINS: List[str] = ["http://localhost:4200", "http://localhost:3000", "http://localhost:5173"]
    
    class Config:
        env_file = ".env"
        case_sensitive = True
