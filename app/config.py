from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    database_url: str = "sqlite:///./qanas.db"
    stockanalysis_splits_url: str = "https://stockanalysis.com/actions/splits/2026/"
    ibkr_base_url: str = "https://localhost:5000/v1/api"
    ibkr_verify_ssl: bool = False
    collect_interval_seconds: int = 60
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
