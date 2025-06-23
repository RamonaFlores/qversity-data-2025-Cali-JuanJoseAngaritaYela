from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = (
        "postgresql+psycopg2://qversity-admin:qversity-admin@localhost:5432/qversity"
    )

settings = Settings()
