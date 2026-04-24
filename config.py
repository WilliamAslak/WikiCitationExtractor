from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    database_url: str
    bot_name: str
    bot_version: str
    contact_email: str
    qlever_url: str

    model_config = SettingsConfigDict(env_file=".ENV")

settings = Settings()