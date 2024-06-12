from pydantic_settings import BaseSettings, SettingsConfigDict


class CatalogSettings(BaseSettings):
    catalog_name: str
    catalog_config: dict[str, str]

    model_config = SettingsConfigDict(env_file=".env")
