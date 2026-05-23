from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    google_cloud_project: str = ""
    vertex_location: str = "us-central1"
    google_genai_use_vertexai: bool = True

    gemini_model: str = "gemini-flash-latest"

    local_mode: bool = True
    stub_llm: bool = False
    chunk_seconds: int = 5
    dedupe_ttl_seconds: int = 60
    max_claims_per_session: int = 50

    firestore_collection: str = "verdicts"
    pubsub_chunk_topic: str = "fact-check-chunks"
    pubsub_verdict_topic: str = "fact-check-verdicts"

    trusted_domains: str = (
        "wikipedia.org,pubmed.ncbi.nlm.nih.gov,worldbank.org,"
        "earthquake.usgs.gov,cdc.gov,nih.gov,who.int"
    )

    host: str = "0.0.0.0"
    port: int = 8080

    @property
    def trusted_domain_list(self) -> list[str]:
        return [d.strip().lower() for d in self.trusted_domains.split(",") if d.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
